package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dota2-analytics/central-server/internal/models"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	keyPrefixQueue       = "queue:jobs:"
	keyDelayedZSet       = "queue:delayed"
	keyActiveLeasesZSet  = "leases:active"
	keyPrefixBatchData   = "batch:data:"
	keyFilterConfig      = "config:match_filter"
	keyLastSeqNum        = "harvester:last_seq_num"
	defaultLeaseTTL      = 30 * time.Minute
	batchDataSafetyTTL       = 48 * time.Hour
	maxRetryAttempts         = 3
	maxActiveLeasesPerWorker = 3
	maxActiveLeasesPerIP     = 12
)

var (
	ErrLeaseNotFound     = errors.New("batch lease not found or already completed")
	ErrTooManyLeases     = errors.New("too many active leases for this worker/IP")
	ErrEmptyQueues       = errors.New("no jobs available in regional or global queues")
)

// LeaseManager coordinates Redis regional job queues, active worker leases, and expired lease recovery.
type LeaseManager struct {
	rdb      *redis.Client
	leaseTTL time.Duration
	mu       sync.Mutex
}

// NewLeaseManager creates a new LeaseManager backed by the provided Redis client.
func NewLeaseManager(rdb *redis.Client, leaseTTL time.Duration) *LeaseManager {
	if leaseTTL <= 0 {
		leaseTTL = defaultLeaseTTL
	}
	return &LeaseManager{
		rdb:      rdb,
		leaseTTL: leaseTTL,
	}
}

func queueKey(region models.MacroRegion) string {
	if region == "" {
		region = models.RegionEU
	}
	return keyPrefixQueue + string(region)
}

func batchDataKey(batchID string) string {
	return keyPrefixBatchData + batchID
}

// EnqueueDelayed places a resolved MatchJob into the delayed ZSET until its ReadyAt timestamp.
func (m *LeaseManager) EnqueueDelayed(ctx context.Context, job models.MatchJob) error {
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	if job.ReadyAt.IsZero() {
		job.ReadyAt = time.Now().UTC()
	}
	raw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return m.rdb.ZAdd(ctx, keyDelayedZSet, redis.Z{
		Score:  float64(job.ReadyAt.Unix()),
		Member: string(raw),
	}).Err()
}

// EnqueueReady pushes a MatchJob directly to the tail of its regional queue.
func (m *LeaseManager) EnqueueReady(ctx context.Context, job models.MatchJob) error {
	if job.Region == "" {
		job.Region = models.ResolveClusterRegion(job.Cluster)
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	raw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return m.rdb.RPush(ctx, queueKey(job.Region), string(raw)).Err()
}

// PromoteDelayedJobs moves jobs whose ReadyAt timestamp has passed from queue:delayed into regional queues.
func (m *LeaseManager) PromoteDelayedJobs(ctx context.Context) (int, error) {
	nowUnix := fmt.Sprintf("%d", time.Now().UTC().Unix())
	items, err := m.rdb.ZRangeByScore(ctx, keyDelayedZSet, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   nowUnix,
		Count: 200,
	}).Result()
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}

	promoted := 0
	for _, raw := range items {
		removed, err := m.rdb.ZRem(ctx, keyDelayedZSet, raw).Result()
		if err != nil || removed == 0 {
			continue
		}
		var job models.MatchJob
		if err := json.Unmarshal([]byte(raw), &job); err != nil {
			continue
		}
		if err := m.EnqueueReady(ctx, job); err == nil {
			promoted++
		}
	}
	return promoted, nil
}

// LeaseBatch pops up to maxTasks from the worker's ranked regionPriority queues (or preferredRegion first)
// and creates a tracked lease in leases:active + batch:data:{batch_id}.
func (m *LeaseManager) LeaseBatch(
	ctx context.Context,
	preferredRegion models.MacroRegion,
	regionPriority []models.MacroRegion,
	workerID string,
	workerIP string,
	maxTasks int,
) (*models.LeaseResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if maxTasks <= 0 {
		maxTasks = 10
	}
	if maxTasks > 100 {
		maxTasks = 100
	}

	// Check active lease limits per worker_id and per IP so one user can run multiple workers.
	activeLeases, err := m.ListActiveLeases(ctx)
	if err == nil {
		activeForWorker := 0
		activeForIP := 0
		for _, l := range activeLeases {
			if workerID != "" && l.WorkerID == workerID {
				activeForWorker++
			}
			if workerIP != "" && l.WorkerIP == workerIP {
				activeForIP++
			}
		}
		if activeForWorker >= maxActiveLeasesPerWorker || activeForIP >= maxActiveLeasesPerIP {
			return nil, ErrTooManyLeases
		}
	}

	// Build deduplicated region order:
	// 1. Pinned preferredRegion (if explicitly set and not "AUTO")
	// 2. Client's latency-ranked regionPriority list (best -> worst)
	// 3. GLOBAL queue
	// 4. Any remaining regions in models.AllRegions
	seenReg := make(map[models.MacroRegion]bool)
	var regionOrder []models.MacroRegion
	addRegion := func(r models.MacroRegion) {
		if r == "" || r == "AUTO" || seenReg[r] {
			return
		}
		seenReg[r] = true
		regionOrder = append(regionOrder, r)
	}

	addRegion(preferredRegion)
	for _, r := range regionPriority {
		addRegion(r)
	}
	addRegion(models.RegionGlobal)
	for _, r := range models.AllRegions {
		addRegion(r)
	}

	var poppedJobs []models.MatchJob
	servedRegion := models.RegionEU
	if len(regionOrder) > 0 {
		servedRegion = regionOrder[0]
	}

	for _, reg := range regionOrder {
		needed := maxTasks - len(poppedJobs)
		if needed <= 0 {
			break
		}
		qKey := queueKey(reg)
		for i := 0; i < needed; i++ {
			raw, err := m.rdb.LPop(ctx, qKey).Result()
			if errors.Is(err, redis.Nil) {
				break
			}
			if err != nil {
				return nil, err
			}
			var job models.MatchJob
			if err := json.Unmarshal([]byte(raw), &job); err != nil {
				continue
			}
			if len(poppedJobs) == 0 {
				servedRegion = reg
			}
			poppedJobs = append(poppedJobs, job)
		}
	}

	if len(poppedJobs) == 0 {
		return nil, ErrEmptyQueues
	}

	batchID := uuid.NewString()
	now := time.Now().UTC()
	expiresAt := now.Add(m.leaseTTL)

	jobMap := make(map[uint64]models.MatchJob, len(poppedJobs))
	tasks := make([]models.LeasedTask, 0, len(poppedJobs))

	for _, job := range poppedJobs {
		jobMap[job.MatchID] = job
		tasks = append(tasks, models.LeasedTask{
			MatchID:         job.MatchID,
			Cluster:         job.Cluster,
			ReplaySalt:      job.ReplaySalt,
			Region:          job.Region,
			URL:             job.URL,
			DurationHintSec: job.DurationSeconds,
		})
	}

	lease := models.ActiveLease{
		BatchID:      batchID,
		WorkerID:     workerID,
		WorkerIP:     workerIP,
		Region:       servedRegion,
		LeasedAt:     now,
		ExpiresAt:    expiresAt,
		Jobs:         jobMap,
		CompletedIDs: make(map[uint64]bool),
	}

	leaseBytes, err := json.Marshal(lease)
	if err != nil {
		return nil, err
	}

	pipe := m.rdb.TxPipeline()
	pipe.Set(ctx, batchDataKey(batchID), string(leaseBytes), batchDataSafetyTTL)
	pipe.ZAdd(ctx, keyActiveLeasesZSet, redis.Z{
		Score:  float64(expiresAt.Unix()),
		Member: batchID,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	return &models.LeaseResponse{
		BatchID:          batchID,
		Region:           servedRegion,
		ExpiresInSeconds: int(m.leaseTTL.Seconds()),
		SchemaVersion:    1,
		Tasks:            tasks,
	}, nil
}

// SubmitOutcome contains validated records ready for storage after processing a batch submission.
type SubmitOutcome struct {
	Response      models.SubmitBatchResponse
	ValidRecords  []models.StoredMatchRecord
}

// SubmitBatchResults processes incremental or full results without parser-version enforcement.
func (m *LeaseManager) SubmitBatchResults(ctx context.Context, req models.SubmitBatchRequest) (*SubmitOutcome, error) {
	return m.SubmitBatchResultsWithValidator(ctx, req, nil)
}

// SubmitBatchResultsWithValidator processes batch results and dismisses any SUCCESS result whose reported parser
// does not match the coordinator's currently active parser configuration (re-queuing those matches for re-parsing).
func (m *LeaseManager) SubmitBatchResultsWithValidator(
	ctx context.Context,
	req models.SubmitBatchRequest,
	matchesActive func(parserName, parserSHA256 string) bool,
) (*SubmitOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	raw, err := m.rdb.Get(ctx, batchDataKey(req.BatchID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrLeaseNotFound
	}
	if err != nil {
		return nil, err
	}

	var lease models.ActiveLease
	if err := json.Unmarshal([]byte(raw), &lease); err != nil {
		return nil, err
	}
	if lease.CompletedIDs == nil {
		lease.CompletedIDs = make(map[uint64]bool)
	}

	accepted := 0
	retried := 0
	failed := 0
	dismissed := 0
	var validRecords []models.StoredMatchRecord

	for _, res := range req.Results {
		job, exists := lease.Jobs[res.MatchID]
		if !exists {
			continue
		}
		if lease.CompletedIDs[res.MatchID] {
			// Already processed in an earlier incremental submission for this batch
			continue
		}

		switch res.Status {
		case models.StatusSuccess:
			if matchesActive != nil && !matchesActive(res.ParserName, res.ParserSHA256) {
				dismissed++
				lease.CompletedIDs[res.MatchID] = true
				_ = m.EnqueueReady(ctx, job)
				continue
			}

			// Ensure payload is valid JSON (default to "{}" if empty)
			payload := res.Payload
			if len(payload) == 0 || !json.Valid(payload) {
				failed++
				lease.CompletedIDs[res.MatchID] = true
				continue
			}

			parserName := res.ParserName
			if parserName == "" {
				parserName = "default"
			}
			parserVersion := res.ParserVersion
			if parserVersion == "" {
				parserVersion = req.ClientVersion
			}

			validRecords = append(validRecords, models.StoredMatchRecord{
				MatchID:       job.MatchID,
				Cluster:       job.Cluster,
				Region:        string(job.Region),
				SchemaVersion: 1,
				ParserName:    parserName,
				ParserVersion: parserVersion,
				WorkerID:      req.WorkerID,
				Payload:       payload,
				CreatedAt:     time.Now().UTC(),
			})
			lease.CompletedIDs[res.MatchID] = true
			accepted++

		case models.StatusTemp404:
			lease.CompletedIDs[res.MatchID] = true
			job.AttemptCount++
			if job.AttemptCount < maxRetryAttempts {
				job.ReadyAt = time.Now().UTC().Add(2 * time.Minute)
				_ = m.EnqueueDelayed(ctx, job)
				retried++
			} else {
				failed++
			}

		case models.StatusExpired404, models.StatusCorruptReplay, models.StatusNoSalt:
			lease.CompletedIDs[res.MatchID] = true
			failed++

		default:
			lease.CompletedIDs[res.MatchID] = true
			failed++
		}
	}

	// Check if all jobs in the lease have now been reported
	allCompleted := len(lease.CompletedIDs) >= len(lease.Jobs)

	if allCompleted {
		pipe := m.rdb.TxPipeline()
		pipe.ZRem(ctx, keyActiveLeasesZSet, req.BatchID)
		pipe.Del(ctx, batchDataKey(req.BatchID))
		_, _ = pipe.Exec(ctx)
	} else {
		// Extend lease TTL on incremental progress and save updated CompletedIDs
		lease.ExpiresAt = time.Now().UTC().Add(m.leaseTTL)
		updatedBytes, _ := json.Marshal(lease)
		pipe := m.rdb.TxPipeline()
		pipe.Set(ctx, batchDataKey(req.BatchID), string(updatedBytes), batchDataSafetyTTL)
		pipe.ZAdd(ctx, keyActiveLeasesZSet, redis.Z{
			Score:  float64(lease.ExpiresAt.Unix()),
			Member: req.BatchID,
		})
		_, _ = pipe.Exec(ctx)
	}

	return &SubmitOutcome{
		Response: models.SubmitBatchResponse{
			Status:           "ACK",
			BatchID:          req.BatchID,
			MatchesAccepted:  accepted,
			MatchesRetried:   retried,
			MatchesFailed:    failed,
			MatchesDismissed: dismissed,
			LeaseCompleted:   allCompleted,
		},
		ValidRecords: validRecords,
	}, nil
}

// ReapExpiredLeases finds expired leases in leases:active, reads their intact payload from batch:data:{batch_id},
// and pushes any unfinished jobs back to the head of their regional queues.
func (m *LeaseManager) ReapExpiredLeases(ctx context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nowUnix := fmt.Sprintf("%d", time.Now().UTC().Unix())
	expiredBatchIDs, err := m.rdb.ZRangeByScore(ctx, keyActiveLeasesZSet, &redis.ZRangeBy{
		Min: "-inf",
		Max: nowUnix,
	}).Result()
	if err != nil {
		return 0, err
	}
	if len(expiredBatchIDs) == 0 {
		return 0, nil
	}

	requeuedJobs := 0
	for _, batchID := range expiredBatchIDs {
		raw, err := m.rdb.Get(ctx, batchDataKey(batchID)).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			continue
		}

		if raw != "" {
			var lease models.ActiveLease
			if err := json.Unmarshal([]byte(raw), &lease); err == nil {
				for matchID, job := range lease.Jobs {
					if lease.CompletedIDs != nil && lease.CompletedIDs[matchID] {
						continue
					}
					job.AttemptCount++
					if job.AttemptCount <= maxRetryAttempts {
						jobBytes, _ := json.Marshal(job)
						// Push back to the HEAD of the regional queue for priority execution
						_ = m.rdb.LPush(ctx, queueKey(job.Region), string(jobBytes)).Err()
						requeuedJobs++
					}
				}
			}
		}

		pipe := m.rdb.TxPipeline()
		pipe.ZRem(ctx, keyActiveLeasesZSet, batchID)
		pipe.Del(ctx, batchDataKey(batchID))
		_, _ = pipe.Exec(ctx)
	}

	if requeuedJobs > 0 {
		log.Printf("[Reaper] Recovered %d unfinished jobs from %d expired leases", requeuedJobs, len(expiredBatchIDs))
	}
	return requeuedJobs, nil
}

// GetQueueDepths returns the number of waiting jobs in each regional queue and the delayed ZSET.
func (m *LeaseManager) GetQueueDepths(ctx context.Context) (map[models.MacroRegion]int64, int64, error) {
	depths := make(map[models.MacroRegion]int64, len(models.AllRegions)+1)
	regions := append([]models.MacroRegion{}, models.AllRegions...)
	regions = append(regions, models.RegionGlobal)

	for _, reg := range regions {
		cnt, err := m.rdb.LLen(ctx, queueKey(reg)).Result()
		if err != nil {
			return nil, 0, err
		}
		depths[reg] = cnt
	}

	delayedCount, err := m.rdb.ZCard(ctx, keyDelayedZSet).Result()
	if err != nil {
		return depths, 0, err
	}
	return depths, delayedCount, nil
}

// ListActiveLeases returns all currently active worker leases.
func (m *LeaseManager) ListActiveLeases(ctx context.Context) ([]models.ActiveLease, error) {
	batchIDs, err := m.rdb.ZRange(ctx, keyActiveLeasesZSet, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	leases := make([]models.ActiveLease, 0, len(batchIDs))
	for _, id := range batchIDs {
		raw, err := m.rdb.Get(ctx, batchDataKey(id)).Result()
		if err != nil {
			continue
		}
		var lease models.ActiveLease
		if err := json.Unmarshal([]byte(raw), &lease); err == nil {
			leases = append(leases, lease)
		}
	}
	return leases, nil
}

// SaveFilterConfig persists the dynamic FilterConfig in Redis.
func (m *LeaseManager) SaveFilterConfig(ctx context.Context, cfg models.FilterConfig) error {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return m.rdb.Set(ctx, keyFilterConfig, string(raw), 0).Err()
}

// LoadFilterConfig loads the persisted FilterConfig from Redis, or returns DefaultFilterConfig if none exists.
func (m *LeaseManager) LoadFilterConfig(ctx context.Context) (models.FilterConfig, error) {
	raw, err := m.rdb.Get(ctx, keyFilterConfig).Result()
	if errors.Is(err, redis.Nil) {
		return models.DefaultFilterConfig(), nil
	}
	if err != nil {
		return models.DefaultFilterConfig(), err
	}
	var cfg models.FilterConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return models.DefaultFilterConfig(), err
	}
	return cfg, nil
}

// GetTotalQueued returns the sum of all regional and delayed queues.
func (m *LeaseManager) GetTotalQueued(ctx context.Context) int64 {
	depths, delayed, err := m.GetQueueDepths(ctx)
	if err != nil {
		return 0
	}
	total := delayed
	for _, c := range depths {
		total += c
	}
	return total
}
