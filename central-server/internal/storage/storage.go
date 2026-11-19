package storage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dota2-analytics/central-server/internal/models"
)

// SinkConfig holds configuration for forwarding parsed matches to an external program.
type SinkConfig struct {
	WebhookURL         string    `json:"webhook_url"`
	WebhookEnabled     bool      `json:"webhook_enabled"`
	FileStorageEnabled bool      `json:"file_storage_enabled"`
	StorageDir         string    `json:"storage_dir"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// SinkStats tracks file storage and external webhook delivery metrics.
type SinkStats struct {
	StorageDir         string `json:"storage_dir"`
	FileStorageEnabled bool   `json:"file_storage_enabled"`
	FilesStored        uint64 `json:"files_stored"`
	WebhookURL         string `json:"webhook_url"`
	WebhookEnabled     bool   `json:"webhook_enabled"`
	WebhookDelivered   uint64 `json:"webhook_delivered"`
	WebhookFailed      uint64 `json:"webhook_failed"`
	WebhookLastError   string `json:"webhook_last_error"`
	StreamSubscribers  int    `json:"stream_subscribers"`
}

// AnalyticsStore writes each parsed match atomically to `<storageDir>/<match_id>.json`
// and forwards newly parsed matches to external programs via HTTP Webhook and live SSE streams.
type AnalyticsStore struct {
	storageDir string
	indexPath  string
	httpClient *http.Client

	recordCh chan models.StoredMatchRecord

	mu               sync.RWMutex
	sinkCfg          SinkConfig
	recentRecords    []models.StoredMatchRecord
	maxRecent        int
	rawReplays       map[uint64][]byte
	rawReplayOrder   []uint64
	totalParsed      atomic.Uint64
	todayParsed      atomic.Uint64
	webhookDelivered atomic.Uint64
	webhookFailed    atomic.Uint64
	webhookLastErr   string

	subMu       sync.Mutex
	subscribers map[chan models.StoredMatchRecord]struct{}
}

// NewAnalyticsStore initializes the on-disk match file store (`data/matches/<match_id>.json`)
// and loads existing index metadata so counters and recent matches persist across restarts.
func NewAnalyticsStore(storageDir string, initialWebhookURL string) *AnalyticsStore {
	if storageDir == "" {
		storageDir = filepath.Join("data", "matches")
	}
	_ = os.MkdirAll(storageDir, 0o755)

	s := &AnalyticsStore{
		storageDir: storageDir,
		indexPath:  filepath.Join(storageDir, "_index.jsonl"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		recordCh:      make(chan models.StoredMatchRecord, 5000),
		recentRecords: make([]models.StoredMatchRecord, 0, 200),
		maxRecent:     200,
		rawReplays:    make(map[uint64][]byte),
		sinkCfg: SinkConfig{
			WebhookURL:         initialWebhookURL,
			WebhookEnabled:     initialWebhookURL != "",
			FileStorageEnabled: true,
			StorageDir:         storageDir,
			UpdatedAt:          time.Now().UTC(),
		},
		subscribers: make(map[chan models.StoredMatchRecord]struct{}),
	}

	s.loadExistingIndex()
	log.Printf("[Storage] File storage active at %s (restored %d parsed matches)", storageDir, s.totalParsed.Load())
	return s
}

// Start launches the background writer and webhook forwarder loop.
func (s *AnalyticsStore) Start(ctx context.Context) {
	go s.workerLoop(ctx)
}

// UpdateSinkConfig updates the in-memory external webhook settings.
func (s *AnalyticsStore) UpdateSinkConfig(webhookURL string, enabled bool) SinkConfig {
	s.mu.Lock()
	s.sinkCfg.WebhookURL = webhookURL
	s.sinkCfg.WebhookEnabled = enabled && webhookURL != ""
	s.sinkCfg.StorageDir = s.storageDir
	s.sinkCfg.UpdatedAt = time.Now().UTC()
	s.webhookLastErr = ""
	cfg := s.sinkCfg
	s.mu.Unlock()

	return cfg
}

// SetFileStorageEnabled enables or disables writing parsed match files to disk in memory.
func (s *AnalyticsStore) SetFileStorageEnabled(enabled bool) SinkConfig {
	s.mu.Lock()
	s.sinkCfg.FileStorageEnabled = enabled
	s.sinkCfg.StorageDir = s.storageDir
	s.sinkCfg.UpdatedAt = time.Now().UTC()
	cfg := s.sinkCfg
	s.mu.Unlock()

	return cfg
}

// GetSinkStats returns current file storage and external webhook delivery telemetry.
func (s *AnalyticsStore) GetSinkStats() SinkStats {
	s.mu.RLock()
	cfg := s.sinkCfg
	lastErr := s.webhookLastErr
	s.mu.RUnlock()

	s.subMu.Lock()
	subs := len(s.subscribers)
	s.subMu.Unlock()

	return SinkStats{
		StorageDir:         s.storageDir,
		FileStorageEnabled: cfg.FileStorageEnabled,
		FilesStored:        s.totalParsed.Load(),
		WebhookURL:         cfg.WebhookURL,
		WebhookEnabled:     cfg.WebhookEnabled,
		WebhookDelivered:   s.webhookDelivered.Load(),
		WebhookFailed:      s.webhookFailed.Load(),
		WebhookLastError:   lastErr,
		StreamSubscribers:  subs,
	}
}

func (s *AnalyticsStore) loadExistingIndex() {
	f, err := os.Open(s.indexPath)
	if err != nil {
		return
	}
	defer f.Close()

	todayStr := time.Now().UTC().Format("2006-01-02")
	scanner := bufio.NewScanner(f)
	// Support large payloads up to 10MB per line in index
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	var all []models.StoredMatchRecord
	var total uint64
	var today uint64

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec models.StoredMatchRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		total++
		if rec.CreatedAt.UTC().Format("2006-01-02") == todayStr {
			today++
		}
		all = append(all, rec)
		if len(all) > s.maxRecent {
			all = all[len(all)-s.maxRecent:]
		}
	}

	// Reverse `all` so newest is first in recentRecords
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}

	s.mu.Lock()
	s.recentRecords = all
	s.mu.Unlock()
	s.totalParsed.Store(total)
	s.todayParsed.Store(today)
}

// EnqueueRecords saves match records to `<storageDir>/<match_id>.json` (and `<match_id>.dem.bz2` for raw passthrough),
// updates the index, broadcasts to live stream subscribers, and forwards to the configured external webhook.
func (s *AnalyticsStore) EnqueueRecords(records []models.StoredMatchRecord) {
	if len(records) == 0 {
		return
	}

	type rawItem struct {
		rawBytes   []byte
		compactRec models.StoredMatchRecord
		fullRec    models.StoredMatchRecord
	}
	items := make([]rawItem, 0, len(records))

	s.mu.Lock()
	for i := range records {
		if records[i].CreatedAt.IsZero() {
			records[i].CreatedAt = time.Now().UTC()
		}
		rawBytes, compactRec := extractRawReplayBytes(records[i])
		s.recentRecords = append([]models.StoredMatchRecord{compactRec}, s.recentRecords...)
		if len(s.recentRecords) > s.maxRecent {
			s.recentRecords = s.recentRecords[:s.maxRecent]
		}
		if len(rawBytes) > 0 {
			s.cacheRawReplayLocked(records[i].MatchID, rawBytes)
		}
		s.totalParsed.Add(1)
		s.todayParsed.Add(1)
		items = append(items, rawItem{rawBytes: rawBytes, compactRec: compactRec, fullRec: records[i]})
	}
	saveToFile := s.sinkCfg.FileStorageEnabled
	s.mu.Unlock()

	for _, item := range items {
		// Write to disk synchronously when file storage is enabled so immediate GET /api/v1/matches/:id reads succeed
		if saveToFile {
			if len(item.rawBytes) > 0 {
				if err := s.writeRawReplayFile(item.compactRec.MatchID, item.rawBytes); err != nil {
					log.Printf("[Storage] Failed writing raw replay %d.dem.bz2: %v", item.compactRec.MatchID, err)
				}
			}
			if err := s.writeMatchFile(item.compactRec); err != nil {
				log.Printf("[Storage] Failed writing match %d file: %v", item.compactRec.MatchID, err)
			}
		}
		s.broadcastToSubscribers(item.fullRec)

		select {
		case s.recordCh <- item.fullRec:
		default:
			go s.forwardToWebhook(context.Background(), item.fullRec)
		}
	}
}

func extractRawReplayBytes(rec models.StoredMatchRecord) ([]byte, models.StoredMatchRecord) {
	if rec.ParserName != "raw" || len(rec.Payload) == 0 {
		return nil, rec
	}
	var obj map[string]any
	if err := json.Unmarshal(rec.Payload, &obj); err != nil {
		return nil, rec
	}
	b64, _ := obj["data_base64"].(string)
	if b64 == "" {
		return nil, rec
	}
	rawBytes, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, rec
	}
	shaHex, _ := obj["sha256"].(string)
	if shaHex == "" {
		sum := sha256.Sum256(rawBytes)
		shaHex = hex.EncodeToString(sum[:])
	}
	compactPayload, _ := json.Marshal(map[string]any{
		"mode":                  "raw",
		"match_id":              rec.MatchID,
		"cluster":               rec.Cluster,
		"region":                rec.Region,
		"format":                "dem.bz2",
		"compressed_size_bytes": len(rawBytes),
		"sha256":                shaHex,
		"file":                  fmt.Sprintf("%d.dem.bz2", rec.MatchID),
	})
	rec.Payload = compactPayload
	return rawBytes, rec
}

func (s *AnalyticsStore) cacheRawReplayLocked(matchID uint64, raw []byte) {
	if _, exists := s.rawReplays[matchID]; !exists {
		s.rawReplayOrder = append(s.rawReplayOrder, matchID)
	}
	s.rawReplays[matchID] = raw
	for len(s.rawReplayOrder) > 8 {
		oldest := s.rawReplayOrder[0]
		s.rawReplayOrder = s.rawReplayOrder[1:]
		delete(s.rawReplays, oldest)
	}
}

func (s *AnalyticsStore) writeRawReplayFile(matchID uint64, raw []byte) error {
	finalPath := filepath.Join(s.storageDir, fmt.Sprintf("%d.dem.bz2", matchID))
	tmpPath := finalPath + ".tmp"
	if err := os.WriteFile(tmpPath, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, finalPath)
}

// GetRawReplayByID reads `<storageDir>/<match_id>.dem.bz2` from disk or recent in-memory raw replay cache.
func (s *AnalyticsStore) GetRawReplayByID(matchID uint64) ([]byte, error) {
	path := filepath.Join(s.storageDir, fmt.Sprintf("%d.dem.bz2", matchID))
	if raw, err := os.ReadFile(path); err == nil {
		return raw, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if raw, ok := s.rawReplays[matchID]; ok {
		cp := make([]byte, len(raw))
		copy(cp, raw)
		return cp, nil
	}
	return nil, os.ErrNotExist
}

func (s *AnalyticsStore) writeMatchFile(rec models.StoredMatchRecord) error {
	pretty, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}

	finalPath := filepath.Join(s.storageDir, fmt.Sprintf("%d.json", rec.MatchID))
	tmpPath := finalPath + ".tmp"

	if err := os.WriteFile(tmpPath, pretty, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}

	// Append single-line JSON entry to _index.jsonl for fast startup recovery
	compact, err := json.Marshal(rec)
	if err == nil {
		f, err := os.OpenFile(s.indexPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			_, _ = f.Write(append(compact, '\n'))
			_ = f.Close()
		}
	}
	return nil
}

// GetMatchByID reads `<storageDir>/<match_id>.json` from disk, or falls back to the in-memory recent buffer
// (allowing REST pulls even when local file saving is disabled).
func (s *AnalyticsStore) GetMatchByID(matchID uint64) (*models.StoredMatchRecord, error) {
	path := filepath.Join(s.storageDir, fmt.Sprintf("%d.json", matchID))
	raw, err := os.ReadFile(path)
	if err == nil {
		var rec models.StoredMatchRecord
		if uErr := json.Unmarshal(raw, &rec); uErr != nil {
			return nil, uErr
		}
		return &rec, nil
	}

	s.mu.RLock()
	for i := range s.recentRecords {
		if s.recentRecords[i].MatchID == matchID {
			rec := s.recentRecords[i]
			s.mu.RUnlock()
			return &rec, nil
		}
	}
	s.mu.RUnlock()

	return nil, err
}

func (s *AnalyticsStore) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-s.recordCh:
			s.forwardToWebhook(ctx, rec)
		}
	}
}

func (s *AnalyticsStore) forwardToWebhook(ctx context.Context, rec models.StoredMatchRecord) {
	s.mu.RLock()
	cfg := s.sinkCfg
	s.mu.RUnlock()

	if !cfg.WebhookEnabled || cfg.WebhookURL == "" {
		return
	}

	body, err := json.Marshal(rec)
	if err != nil {
		return
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, cfg.WebhookURL, bytes.NewReader(body))
		if err != nil {
			cancel()
			lastErr = err
			break
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Dota2ReplayCourier-Coordinator/1.2")
		req.Header.Set("X-Match-ID", fmt.Sprintf("%d", rec.MatchID))

		resp, err := s.httpClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			cancel()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				s.webhookDelivered.Add(1)
				return
			}
			lastErr = fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
		} else {
			cancel()
			lastErr = err
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	if lastErr != nil {
		s.webhookFailed.Add(1)
		s.mu.Lock()
		s.webhookLastErr = lastErr.Error()
		s.mu.Unlock()
	}
}

// SubscribeStream registers a channel that receives every newly parsed match in real time.
func (s *AnalyticsStore) SubscribeStream() (chan models.StoredMatchRecord, func()) {
	ch := make(chan models.StoredMatchRecord, 64)
	s.subMu.Lock()
	s.subscribers[ch] = struct{}{}
	s.subMu.Unlock()

	unsubscribe := func() {
		s.subMu.Lock()
		delete(s.subscribers, ch)
		close(ch)
		s.subMu.Unlock()
	}
	return ch, unsubscribe
}

func (s *AnalyticsStore) broadcastToSubscribers(rec models.StoredMatchRecord) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for ch := range s.subscribers {
		select {
		case ch <- rec:
		default:
		}
	}
}

// GetRecentRecords returns up to limit of the most recently ingested match records.
func (s *AnalyticsStore) GetRecentRecords(limit int) []models.StoredMatchRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 || limit > len(s.recentRecords) {
		limit = len(s.recentRecords)
	}
	out := make([]models.StoredMatchRecord, limit)
	copy(out, s.recentRecords[:limit])
	return out
}

// GetCounters returns (totalParsed, todayParsed, storageActive).
func (s *AnalyticsStore) GetCounters() (uint64, uint64, bool) {
	return s.totalParsed.Load(), s.todayParsed.Load(), true
}
