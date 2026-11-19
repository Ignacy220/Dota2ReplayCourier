package harvester

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/dota2-analytics/central-server/internal/models"
	"github.com/dota2-analytics/central-server/internal/queue"
	"github.com/dota2-analytics/central-server/internal/resolver"
)

// Harvester polls for new Dota 2 matches, applies dynamic FilterEngine rules,
// resolves replay salts, and enqueues jobs into the regional Redis queues.
type Harvester struct {
	steamAPIKey  string
	filterEngine *FilterEngine
	leaseMgr     *queue.LeaseManager
	saltResolver resolver.SaltResolver
	httpClient   *http.Client

	mu             sync.Mutex
	seenMatches    map[uint64]time.Time
	lastSeqNum     uint64
	openDotaCursor uint64
	pollTickCount  uint64
}

// NewHarvester creates a new Harvester service.
func NewHarvester(
	steamAPIKey string,
	filterEngine *FilterEngine,
	leaseMgr *queue.LeaseManager,
	saltResolver resolver.SaltResolver,
) *Harvester {
	return &Harvester{
		steamAPIKey:  steamAPIKey,
		filterEngine: filterEngine,
		leaseMgr:     leaseMgr,
		saltResolver: saltResolver,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		seenMatches: make(map[uint64]time.Time),
	}
}

// Start launches the Harvester background loops (polling, delayed queue promotion, and lease reaper).
func (h *Harvester) Start(ctx context.Context) {
	// Run an immediate initial poll on startup so queues begin populating right away
	go func() {
		if h.filterEngine.GetConfig().Enabled {
			_ = h.PollOnce(ctx)
		}
	}()
	go h.pollLoop(ctx)
	go h.maintenanceLoop(ctx)
}

func (h *Harvester) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = h.leaseMgr.PromoteDelayedJobs(ctx)
			_, _ = h.leaseMgr.ReapExpiredLeases(ctx)
			h.pruneSeenCache()
		}
	}
}

func (h *Harvester) pollLoop(ctx context.Context) {
	for {
		cfg := h.filterEngine.GetConfig()
		interval := time.Duration(cfg.PollIntervalMs) * time.Millisecond
		if interval < 500*time.Millisecond {
			interval = 2 * time.Second
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			if !cfg.Enabled {
				continue
			}
			if cfg.MaxQueueSize > 0 && h.leaseMgr.GetTotalQueued(ctx) >= int64(cfg.MaxQueueSize) {
				continue
			}
			if err := h.PollOnce(ctx); err != nil {
				log.Printf("[Harvester] Poll cycle warning: %v", err)
			}
		}
	}
}

// PollOnce executes a single discovery cycle from Steam Web API (or OpenDota publicMatches if no Steam key).
// Filtered match IDs are enqueued directly into regional queues so Desktop Workers resolve salts using their own quotas.
func (h *Harvester) PollOnce(ctx context.Context) error {
	var candidates []models.RawMatchCandidate
	var err error

	if h.GetSteamAPIKey() != "" {
		candidates, err = h.fetchFromSteam(ctx)
	} else {
		candidates, err = h.fetchFromOpenDotaPublic(ctx)
	}
	if err != nil {
		return err
	}

	cfg := h.filterEngine.GetConfig()

	for _, cand := range candidates {
		if h.isSeen(cand.MatchID) {
			continue
		}
		h.markSeen(cand.MatchID)

		accepted, region, _ := h.filterEngine.Evaluate(cand)
		if !accepted {
			continue
		}

		cluster := cand.Cluster
		if cluster == 0 {
			cluster = 133
		}
		resolvedRegion := models.ResolveClusterRegion(cluster)
		if region != "" {
			resolvedRegion = region
		}

		radiantWin := cand.RadiantWin
		now := time.Now().UTC()

		readyAt := now
		if cfg.CDNDelaySeconds > 0 {
			if cand.StartTime > 0 && cand.DurationSeconds > 0 {
				matchEnd := time.Unix(cand.StartTime+int64(cand.DurationSeconds), 0).UTC()
				if matchEnd.After(now) {
					matchEnd = now
				}
				candidateReady := matchEnd.Add(time.Duration(cfg.CDNDelaySeconds) * time.Second)
				if candidateReady.After(now) {
					readyAt = candidateReady
				}
			}
		}

		job := models.MatchJob{
			MatchID:         cand.MatchID,
			MatchSeqNum:     cand.MatchSeqNum,
			Cluster:         cluster,
			ReplaySalt:      0, // Resolved by worker (or by coordinator fallback if worker is rate-limited)
			Region:          resolvedRegion,
			URL:             "",
			DurationSeconds: cand.DurationSeconds,
			RadiantWin:      &radiantWin,
			HeroIDs:         cand.HeroIDs,
			LobbyType:       cand.LobbyType,
			GameMode:        cand.GameMode,
			CreatedAt:       now,
			ReadyAt:         readyAt,
		}

		if readyAt.After(now) {
			_ = h.leaseMgr.EnqueueDelayed(ctx, job)
		} else {
			_ = h.leaseMgr.EnqueueReady(ctx, job)
		}
	}

	return nil
}

// ResolveJobSalt uses the Coordinator's reserve quota to resolve replay_salt for a worker that hit its local rate limit.
func (h *Harvester) ResolveJobSalt(ctx context.Context, matchID uint64, clusterHint uint32) (*resolver.SaltResult, error) {
	return h.saltResolver.Resolve(ctx, matchID, clusterHint)
}

type keyConfigurableResolver interface {
	SetKeys(stratzToken, openDotaAPIKey string)
	GetKeys() (stratzToken, openDotaAPIKey string)
}

// SetAPIKeys updates the Steam, STRATZ, and OpenDota API keys used by the Harvester and its resolver.
func (h *Harvester) SetAPIKeys(steamKey, stratzToken, openDotaAPIKey string) {
	steamKey = strings.TrimSpace(steamKey)
	h.mu.Lock()
	if h.steamAPIKey != steamKey {
		h.steamAPIKey = steamKey
		h.lastSeqNum = 0
	}
	h.mu.Unlock()

	if kr, ok := h.saltResolver.(keyConfigurableResolver); ok {
		kr.SetKeys(stratzToken, openDotaAPIKey)
	}
}

// GetAPIKeys returns the currently configured Steam, STRATZ, and OpenDota API keys.
func (h *Harvester) GetAPIKeys() (steamKey, stratzToken, openDotaAPIKey string) {
	h.mu.Lock()
	steamKey = h.steamAPIKey
	h.mu.Unlock()
	if kr, ok := h.saltResolver.(keyConfigurableResolver); ok {
		stratzToken, openDotaAPIKey = kr.GetKeys()
	}
	return steamKey, stratzToken, openDotaAPIKey
}

// GetSteamAPIKey returns the currently configured Steam Web API key.
func (h *Harvester) GetSteamAPIKey() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.steamAPIKey
}

const seqNumLookbackOffset uint64 = 1_000_000

// bootstrapStartingSeqNum resolves the starting match_seq_num on initial startup:
// 1. Calls Valve's GetMatchHistory requesting 1 match (matches_requested=1).
// 2. Extracts match_id (and match_seq_num if present; otherwise queries GetMatchDetails for match_id).
// 3. Subtracts 2,000,000 from match_seq_num (x - 2,000,000) and returns that starting point.
func (h *Harvester) bootstrapStartingSeqNum(ctx context.Context) (uint64, error) {
	steamKey := h.GetSteamAPIKey()
	histURL := fmt.Sprintf(
		"https://api.steampowered.com/IDota2Match_570/GetMatchHistory/v1?key=%s&matches_requested=1",
		url.QueryEscape(steamKey),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, histURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GetMatchHistory bootstrap returned status %d", resp.StatusCode)
	}

	var histPayload struct {
		Result struct {
			Status  int `json:"status"`
			Matches []struct {
				MatchID     uint64 `json:"match_id"`
				MatchSeqNum uint64 `json:"match_seq_num"`
			} `json:"matches"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&histPayload); err != nil {
		return 0, err
	}
	if len(histPayload.Result.Matches) == 0 || histPayload.Result.Matches[0].MatchID == 0 {
		return 0, fmt.Errorf("GetMatchHistory returned no matches")
	}

	latestMatchID := histPayload.Result.Matches[0].MatchID
	seqNum := histPayload.Result.Matches[0].MatchSeqNum

	// If GetMatchHistory did not include match_seq_num, fetch it via GetMatchDetails using latestMatchID
	if seqNum == 0 {
		detailsURL := fmt.Sprintf(
			"https://api.steampowered.com/IDota2Match_570/GetMatchDetails/v1?key=%s&match_id=%d",
			url.QueryEscape(steamKey), latestMatchID,
		)
		dReq, err := http.NewRequestWithContext(ctx, http.MethodGet, detailsURL, nil)
		if err != nil {
			return 0, err
		}
		dResp, err := h.httpClient.Do(dReq)
		if err != nil {
			return 0, err
		}
		defer dResp.Body.Close()

		if dResp.StatusCode == http.StatusOK {
			var detPayload struct {
				Result struct {
					MatchSeqNum uint64 `json:"match_seq_num"`
				} `json:"result"`
			}
			if err := json.NewDecoder(dResp.Body).Decode(&detPayload); err == nil {
				seqNum = detPayload.Result.MatchSeqNum
			}
		}
	}

	if seqNum == 0 {
		return 0, fmt.Errorf("could not resolve match_seq_num for match_id %d", latestMatchID)
	}

	startSeq := seqNum
	if seqNum > seqNumLookbackOffset {
		startSeq = seqNum - seqNumLookbackOffset
	}
	log.Printf("[Harvester] Bootstrapped starting point from match_id=%d (match_seq_num=%d) -> x-2,000,000 = %d", latestMatchID, seqNum, startSeq)
	return startSeq, nil
}

func (h *Harvester) fetchFromSteam(ctx context.Context) ([]models.RawMatchCandidate, error) {
	steamKey := h.GetSteamAPIKey()
	if h.lastSeqNum == 0 {
		startSeq, err := h.bootstrapStartingSeqNum(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to bootstrap starting match_seq_num: %w", err)
		}
		h.lastSeqNum = startSeq
		h.filterEngine.SetLastSeqNum(startSeq)
	}

	endpoint := fmt.Sprintf(
		"https://api.steampowered.com/IDota2Match_570/GetMatchHistoryBySequenceNum/v1?key=%s&start_at_match_seq_num=%d&matches_requested=100",
		url.QueryEscape(steamKey), h.lastSeqNum,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("steam API returned status %d", resp.StatusCode)
	}

	var payload struct {
		Result struct {
			Status  int `json:"status"`
			Matches []struct {
				MatchID      uint64 `json:"match_id"`
				MatchSeqNum  uint64 `json:"match_seq_num"`
				Cluster      uint32 `json:"cluster"`
				LobbyType    int    `json:"lobby_type"`
				GameMode     int    `json:"game_mode"`
				HumanPlayers int    `json:"human_players"`
				Duration     int    `json:"duration"`
				RadiantWin   bool   `json:"radiant_win"`
				StartTime    int64  `json:"start_time"`
				Players      []struct {
					HeroID int `json:"hero_id"`
				} `json:"players"`
			} `json:"matches"`
		} `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	out := make([]models.RawMatchCandidate, 0, len(payload.Result.Matches))
	for _, m := range payload.Result.Matches {
		if m.MatchSeqNum >= h.lastSeqNum {
			h.lastSeqNum = m.MatchSeqNum + 1
		}
		heroes := make([]int, 0, len(m.Players))
		for _, p := range m.Players {
			if p.HeroID > 0 {
				heroes = append(heroes, p.HeroID)
			}
		}
		out = append(out, models.RawMatchCandidate{
			MatchID:         m.MatchID,
			MatchSeqNum:     m.MatchSeqNum,
			Cluster:         m.Cluster,
			LobbyType:       m.LobbyType,
			GameMode:        m.GameMode,
			HumanPlayers:    m.HumanPlayers,
			DurationSeconds: m.Duration,
			RadiantWin:      m.RadiantWin,
			HeroIDs:         heroes,
			StartTime:       m.StartTime,
			DiscoveredAt:    time.Now().UTC(),
		})
	}
	return out, nil
}

func (h *Harvester) fetchFromOpenDotaPublic(ctx context.Context) ([]models.RawMatchCandidate, error) {
	h.mu.Lock()
	cursor := h.openDotaCursor
	h.mu.Unlock()

	_, _, openDotaKey := h.GetAPIKeys()

	// On first run (cursor == 0), fetch the latest match from /api/publicMatches and subtract 2,000,000
	// so we start harvesting from completed matches whose replays and salts are already available.
	if cursor == 0 {
		headURL := "https://api.opendota.com/api/publicMatches"
		if openDotaKey != "" {
			headURL += "?api_key=" + url.QueryEscape(openDotaKey)
		}
		headReq, err := http.NewRequestWithContext(ctx, http.MethodGet, headURL, nil)
		if err == nil {
			headReq.Header.Set("User-Agent", "Dota2ReplayCourier-CentralServer/1.2")
			if headResp, err := h.httpClient.Do(headReq); err == nil {
				var headRows []struct {
					MatchID     uint64 `json:"match_id"`
					MatchSeqNum uint64 `json:"match_seq_num"`
				}
				_ = json.NewDecoder(headResp.Body).Decode(&headRows)
				_ = headResp.Body.Close()
				if len(headRows) > 0 && headRows[0].MatchID > seqNumLookbackOffset {
					cursor = headRows[0].MatchID - seqNumLookbackOffset
					h.mu.Lock()
					h.openDotaCursor = cursor
					h.mu.Unlock()
					log.Printf("[Harvester] Bootstrapped starting cursor from latest match_id=%d -> x-2,000,000 = %d", headRows[0].MatchID, cursor)
				}
			}
		}
	}

	endpoint := "https://api.opendota.com/api/publicMatches"
	sep := "?"
	if cursor > 0 {
		endpoint = fmt.Sprintf("https://api.opendota.com/api/publicMatches?less_than_match_id=%d", cursor)
		sep = "&"
	}
	if openDotaKey != "" {
		endpoint += sep + "api_key=" + url.QueryEscape(openDotaKey)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Dota2ReplayCourier-CentralServer/1.2")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opendota publicMatches returned status %d", resp.StatusCode)
	}

	var rows []struct {
		MatchID     uint64 `json:"match_id"`
		MatchSeqNum uint64 `json:"match_seq_num"`
		RadiantWin  bool   `json:"radiant_win"`
		StartTime   int64  `json:"start_time"`
		Duration    int    `json:"duration"`
		LobbyType   int    `json:"lobby_type"`
		GameMode    int    `json:"game_mode"`
		Cluster     uint32 `json:"cluster"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}

	var minMatchID uint64
	out := make([]models.RawMatchCandidate, 0, len(rows))
	for _, r := range rows {
		if minMatchID == 0 || (r.MatchID > 0 && r.MatchID < minMatchID) {
			minMatchID = r.MatchID
		}
		cluster := r.Cluster
		if cluster == 0 {
			cluster = 133
		}
		out = append(out, models.RawMatchCandidate{
			MatchID:         r.MatchID,
			MatchSeqNum:     r.MatchSeqNum,
			Cluster:         cluster,
			LobbyType:       r.LobbyType,
			GameMode:        r.GameMode,
			HumanPlayers:    10,
			DurationSeconds: r.Duration,
			RadiantWin:      r.RadiantWin,
			StartTime:       r.StartTime,
			DiscoveredAt:    time.Now().UTC(),
		})
	}

	if minMatchID > 0 {
		h.mu.Lock()
		h.openDotaCursor = minMatchID
		h.mu.Unlock()
	}

	return out, nil
}

func (h *Harvester) isSeen(matchID uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.seenMatches[matchID]
	return ok
}

func (h *Harvester) markSeen(matchID uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seenMatches[matchID] = time.Now().UTC()
}

func (h *Harvester) pruneSeenCache() {
	h.mu.Lock()
	defer h.mu.Unlock()
	cutoff := time.Now().UTC().Add(-2 * time.Hour)
	for id, ts := range h.seenMatches {
		if ts.Before(cutoff) {
			delete(h.seenMatches, id)
		}
	}
}
