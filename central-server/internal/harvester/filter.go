package harvester

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dota2-analytics/central-server/internal/models"
)

// FilterEngine evaluates incoming matches against a live-configurable FilterConfig.
type FilterEngine struct {
	mu     sync.RWMutex
	config models.FilterConfig

	totalScanned      atomic.Uint64
	totalAccepted     atomic.Uint64
	droppedByLobby    atomic.Uint64
	droppedByGameMode atomic.Uint64
	droppedByDuration atomic.Uint64
	droppedByHumans   atomic.Uint64
	droppedByRegion   atomic.Uint64
	lastMatchSeqNum   atomic.Uint64
}

// NewFilterEngine creates a FilterEngine with the provided initial configuration.
func NewFilterEngine(cfg models.FilterConfig) *FilterEngine {
	if cfg.PollIntervalMs <= 0 {
		cfg.PollIntervalMs = 2000
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 10000
	}
	return &FilterEngine{
		config: cfg,
	}
}

// GetConfig returns a thread-safe copy of the current FilterConfig.
func (f *FilterEngine) GetConfig() models.FilterConfig {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cfg := f.config
	cfg.AllowedLobbyTypes = slices.Clone(f.config.AllowedLobbyTypes)
	cfg.AllowedGameModes = slices.Clone(f.config.AllowedGameModes)
	cfg.AllowedRegions = slices.Clone(f.config.AllowedRegions)
	return cfg
}

// UpdateConfig atomically updates the active FilterConfig.
func (f *FilterEngine) UpdateConfig(cfg models.FilterConfig) models.FilterConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cfg.PollIntervalMs < 500 {
		cfg.PollIntervalMs = 500
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 10000
	}
	if cfg.CDNDelaySeconds < 0 {
		cfg.CDNDelaySeconds = 0
	}
	cfg.UpdatedAt = time.Now().UTC()
	f.config = cfg
	return cfg
}

// SetLastSeqNum records the most recent match_seq_num processed.
func (f *FilterEngine) SetLastSeqNum(seq uint64) {
	f.lastMatchSeqNum.Store(seq)
}

// GetStats returns the current filtering metrics.
func (f *FilterEngine) GetStats() models.FilterStats {
	return models.FilterStats{
		TotalScanned:      f.totalScanned.Load(),
		TotalAccepted:     f.totalAccepted.Load(),
		DroppedByLobby:    f.droppedByLobby.Load(),
		DroppedByGameMode: f.droppedByGameMode.Load(),
		DroppedByDuration: f.droppedByDuration.Load(),
		DroppedByHumans:   f.droppedByHumans.Load(),
		DroppedByRegion:   f.droppedByRegion.Load(),
		LastMatchSeqNum:   f.lastMatchSeqNum.Load(),
	}
}

// Evaluate checks if a RawMatchCandidate passes all active filter criteria.
// Returns (accepted, resolvedRegion, rejectReason).
func (f *FilterEngine) Evaluate(m models.RawMatchCandidate) (bool, models.MacroRegion, string) {
	f.totalScanned.Add(1)
	if m.MatchSeqNum > 0 {
		f.lastMatchSeqNum.Store(m.MatchSeqNum)
	}

	f.mu.RLock()
	cfg := f.config
	f.mu.RUnlock()

	if len(cfg.AllowedLobbyTypes) > 0 && !slices.Contains(cfg.AllowedLobbyTypes, m.LobbyType) {
		f.droppedByLobby.Add(1)
		return false, "", "lobby_type"
	}

	if len(cfg.AllowedGameModes) > 0 && !slices.Contains(cfg.AllowedGameModes, m.GameMode) {
		f.droppedByGameMode.Add(1)
		return false, "", "game_mode"
	}

	if cfg.Require10Humans && m.HumanPlayers != 10 {
		f.droppedByHumans.Add(1)
		return false, "", "human_players"
	}

	if cfg.MinDurationSeconds > 0 && m.DurationSeconds < cfg.MinDurationSeconds {
		f.droppedByDuration.Add(1)
		return false, "", "min_duration"
	}

	if cfg.MaxDurationSeconds > 0 && m.DurationSeconds > cfg.MaxDurationSeconds {
		f.droppedByDuration.Add(1)
		return false, "", "max_duration"
	}

	region := models.ResolveClusterRegion(m.Cluster)
	if len(cfg.AllowedRegions) > 0 && !slices.Contains(cfg.AllowedRegions, region) {
		f.droppedByRegion.Add(1)
		return false, region, "region"
	}

	f.totalAccepted.Add(1)
	return true, region, ""
}
