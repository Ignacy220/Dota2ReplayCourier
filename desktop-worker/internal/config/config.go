package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dota2-analytics/desktop-worker/internal/netprobe"
)

// GamingParseBehavior controls what happens if a game launches while a match is being parsed in RAM.
type GamingParseBehavior string

const (
	// BehaviorHoldInRAM pauses parsing and keeps the ~50MB compressed replay in RAM until the game exits.
	BehaviorHoldInRAM GamingParseBehavior = "HOLD_IN_RAM"
	// BehaviorEcoSlowdown lowers OS process priority to Idle and inserts sleep yields to finish parsing with minimal CPU.
	BehaviorEcoSlowdown GamingParseBehavior = "ECO_SLOWDOWN"
)

// IdlePolicy controls how user keyboard/mouse inactivity (GetLastInputInfo) affects scheduling.
type IdlePolicy string

const (
	IdlePolicyAlwaysRun     IdlePolicy = "ALWAYS_RUN"
	IdlePolicyTurboWhenIdle IdlePolicy = "TURBO_WHEN_IDLE"
	IdlePolicyOnlyWhenIdle  IdlePolicy = "ONLY_WHEN_IDLE"
)

// WorkerConfig holds all user-configurable settings and persistent contributor progression for the Desktop Worker.
type WorkerConfig struct {
	ServerURL               string                   `json:"server_url"`
	WorkerID                string                   `json:"worker_id"`
	PreferredRegion         string                   `json:"preferred_region"`
	RegionPriority          []string                 `json:"region_priority,omitempty"`
	RegionLatencies         []netprobe.RegionLatency `json:"region_latencies,omitempty"`
	AuthToken               string                   `json:"auth_token,omitempty"`
	AccountNickname         string                   `json:"account_nickname,omitempty"`
	AccountFaction          string                   `json:"account_faction,omitempty"`
	PromptedAuth            bool                     `json:"prompted_auth"`
	AnonymousXP             uint64                   `json:"anonymous_xp,omitempty"`
	PauseOnGaming           bool                     `json:"pause_on_gaming"`
	GamingParseBehavior     GamingParseBehavior      `json:"gaming_parse_behavior"`
	IdlePolicy              IdlePolicy               `json:"idle_policy"`
	IdleThresholdSeconds    int                      `json:"idle_threshold_seconds"`
	WatchedProcesses        []string                 `json:"watched_processes"`
	MaxDailyBandwidthMB     int                      `json:"max_daily_bandwidth_mb"`
	DownloadSpeedLimitKBps  int                      `json:"download_speed_limit_kbps"`
	UsedBandwidthTodayBytes uint64                   `json:"used_bandwidth_today_bytes"`
	BandwidthResetDate      string                   `json:"bandwidth_reset_date"`
	ParsedTodayCount        uint64                   `json:"parsed_today_count"`
	TotalParsedAllTime      uint64                   `json:"total_parsed_all_time"`
	ContributorXP           uint64                   `json:"contributor_xp"`
	WardsExtractedTotal     uint64                   `json:"wards_extracted_total"`
	StreakDays              int                      `json:"streak_days"`
	LastActiveDate          string                   `json:"last_active_date"`
	StratzAPIKey            string                   `json:"stratz_api_key"`
	OpenDotaAPIKey          string                   `json:"opendota_api_key"`
}

// DefaultConfig returns sensible default settings for the Desktop Worker.
func DefaultConfig() WorkerConfig {
	today := time.Now().Format("2006-01-02")
	return WorkerConfig{
		ServerURL:            "",
		WorkerID:             generateWorkerID(),
		PreferredRegion:      "AUTO",
		PauseOnGaming:        true,
		GamingParseBehavior:  BehaviorHoldInRAM,
		IdlePolicy:           IdlePolicyTurboWhenIdle,
		IdleThresholdSeconds: 300, // 5 minutes
		WatchedProcesses: []string{
			"dota2",
			"cs2",
			"valorant",
		},
		MaxDailyBandwidthMB:    10240, // 10 GB/day default
		DownloadSpeedLimitKBps: 0,     // 0 = unlimited
		BandwidthResetDate:     today,
		LastActiveDate:         today,
		StreakDays:             1,
	}
}

func generateWorkerID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "worker-" + hex.EncodeToString(b)
}

func cloneConfig(c WorkerConfig) WorkerConfig {
	if len(c.WatchedProcesses) > 0 {
		c.WatchedProcesses = append([]string(nil), c.WatchedProcesses...)
	}
	if len(c.RegionPriority) > 0 {
		c.RegionPriority = append([]string(nil), c.RegionPriority...)
	}
	if len(c.RegionLatencies) > 0 {
		c.RegionLatencies = append([]netprobe.RegionLatency(nil), c.RegionLatencies...)
	}
	return c
}

// Manager provides thread-safe access and disk persistence for WorkerConfig.
type Manager struct {
	mu        sync.RWMutex
	cfg       WorkerConfig
	persisted WorkerConfig
	filePath  string
}

// NewManager loads or initializes WorkerConfig from the OS user config directory.
func NewManager(customPath string) *Manager {
	path := customPath
	if path == "" {
		dir, err := os.UserConfigDir()
		if err != nil {
			dir = "."
		}
		path = filepath.Join(dir, "Dota2ReplayCourier", "config.json")
	}

	def := DefaultConfig()
	m := &Manager{
		cfg:       cloneConfig(def),
		persisted: cloneConfig(def),
		filePath:  path,
	}
	m.load()
	m.checkDailyResetLocked()
	m.persisted = cloneConfig(m.cfg)
	return m
}

func (m *Manager) load() {
	raw, err := os.ReadFile(m.filePath)
	if err != nil {
		return
	}
	var loaded WorkerConfig
	if err := json.Unmarshal(raw, &loaded); err == nil {
		def := DefaultConfig()
		m.cfg = loaded
		if m.cfg.WorkerID == "" {
			m.cfg.WorkerID = generateWorkerID()
		}
		if m.cfg.PreferredRegion == "" {
			m.cfg.PreferredRegion = def.PreferredRegion
		}
		if m.cfg.GamingParseBehavior == "" {
			m.cfg.GamingParseBehavior = def.GamingParseBehavior
		}
		if m.cfg.IdlePolicy == "" {
			m.cfg.IdlePolicy = def.IdlePolicy
		}
		if m.cfg.MaxDailyBandwidthMB < 0 {
			m.cfg.MaxDailyBandwidthMB = 0
		}
		if m.cfg.AuthToken == "" && m.cfg.AnonymousXP == 0 && m.cfg.ContributorXP > 0 {
			m.cfg.AnonymousXP = m.cfg.ContributorXP
		}
		if m.cfg.StreakDays <= 0 {
			m.cfg.StreakDays = 1
		}
	}
}

// Save writes the persisted configuration to disk.
func (m *Manager) Save() error {
	m.mu.RLock()
	raw, err := json.MarshalIndent(m.persisted, "", "  ")
	m.mu.RUnlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.filePath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(m.filePath, raw, 0o644)
}

// Get returns a snapshot of the current configuration.
func (m *Manager) Get() WorkerConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkDailyResetLocked()
	return cloneConfig(m.cfg)
}

// Update modifies the configuration and persists it to disk.
func (m *Manager) Update(fn func(*WorkerConfig)) WorkerConfig {
	m.mu.Lock()
	m.checkDailyResetLocked()
	fn(&m.cfg)
	fn(&m.persisted)
	snapshot := cloneConfig(m.cfg)
	m.mu.Unlock()
	_ = m.Save()
	return snapshot
}

// UpdateEphemeral modifies the active in-memory configuration without persisting to disk.
func (m *Manager) UpdateEphemeral(fn func(*WorkerConfig)) WorkerConfig {
	m.mu.Lock()
	m.checkDailyResetLocked()
	fn(&m.cfg)
	snapshot := cloneConfig(m.cfg)
	m.mu.Unlock()
	return snapshot
}

// SaveCurrentToDisk copies the active in-memory configuration (including session overrides) to disk.
func (m *Manager) SaveCurrentToDisk() error {
	m.mu.Lock()
	m.checkDailyResetLocked()
	m.persisted = cloneConfig(m.cfg)
	m.mu.Unlock()
	return m.Save()
}

// AddBandwidthBytes records downloaded bytes against today's bandwidth quota.
func (m *Manager) AddBandwidthBytes(n uint64) {
	m.mu.Lock()
	m.checkDailyResetLocked()
	m.cfg.UsedBandwidthTodayBytes += n
	m.persisted.UsedBandwidthTodayBytes += n
	m.mu.Unlock()
}

// RecordMatchParsed increments daily and all-time counters and awards contributor XP.
func (m *Manager) RecordMatchParsed(xpGain uint64, wardsCount uint64) uint64 {
	m.mu.Lock()
	m.checkDailyResetLocked()
	applyParsed := func(c *WorkerConfig) uint64 {
		c.ParsedTodayCount++
		c.TotalParsedAllTime++
		c.ContributorXP += xpGain
		if c.AuthToken == "" {
			c.AnonymousXP = c.ContributorXP
		}
		c.WardsExtractedTotal += wardsCount
		return c.ParsedTodayCount
	}
	val := applyParsed(&m.cfg)
	_ = applyParsed(&m.persisted)
	m.mu.Unlock()
	_ = m.Save()
	return val
}

// SetRegionProbeResults persists the sorted region latency results and priority list.
func (m *Manager) SetRegionProbeResults(latencies []netprobe.RegionLatency, priority []string) WorkerConfig {
	return m.Update(func(c *WorkerConfig) {
		c.RegionLatencies = append([]netprobe.RegionLatency(nil), latencies...)
		c.RegionPriority = append([]string(nil), priority...)
	})
}

// SetAuth stores an authenticated session token, nickname, permanent faction, and server-authoritative XP.
func (m *Manager) SetAuth(token, nickname, faction string, serverXP, totalMatches uint64) WorkerConfig {
	return m.Update(func(c *WorkerConfig) {
		if c.AuthToken == "" {
			c.AnonymousXP = c.ContributorXP
		}
		c.AuthToken = token
		c.AccountNickname = nickname
		c.AccountFaction = faction
		c.PromptedAuth = true
		c.ContributorXP = serverXP
		if totalMatches > c.TotalParsedAllTime {
			c.TotalParsedAllTime = totalMatches
		}
	})
}

// ClearAuth signs the local worker out and restores its local anonymous XP counter.
func (m *Manager) ClearAuth() WorkerConfig {
	return m.Update(func(c *WorkerConfig) {
		c.AuthToken = ""
		c.AccountNickname = ""
		c.AccountFaction = ""
		c.ContributorXP = c.AnonymousXP
	})
}

// MarkAuthPrompted records that the user dismissed or responded to the first-startup account modal.
func (m *Manager) MarkAuthPrompted() WorkerConfig {
	return m.Update(func(c *WorkerConfig) {
		c.PromptedAuth = true
	})
}

// SyncServerProfile updates cached XP, nickname, and faction from the authoritative server response.
func (m *Manager) SyncServerProfile(xp, totalMatches uint64, nickname, faction string) bool {
	m.mu.Lock()
	if m.cfg.AuthToken == "" {
		m.mu.Unlock()
		return false
	}
	changed := false
	syncTarget := func(c *WorkerConfig, track bool) {
		if c.ContributorXP != xp {
			c.ContributorXP = xp
			if track {
				changed = true
			}
		}
		if nickname != "" && c.AccountNickname != nickname {
			c.AccountNickname = nickname
			if track {
				changed = true
			}
		}
		if faction != "" && c.AccountFaction != faction {
			c.AccountFaction = faction
			if track {
				changed = true
			}
		}
		if totalMatches > c.TotalParsedAllTime {
			c.TotalParsedAllTime = totalMatches
			if track {
				changed = true
			}
		}
	}
	syncTarget(&m.cfg, true)
	syncTarget(&m.persisted, false)
	m.mu.Unlock()
	if changed {
		_ = m.Save()
	}
	return changed
}

// IncrementParsedToday increments the worker's daily parsed counter with default XP.
func (m *Manager) IncrementParsedToday() uint64 {
	return m.RecordMatchParsed(50, 3)
}

// HasBandwidthQuota returns true if today's bandwidth usage is below MaxDailyBandwidthMB.
func (m *Manager) HasBandwidthQuota() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkDailyResetLocked()
	if m.cfg.MaxDailyBandwidthMB <= 0 {
		return true
	}
	maxBytes := uint64(m.cfg.MaxDailyBandwidthMB) * 1024 * 1024
	return m.cfg.UsedBandwidthTodayBytes < maxBytes
}

func (m *Manager) checkDailyResetLocked() {
	today := time.Now().Format("2006-01-02")
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	applyReset := func(c *WorkerConfig) {
		if c.BandwidthResetDate != today {
			if c.LastActiveDate == yesterday {
				c.StreakDays++
			} else if c.LastActiveDate != today {
				c.StreakDays = 1
			}
			c.LastActiveDate = today
			c.BandwidthResetDate = today
			c.UsedBandwidthTodayBytes = 0
			c.ParsedTodayCount = 0
		}
	}
	applyReset(&m.cfg)
	applyReset(&m.persisted)
}

// ParserCacheDir returns the local directory used to cache verified server-assigned parser artifacts.
func (m *Manager) ParserCacheDir() string {
	return filepath.Join(filepath.Dir(m.filePath), "parsers")
}
