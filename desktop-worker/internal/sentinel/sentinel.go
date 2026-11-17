package sentinel

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/dota2-analytics/desktop-worker/internal/config"
)

// Snapshot represents the current OS environment state observed by Gamer Guard.
type Snapshot struct {
	GameRunning     bool      `json:"game_running"`
	DetectedGame    string    `json:"detected_game"`
	UserIdleSeconds int       `json:"user_idle_seconds"`
	IsUserIdle      bool      `json:"is_user_idle"`
	EcoPriority     bool      `json:"eco_priority"`
	CheckedAt       time.Time `json:"checked_at"`
}

// Sentinel monitors running game processes and user keyboard/mouse inactivity across Windows, macOS, and Linux.
type Sentinel struct {
	cfgMgr *config.Manager

	mu           sync.RWMutex
	lastSnapshot Snapshot
	listeners    []func(prev, curr Snapshot)
}

// NewSentinel creates a new Gamer Guard Sentinel.
func NewSentinel(cfgMgr *config.Manager) *Sentinel {
	// Default the worker process to below-normal priority so foreground apps always take precedence.
	_ = setOSProcessEcoPriority(false)
	s := &Sentinel{
		cfgMgr: cfgMgr,
	}
	s.probeOnce()
	return s
}

// OnChange registers a callback invoked whenever the Sentinel detects a state transition (e.g. a game launches or closes).
func (s *Sentinel) OnChange(fn func(prev, curr Snapshot)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, fn)
}

// GetSnapshot returns the most recent Gamer Guard snapshot.
func (s *Sentinel) GetSnapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastSnapshot
}

// Start runs the background Sentinel loop every 3 seconds in a goroutine until ctx is canceled.
func (s *Sentinel) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.probeOnce()
			}
		}
	}()
}

// ProbeNow triggers an immediate process and user-idle check and returns the resulting Snapshot.
func (s *Sentinel) ProbeNow() Snapshot {
	return s.probeOnce()
}

func (s *Sentinel) probeOnce() Snapshot {
	cfg := s.cfgMgr.Get()

	detectedGame := ""
	if cfg.PauseOnGaming && len(cfg.WatchedProcesses) > 0 {
		detectedGame = detectWatchedGameProcess(cfg.WatchedProcesses)
	}
	gameRunning := detectedGame != ""

	idleDur, err := getOSUserIdleDuration()
	idleSec := 0
	if err == nil && idleDur > 0 {
		idleSec = int(idleDur.Seconds())
	}

	threshold := cfg.IdleThresholdSeconds
	if threshold <= 0 {
		threshold = 300
	}
	isIdle := idleSec >= threshold

	// Adjust OS process priority to IDLE when a game is running in Eco mode
	ecoMode := gameRunning && cfg.GamingParseBehavior == config.BehaviorEcoSlowdown
	_ = setOSProcessEcoPriority(ecoMode)

	curr := Snapshot{
		GameRunning:     gameRunning,
		DetectedGame:    detectedGame,
		UserIdleSeconds: idleSec,
		IsUserIdle:      isIdle,
		EcoPriority:     ecoMode,
		CheckedAt:       time.Now().UTC(),
	}

	s.mu.Lock()
	prev := s.lastSnapshot
	s.lastSnapshot = curr
	cbs := append([]func(prev, curr Snapshot){}, s.listeners...)
	s.mu.Unlock()

	if prev.GameRunning != curr.GameRunning || prev.IsUserIdle != curr.IsUserIdle {
		for _, fn := range cbs {
			fn(prev, curr)
		}
	}

	return curr
}

// NotifyUser sends a native desktop notification (Windows Toast / macOS Notification / Linux notify-send).
func (s *Sentinel) NotifyUser(title, message string) {
	go func() {
		_ = sendOSDesktopNotification(title, message)
	}()
}

// matchProcessName checks if any running process matches the user's watched games (case-insensitive).
func matchProcessName(runningNames []string, watchlist []string) string {
	entries := make([]struct {
		clean string
		orig  string
	}, 0, len(watchlist))
	for _, w := range watchlist {
		orig := strings.TrimSpace(w)
		clean := strings.TrimSuffix(strings.ToLower(orig), ".exe")
		if clean != "" {
			entries = append(entries, struct {
				clean string
				orig  string
			}{clean: clean, orig: orig})
		}
	}
	for _, proc := range runningNames {
		pLower := strings.ToLower(strings.TrimSpace(proc))
		pBase := strings.TrimSuffix(pLower, ".exe")
		for _, e := range entries {
			if pLower == e.clean || pBase == e.clean || (len(e.clean) >= 3 && strings.Contains(pBase, e.clean)) {
				return e.orig
			}
		}
	}
	return ""
}
