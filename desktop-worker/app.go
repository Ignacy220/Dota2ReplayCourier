package main

import (
	"context"
	"strings"

	"github.com/dota2-analytics/desktop-worker/internal/config"
	"github.com/dota2-analytics/desktop-worker/internal/orchestrator"
	"github.com/dota2-analytics/desktop-worker/internal/sentinel"
)

// PreferencesUpdate represents user-editable settings from the Desktop Worker UI.
type PreferencesUpdate struct {
	ServerURL              string   `json:"server_url"`
	PreferredRegion        string   `json:"preferred_region"`
	PauseOnGaming          bool     `json:"pause_on_gaming"`
	GamingParseBehavior    string   `json:"gaming_parse_behavior"`
	IdlePolicy             string   `json:"idle_policy"`
	IdleThresholdSeconds   int      `json:"idle_threshold_seconds"`
	WatchedProcesses       []string `json:"watched_processes"`
	MaxDailyBandwidthMB    int      `json:"max_daily_bandwidth_mb"`
	DownloadSpeedLimitKBps int      `json:"download_speed_limit_kbps"`
	StratzAPIKey           string   `json:"stratz_api_key"`
	OpenDotaAPIKey         string   `json:"opendota_api_key"`
}

// App is the Wails-bound controller for the Desktop Worker UI.
type App struct {
	ctx      context.Context
	cfgMgr   *config.Manager
	sentinel *sentinel.Sentinel
	orch     *orchestrator.Orchestrator
}

// NewApp creates a new Desktop Worker App instance.
func NewApp(cfgMgr *config.Manager, sent *sentinel.Sentinel, orch *orchestrator.Orchestrator) *App {
	return &App{
		cfgMgr:   cfgMgr,
		sentinel: sent,
		orch:     orch,
	}
}

// Startup is called when the desktop application starts.
func (a *App) Startup(ctx context.Context) {
	a.ctx = ctx
	a.sentinel.Start(ctx)
	a.orch.Start(ctx)
}

// GetStatus returns the current worker status snapshot for the UI.
func (a *App) GetStatus() orchestrator.StatusSnapshot {
	return a.orch.GetStatusSnapshot()
}

// TogglePause pauses or resumes the worker manually.
func (a *App) TogglePause() orchestrator.StatusSnapshot {
	a.orch.ToggleUserPaused()
	return a.orch.GetStatusSnapshot()
}

// TestServerConnection updates the server URL (if non-empty), immediately probes the server, and returns the status snapshot.
func (a *App) TestServerConnection(serverURL string) orchestrator.StatusSnapshot {
	clean := strings.TrimSpace(serverURL)
	if clean != "" {
		if !strings.HasPrefix(clean, "http://") && !strings.HasPrefix(clean, "https://") {
			clean = "http://" + clean
		}
		clean = strings.TrimRight(clean, "/")
		a.cfgMgr.Update(func(c *config.WorkerConfig) {
			c.ServerURL = clean
		})
	}
	a.orch.ProbeServerNow()
	return a.orch.GetStatusSnapshot()
}

// SavePreferences updates and persists the user's Gamer Guard, bandwidth, and server URL settings.
func (a *App) SavePreferences(update PreferencesUpdate) orchestrator.StatusSnapshot {
	a.cfgMgr.Update(func(c *config.WorkerConfig) {
		if s := strings.TrimSpace(update.ServerURL); s != "" {
			if !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
				s = "http://" + s
			}
			c.ServerURL = strings.TrimRight(s, "/")
		} else {
			c.ServerURL = ""
		}
		if update.PreferredRegion != "" {
			c.PreferredRegion = update.PreferredRegion
		}
		c.PauseOnGaming = update.PauseOnGaming
		if update.GamingParseBehavior != "" {
			c.GamingParseBehavior = config.GamingParseBehavior(update.GamingParseBehavior)
		}
		if update.IdlePolicy != "" {
			c.IdlePolicy = config.IdlePolicy(update.IdlePolicy)
		}
		if update.IdleThresholdSeconds > 0 {
			c.IdleThresholdSeconds = update.IdleThresholdSeconds
		}
		if len(update.WatchedProcesses) > 0 {
			clean := make([]string, 0, len(update.WatchedProcesses))
			for _, p := range update.WatchedProcesses {
				if trimmed := strings.TrimSpace(p); trimmed != "" {
					clean = append(clean, trimmed)
				}
			}
			c.WatchedProcesses = clean
		}
		if update.MaxDailyBandwidthMB >= 0 {
			c.MaxDailyBandwidthMB = update.MaxDailyBandwidthMB
		}
		if update.DownloadSpeedLimitKBps >= 0 {
			c.DownloadSpeedLimitKBps = update.DownloadSpeedLimitKBps
		}
		c.StratzAPIKey = strings.TrimSpace(update.StratzAPIKey)
		c.OpenDotaAPIKey = strings.TrimSpace(update.OpenDotaAPIKey)
	})

	a.orch.ResetSaltQuota()
	a.sentinel.ProbeNow()
	a.orch.ProbeServerNow()
	return a.orch.GetStatusSnapshot()
}

// AuthRegister creates a new contributor account on the Central Server and signs in immediately.
func (a *App) AuthRegister(nickname, password, faction string) (orchestrator.StatusSnapshot, error) {
	if err := a.orch.RegisterAccount(context.Background(), nickname, password, faction); err != nil {
		return a.orch.GetStatusSnapshot(), err
	}
	return a.orch.GetStatusSnapshot(), nil
}

// AuthLogin signs into an existing contributor account and syncs its server-authoritative XP.
func (a *App) AuthLogin(nickname, password string) (orchestrator.StatusSnapshot, error) {
	if err := a.orch.LoginAccount(context.Background(), nickname, password); err != nil {
		return a.orch.GetStatusSnapshot(), err
	}
	return a.orch.GetStatusSnapshot(), nil
}

// AuthLogout revokes the current session and reverts this worker to anonymous mode.
func (a *App) AuthLogout() orchestrator.StatusSnapshot {
	a.orch.LogoutAccount(context.Background())
	return a.orch.GetStatusSnapshot()
}

// DismissAuthPrompt dismisses the initial account creation prompt so the worker runs anonymously.
func (a *App) DismissAuthPrompt() orchestrator.StatusSnapshot {
	a.orch.DismissAuthPrompt()
	return a.orch.GetStatusSnapshot()
}

// ReprobeRegions re-measures Valve regional relay latencies and updates region scheduling priority.
func (a *App) ReprobeRegions() orchestrator.StatusSnapshot {
	a.orch.ProbeRegionsNow()
	return a.orch.GetStatusSnapshot()
}
