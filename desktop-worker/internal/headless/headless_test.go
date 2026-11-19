package headless

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dota2-analytics/desktop-worker/internal/client"
	"github.com/dota2-analytics/desktop-worker/internal/config"
	"github.com/dota2-analytics/desktop-worker/internal/downloader"
	"github.com/dota2-analytics/desktop-worker/internal/netprobe"
	"github.com/dota2-analytics/desktop-worker/internal/orchestrator"
	"github.com/dota2-analytics/desktop-worker/internal/sentinel"
)

func TestEphemeralStartupFlagsNotSavedEvenAfterRecordMatchParsed(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	cfgMgr := config.NewManager(cfgPath)
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = "https://saved.example.com"
		c.PreferredRegion = "EU"
		c.MaxDailyBandwidthMB = 1000
	})

	sent := sentinel.NewSentinel(cfgMgr)
	orch := orchestrator.NewOrchestrator(cfgMgr, sent, client.NewAPIClient(), downloader.NewDownloader())

	flags, err := ParseFlags([]string{
		"--server=https://session.example.com",
		"--region=NA",
		"--stratz-key=tmp-stratz-key",
		"--opendota-key=tmp-od-key",
		"--pause-on-gaming=false",
		"--gaming-behavior=ECO_SLOWDOWN",
		"--idle-policy=ONLY_WHEN_IDLE",
		"--idle-threshold=90",
		"--bandwidth-limit=2048",
		"--speed-limit=500",
		"--paused=true",
	})
	if err != nil {
		t.Fatalf("ParseFlags failed: %v", err)
	}
	if !flags.Headless {
		t.Fatalf("expected Headless=true automatically when flags are provided")
	}

	if err := ApplyStartupFlags(flags, cfgMgr, orch); err != nil {
		t.Fatalf("ApplyStartupFlags failed: %v", err)
	}

	activeCfg := cfgMgr.Get()
	if activeCfg.ServerURL != "https://session.example.com" || activeCfg.PreferredRegion != "NA" ||
		activeCfg.StratzAPIKey != "tmp-stratz-key" || activeCfg.MaxDailyBandwidthMB != 2048 ||
		activeCfg.PauseOnGaming || activeCfg.GamingParseBehavior != config.BehaviorEcoSlowdown ||
		activeCfg.IdlePolicy != config.IdlePolicyOnlyWhenIdle || activeCfg.IdleThresholdSeconds != 90 ||
		activeCfg.DownloadSpeedLimitKBps != 500 {
		t.Fatalf("unexpected active in-memory config: %+v", activeCfg)
	}
	if !orch.GetStatusSnapshot().UserPaused {
		t.Fatalf("expected --paused=true to set UserPaused=true")
	}

	// Simulate match parsed during session (saves counters to disk, must NOT leak ephemeral overrides).
	cfgMgr.RecordMatchParsed(50, 4)

	reloaded := config.NewManager(cfgPath).Get()
	if reloaded.TotalParsedAllTime != 1 || reloaded.ContributorXP != 50 {
		t.Fatalf("expected parsed counters to persist to disk, got total=%d xp=%d", reloaded.TotalParsedAllTime, reloaded.ContributorXP)
	}
	if reloaded.ServerURL != "https://saved.example.com" || reloaded.PreferredRegion != "EU" || reloaded.StratzAPIKey != "" {
		t.Fatalf("ephemeral startup flags leaked onto disk: %+v", reloaded)
	}

	// Explicit save persists current in-memory configuration.
	if err := cfgMgr.SaveCurrentToDisk(); err != nil {
		t.Fatalf("SaveCurrentToDisk failed: %v", err)
	}
	reloadedAfterSave := config.NewManager(cfgPath).Get()
	if reloadedAfterSave.ServerURL != "https://session.example.com" || reloadedAfterSave.PreferredRegion != "NA" || reloadedAfterSave.StratzAPIKey != "tmp-stratz-key" {
		t.Fatalf("expected SaveCurrentToDisk to persist session overrides, got %+v", reloadedAfterSave)
	}
}

func TestInteractiveCommandsAndQuit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stats":
			_ = json.NewEncoder(w).Encode(client.PublicStats{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	cfgMgr := config.NewManager(cfgPath)
	sent := sentinel.NewSentinel(cfgMgr)
	orch := orchestrator.NewOrchestrator(cfgMgr, sent, client.NewAPIClient(), downloader.NewDownloader())
	orch.SetRegionProber(func(ctx context.Context) ([]netprobe.RegionLatency, []string) {
		return []netprobe.RegionLatency{{Region: "EU", LatencyMs: 12, Reachable: true}}, []string{"EU"}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	script := strings.Join([]string{
		"help",
		"pause",
		"resume",
		"server " + srv.URL,
		"region SEA",
		"keys stratz sk_test_123",
		"guard off ECO_SLOWDOWN",
		"idle ONLY_WHEN_IDLE 120",
		"limits 4096 800",
		"status",
		"save",
		"quit",
	}, "\n") + "\n"

	var out bytes.Buffer
	Run(ctx, strings.NewReader(script), &out, cancel, cfgMgr, sent, orch)

	output := out.String()
	for _, want := range []string{
		"Worker paused",
		"Worker resumed",
		"Preferred region saved: SEA",
		"Saved STRATZ key (set=true)",
		"Gamer Guard saved: enabled=false behavior=ECO_SLOWDOWN",
		"Idle policy saved: ONLY_WHEN_IDLE (threshold=120s)",
		"Bandwidth limits saved: 4096 MB/day, 800 KB/s",
		"Saved current worker configuration to disk",
		"Shutting down Worker...",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}

	reloaded := config.NewManager(cfgPath).Get()
	if reloaded.ServerURL != srv.URL || reloaded.PreferredRegion != "SEA" ||
		reloaded.StratzAPIKey != "sk_test_123" || reloaded.PauseOnGaming ||
		reloaded.GamingParseBehavior != config.BehaviorEcoSlowdown ||
		reloaded.IdlePolicy != config.IdlePolicyOnlyWhenIdle ||
		reloaded.IdleThresholdSeconds != 120 ||
		reloaded.MaxDailyBandwidthMB != 4096 ||
		reloaded.DownloadSpeedLimitKBps != 800 {
		t.Fatalf("unexpected persisted worker config after interactive commands: %+v", reloaded)
	}
}

func TestSlickWorkerCLIFeatures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stats":
			_ = json.NewEncoder(w).Encode(client.PublicStats{})
		case "/api/v1/auth/login":
			_ = json.NewEncoder(w).Encode(client.AuthResponse{
				Token: "tok-alice",
				User: client.UserProfile{
					Nickname: "alice",
					Faction:  "radiant",
					XP:       500,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	cfgMgr := config.NewManager(cfgPath)
	sent := sentinel.NewSentinel(cfgMgr)
	orch := orchestrator.NewOrchestrator(cfgMgr, sent, client.NewAPIClient(), downloader.NewDownloader())
	orch.SetRegionProber(func(ctx context.Context) ([]netprobe.RegionLatency, []string) {
		return []netprobe.RegionLatency{{Region: "EU", LatencyMs: 12, Reachable: true}}, []string{"EU"}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Exercise setup wizard, inspect-by-default bare commands, aliases, prefix/fuzzy matching,
	// masked password login flow, logs/history, watch mode, and quit.
	script := strings.Join([]string{
		"setup",
		srv.URL, // Coordinator URL prompt in setup wizard
		"l",     // Choose login in setup wizard
		"alice", // Nickname prompt
		"secret123", // Password prompt
		"server",
		"region",
		"guard",
		"idle",
		"limits",
		"keys",
		"s",
		"p",
		"p",
		"sttaus",
		"reg EU",
		"lim 2048",
		"limits 0 0",
		"limits",
		"l off",
		"l errors",
		"l on",
		"l 5",
		"history",
		"!!",
		"w",
		"",
		"q",
	}, "\n") + "\n"

	var out bytes.Buffer
	Run(ctx, strings.NewReader(script), &out, cancel, cfgMgr, sent, orch)

	output := out.String()
	for _, want := range []string{
		"First-run setup",
		"Signed in as alice (radiant, 500 XP)",
		"Server: " + srv.URL,
		"Preferred region: AUTO",
		"Gamer Guard: enabled=true",
		"Idle policy:",
		"Bandwidth limits:",
		"Local Salt Keys:",
		"CONNECTION",
		"ACCOUNT",
		"Worker paused",
		"Worker resumed",
		`did you mean "status"`,
		`ambiguous command "reg"`,
		"Bandwidth limits saved: 2048 MB/day",
		"Bandwidth limits saved: 0 MB/day (unlimited), 0 KB/s (unlimited)",
		"Background logs mode: off",
		"Background logs mode: errors",
		"Background logs mode: on",
		"[WATCH]",
		"Shutting down Worker...",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}

	// Verify password did NOT leak into command history output.
	if strings.Contains(output, "secret123") {
		t.Fatalf("expected plaintext password not to appear in output/history")
	}

	// Verify 0 (unlimited) persisted across reload and satisfies quota check.
	reloadedMgr := config.NewManager(cfgPath)
	reloaded := reloadedMgr.Get()
	if reloaded.MaxDailyBandwidthMB != 0 || reloaded.DownloadSpeedLimitKBps != 0 {
		t.Fatalf("expected unlimited (0/0) limits to persist across reload, got daily=%d speed=%d",
			reloaded.MaxDailyBandwidthMB, reloaded.DownloadSpeedLimitKBps)
	}
	if !reloadedMgr.HasBandwidthQuota() {
		t.Fatalf("expected HasBandwidthQuota()=true when MaxDailyBandwidthMB=0")
	}
}

func TestWorkerRawLineEditorAndHistory(t *testing.T) {
	var out bytes.Buffer
	sess := &session{
		br:        bufio.NewReader(strings.NewReader("re\t\r\n\x1b[A\r\n")),
		out:       &out,
		prompt:    "courier> ",
		rawMode:   true,
		ansi:      false,
		logMode:   "on",
		completer: workerCompleter,
	}

	// "re" matches both "region", "regions", "register", "resume" -> Wait, "reg" matches "region" and "register".
	// Let's test unambiguous "gu\t\r\n" -> "guard " and Up arrow recall.
	sess.br = bufio.NewReader(strings.NewReader("gu\t\r\n\x1b[A\r\n"))
	line1, err := sess.readRawLine("courier> ", false)
	if err != nil {
		t.Fatalf("readRawLine #1 error: %v", err)
	}
	if strings.TrimSpace(line1) != "guard" {
		t.Fatalf("expected Tab completion of 'gu' -> 'guard', got %q", line1)
	}
	sess.AddHistory(strings.TrimSpace(line1))

	line2, err := sess.readRawLine("courier> ", false)
	if err != nil {
		t.Fatalf("readRawLine #2 error: %v", err)
	}
	if strings.TrimSpace(line2) != "guard" {
		t.Fatalf("expected Up arrow history recall 'guard', got %q", line2)
	}
}

