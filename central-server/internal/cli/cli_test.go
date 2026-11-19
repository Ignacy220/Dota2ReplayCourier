package cli

import (
	"bufio"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dota2-analytics/central-server/internal/accounts"
	"github.com/dota2-analytics/central-server/internal/api"
	"github.com/dota2-analytics/central-server/internal/harvester"
	"github.com/dota2-analytics/central-server/internal/models"
	"github.com/dota2-analytics/central-server/internal/parsers"
	"github.com/dota2-analytics/central-server/internal/queue"
	"github.com/dota2-analytics/central-server/internal/resolver"
	"github.com/dota2-analytics/central-server/internal/storage"
	"github.com/redis/go-redis/v9"
)

func setupTestDeps(t *testing.T) (Dependencies, string, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	leaseMgr := queue.NewLeaseManager(rdb, 5*time.Minute)
	filterEngine := harvester.NewFilterEngine(models.DefaultFilterConfig())
	harv := harvester.NewHarvester("", filterEngine, leaseMgr, resolver.NewMultiResolver("", ""))

	tmpDir := t.TempDir()
	store := storage.NewAnalyticsStore(filepath.Join(tmpDir, "matches"), "")
	accStore, err := accounts.NewStore(filepath.Join(tmpDir, "accounts.db"))
	if err != nil {
		mr.Close()
		t.Fatalf("accounts.NewStore: %v", err)
	}
	parserMgr, err := parsers.NewManager(filepath.Join(tmpDir, "parsers"))
	if err != nil {
		accStore.Close()
		mr.Close()
		t.Fatalf("parsers.NewManager: %v", err)
	}

	srv := api.NewServer(leaseMgr, filterEngine, harv, store, accStore, parserMgr, "")
	srv.SetAPIKeysPath(filepath.Join(tmpDir, "api_keys.json"))

	cleanup := func() {
		accStore.Close()
		mr.Close()
	}

	return Dependencies{
		FilterEngine: filterEngine,
		Harvester:    harv,
		Store:        store,
		LeaseMgr:     leaseMgr,
		ParserMgr:    parserMgr,
		Server:       srv,
		DataDir:      tmpDir,
		BaseURL:      "http://localhost:8080",
	}, tmpDir, cleanup
}

func TestParseAndApplyStartupFlagsEphemeral(t *testing.T) {
	deps, tmpDir, cleanup := setupTestDeps(t)
	defer cleanup()

	flags, err := ParseFlags([]string{
		"--harvester",
		"--steam-key=steam-sess-key",
		"--stratz-key=stratz-sess-key",
		"--regions=EU,NA",
		"--lobby-types=7,0",
		"--game-modes=22",
		"--min-duration=1500",
		"--max-duration=3600",
		"--require-10-humans=false",
		"--file-storage=false",
		"--webhook-url=https://example.com/hook",
		"--webhook=true",
		"--parser=default",
	})
	if err != nil {
		t.Fatalf("ParseFlags failed: %v", err)
	}
	if !flags.Headless {
		t.Fatalf("expected Headless=true automatically when flags are provided")
	}

	if err := ApplyStartupFlags(flags, deps.FilterEngine, deps.Harvester, deps.Store, deps.ParserMgr, deps.Server); err != nil {
		t.Fatalf("ApplyStartupFlags failed: %v", err)
	}

	// Verify /admin and /api/v1/admin/overview are disabled in headless mode, while /api/v1/stats works.
	respAdmin, err := deps.Server.App().Test(httptest.NewRequest(http.MethodGet, "/admin", nil), -1)
	if err != nil || respAdmin.StatusCode != http.StatusNotFound {
		t.Fatalf("expected GET /admin -> 404 in headless mode, got status=%d err=%v", respAdmin.StatusCode, err)
	}
	respAdminAPI, err := deps.Server.App().Test(httptest.NewRequest(http.MethodGet, "/api/v1/admin/overview", nil), -1)
	if err != nil || respAdminAPI.StatusCode != http.StatusNotFound {
		t.Fatalf("expected GET /api/v1/admin/overview -> 404 in headless mode, got status=%d err=%v", respAdminAPI.StatusCode, err)
	}
	respStats, err := deps.Server.App().Test(httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil), -1)
	if err != nil || respStats.StatusCode != http.StatusOK {
		t.Fatalf("expected GET /api/v1/stats -> 200 in headless mode, got status=%d err=%v", respStats.StatusCode, err)
	}

	// Verify in-memory overrides applied.
	fCfg := deps.FilterEngine.GetConfig()
	if !fCfg.Enabled || fCfg.MinDurationSeconds != 1500 || fCfg.Require10Humans {
		t.Fatalf("unexpected filter config after startup flags: %+v", fCfg)
	}
	steam, stratz, _ := deps.Harvester.GetAPIKeys()
	if steam != "steam-sess-key" || stratz != "stratz-sess-key" {
		t.Fatalf("unexpected API keys after startup flags: steam=%q stratz=%q", steam, stratz)
	}
	sink := deps.Store.GetSinkStats()
	if sink.FileStorageEnabled || !sink.WebhookEnabled || sink.WebhookURL != "https://example.com/hook" {
		t.Fatalf("unexpected sink config after startup flags: %+v", sink)
	}

	// Verify startup flags did NOT persist to disk (including no _sink_config.json leak).
	for _, fname := range []string{"api_keys.json", "filter_config.json", "sink_config.json", filepath.Join("matches", "_sink_config.json")} {
		if _, err := os.Stat(filepath.Join(tmpDir, fname)); !os.IsNotExist(err) {
			t.Fatalf("expected %s not to be written by startup flags", fname)
		}
	}
}

func TestInteractiveCommandsAndPersistence(t *testing.T) {
	deps, tmpDir, cleanup := setupTestDeps(t)
	defer cleanup()
	authPath := filepath.Join(tmpDir, "admin_auth.json")
	deps.Server.EnableAdminPasswordAuth(authPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopped := false
	stop := func() {
		stopped = true
		cancel()
	}

	script := strings.Join([]string{
		"help",
		"harvester on",
		"keys steam my-steam-key",
		"filter regions NA,SEA",
		"filter min-duration 1800",
		"file-storage off",
		"webhook https://example.com/cb",
		"webhook on",
		"parser list",
		"parser activate default",
		"password cliAdminPass99",
		"history",
		"inject 991122 133",
		"status",
		"save",
		"quit",
	}, "\n") + "\n"

	var out bytes.Buffer
	RunInteractiveLoop(ctx, strings.NewReader(script), &out, stop, deps)

	if !stopped {
		t.Fatalf("expected 'quit' to trigger stop callback")
	}
	output := out.String()
	if !strings.Contains(output, "Harvester is now ON") || !strings.Contains(output, "Enqueued job: match_id=991122") {
		t.Fatalf("unexpected CLI output:\n%s", output)
	}
	if strings.Contains(output, "cliAdminPass99") {
		t.Fatalf("expected plaintext admin password not to appear in history output")
	}
	if authRaw, err := os.ReadFile(authPath); err != nil || bytes.Contains(authRaw, []byte("cliAdminPass99")) || !bytes.Contains(authRaw, []byte("$2")) {
		t.Fatalf("expected hashed admin_auth.json on disk, got err=%v content=%s", err, authRaw)
	}

	steam, _, _, okKeys := api.LoadPersistedAPIKeys(filepath.Join(tmpDir, "api_keys.json"))
	if !okKeys || steam != "my-steam-key" {
		t.Fatalf("expected persisted steam key, got ok=%v steam=%q", okKeys, steam)
	}

	fCfg, okFilter := api.LoadPersistedFilterConfig(filepath.Join(tmpDir, "filter_config.json"))
	if !okFilter || fCfg.Enabled || fCfg.MinDurationSeconds != 1800 || len(fCfg.AllowedRegions) != 2 {
		t.Fatalf("expected persisted filter config with Enabled=false, got ok=%v cfg=%+v", okFilter, fCfg)
	}

	whURL, whOn, fsOn, okSink := api.LoadPersistedSinkConfig(filepath.Join(tmpDir, "sink_config.json"))
	if !okSink || whURL != "https://example.com/cb" || !whOn || fsOn {
		t.Fatalf("expected persisted sink config, got ok=%v url=%q wh=%v fs=%v", okSink, whURL, whOn, fsOn)
	}

	depths, _, err := deps.LeaseMgr.GetQueueDepths(context.Background())
	if err != nil {
		t.Fatalf("GetQueueDepths failed: %v", err)
	}
	if depths[models.RegionEU] < 1 {
		t.Fatalf("expected injected cluster 133 job in EU queue, got %+v", depths)
	}
}

func TestSlickCLIFeatures(t *testing.T) {
	deps, _, cleanup := setupTestDeps(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	script := strings.Join([]string{
		"harvester",
		"file-storage",
		"webhook",
		"keys",
		"filter",
		"parser",
		"s",
		"h",
		"harv off",
		"fil show",
		"sttaus",
		"i 3",
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
	RunInteractiveLoop(ctx, strings.NewReader(script), &out, cancel, deps)

	output := out.String()
	for _, want := range []string{
		"Harvester is OFF",
		"Local file storage is ON",
		"Webhook: enabled=OFF",
		"API Keys:",
		"Filter: regions=",
		"Active Parser: default",
		"HARVESTER",
		"Harvester is now ON",
		"Harvester is now OFF",
		`did you mean "status"`,
		"Enqueued 3 sample jobs",
		"Background logs mode: off",
		"Background logs mode: errors",
		"Background logs mode: on",
		"[WATCH] Harvester: OFF",
		"Shutting down Coordinator...",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}

	depths, _, err := deps.LeaseMgr.GetQueueDepths(context.Background())
	if err != nil {
		t.Fatalf("GetQueueDepths failed: %v", err)
	}
	if depths[models.RegionEU] != 3 {
		t.Fatalf("expected 3 sample jobs from 'i 3', got %+v", depths)
	}
}

func TestRawLineEditorHistoryAndTab(t *testing.T) {
	var out bytes.Buffer
	sess := &session{
		br:        bufio.NewReader(strings.NewReader("st\t\r\n\x1b[A\r\n")),
		out:       &out,
		prompt:    "server> ",
		rawMode:   true,
		ansi:      false,
		logMode:   "on",
		completer: func(line string) []string { return serverCompleter(Dependencies{}, line) },
	}

	// "st\t\r\n" should autocomplete "status "
	line1, err := sess.readRawLine("server> ", false)
	if err != nil {
		t.Fatalf("readRawLine #1 error: %v", err)
	}
	if strings.TrimSpace(line1) != "status" {
		t.Fatalf("expected Tab completion of 'st' -> 'status', got %q", line1)
	}
	sess.AddHistory(strings.TrimSpace(line1))

	// "\x1b[A\r\n" (Up arrow) should recall "status"
	line2, err := sess.readRawLine("server> ", false)
	if err != nil {
		t.Fatalf("readRawLine #2 error: %v", err)
	}
	if strings.TrimSpace(line2) != "status" {
		t.Fatalf("expected Up arrow history recall 'status', got %q", line2)
	}
}

