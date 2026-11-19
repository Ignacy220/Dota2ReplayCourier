package api

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dota2-analytics/central-server/internal/accounts"
	"github.com/dota2-analytics/central-server/internal/harvester"
	"github.com/dota2-analytics/central-server/internal/models"
	"github.com/dota2-analytics/central-server/internal/parsers"
	"github.com/dota2-analytics/central-server/internal/queue"
	"github.com/dota2-analytics/central-server/internal/resolver"
	"github.com/dota2-analytics/central-server/internal/storage"
	"github.com/redis/go-redis/v9"
)

func setupTestServer(t *testing.T, leaseTTL time.Duration, webhookURL string) (*Server, *queue.LeaseManager, string) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	tmpDir := t.TempDir()
	storageDir := filepath.Join(tmpDir, "matches")
	dbPath := filepath.Join(tmpDir, "accounts.db")
	parsersDir := filepath.Join(tmpDir, "parsers")

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	leaseMgr := queue.NewLeaseManager(rdb, leaseTTL)
	filterEngine := harvester.NewFilterEngine(models.DefaultFilterConfig())
	saltRes := resolver.NewMultiResolver("", "")
	harv := harvester.NewHarvester("", filterEngine, leaseMgr, saltRes)
	store := storage.NewAnalyticsStore(storageDir, webhookURL)
	accStore, err := accounts.NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite accounts store: %v", err)
	}
	t.Cleanup(func() { _ = accStore.Close() })

	parserMgr, err := parsers.NewManager(parsersDir)
	if err != nil {
		t.Fatalf("failed to create parsers manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store.Start(ctx)

	srv := NewServer(leaseMgr, filterEngine, harv, store, accStore, parserMgr, "")
	return srv, leaseMgr, storageDir
}

func TestDynamicFilterEngine(t *testing.T) {
	fe := harvester.NewFilterEngine(models.DefaultFilterConfig())

	ok, reg, reason := fe.Evaluate(models.RawMatchCandidate{
		MatchID:         7890000001,
		Cluster:         133,
		LobbyType:       7,
		GameMode:        22,
		HumanPlayers:    10,
		DurationSeconds: 2100,
	})
	if !ok || reg != models.RegionEU {
		t.Fatalf("expected match to be accepted in EU, got ok=%v reg=%s reason=%s", ok, reg, reason)
	}

	ok, _, reason = fe.Evaluate(models.RawMatchCandidate{
		MatchID:         7890000002,
		Cluster:         133,
		LobbyType:       7,
		GameMode:        23,
		HumanPlayers:    10,
		DurationSeconds: 1200,
	})
	if ok || reason != "game_mode" {
		t.Fatalf("expected turbo match to be rejected by game_mode, got ok=%v reason=%s", ok, reason)
	}

	cfg := fe.GetConfig()
	cfg.AllowedLobbyTypes = []int{0, 7}
	cfg.AllowedGameModes = []int{22, 23}
	fe.UpdateConfig(cfg)

	ok, _, reason = fe.Evaluate(models.RawMatchCandidate{
		MatchID:         7890000003,
		Cluster:         121,
		LobbyType:       0,
		GameMode:        23,
		HumanPlayers:    10,
		DurationSeconds: 1200,
	})
	if !ok {
		t.Fatalf("expected updated filter to accept match, got rejected by %s", reason)
	}
}

func TestLeaseReaperRecoversUnfinishedJobsWithoutDataLoss(t *testing.T) {
	ctx := context.Background()
	_, leaseMgr, _ := setupTestServer(t, 1*time.Second, "")

	for i := 1; i <= 2; i++ {
		err := leaseMgr.EnqueueReady(ctx, models.MatchJob{
			MatchID: uint64(7890001000 + i),
			Cluster: 133,
			Region:  models.RegionEU,
			URL:     "http://replay133.valve.net/570/test.dem.bz2",
		})
		if err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	leaseResp, err := leaseMgr.LeaseBatch(ctx, models.RegionEU, nil, "worker-1", "10.0.0.1", 5)
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}
	if len(leaseResp.Tasks) != 2 {
		t.Fatalf("expected 2 leased tasks, got %d", len(leaseResp.Tasks))
	}

	_, err = leaseMgr.SubmitBatchResults(ctx, models.SubmitBatchRequest{
		BatchID:       leaseResp.BatchID,
		WorkerID:      "worker-1",
		ClientVersion: "1.2.0",
		Results: []models.TaskResult{
			{
				MatchID: 7890001001,
				Status:  models.StatusSuccess,
				Payload: json.RawMessage(`{"arbitrary_blackbox_output": [1, 2, 3]}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("incremental submit failed: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)
	requeued, err := leaseMgr.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("reaper failed: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("expected exactly 1 unfinished job to be recovered by reaper, got %d", requeued)
	}

	leaseResp2, err := leaseMgr.LeaseBatch(ctx, models.RegionEU, nil, "worker-2", "10.0.0.2", 5)
	if err != nil {
		t.Fatalf("second lease failed: %v", err)
	}
	if len(leaseResp2.Tasks) != 1 || leaseResp2.Tasks[0].MatchID != 7890001002 {
		t.Fatalf("expected recovered match 7890001002, got %+v", leaseResp2.Tasks)
	}
}

func TestPerMatchFileStorageAndExternalWebhookForwarding(t *testing.T) {
	var webhookHits atomic.Int32
	extServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer extServer.Close()

	srv, _, storageDir := setupTestServer(t, 10*time.Minute, extServer.URL)

	reqInject := httptest.NewRequest(http.MethodPost, "/api/v1/admin/inject-demo-batch", nil)
	respInject, err := srv.App().Test(reqInject, -1)
	if err != nil || respInject.StatusCode != http.StatusOK {
		t.Fatalf("inject-demo-batch failed: status=%d err=%v", respInject.StatusCode, err)
	}

	reqDemo := httptest.NewRequest(http.MethodGet, "/api/v1/demo/sample.dem.bz2", nil)
	respDemo, err := srv.App().Test(reqDemo, -1)
	if err != nil || respDemo.StatusCode != http.StatusOK {
		t.Fatalf("demo download failed: %v", err)
	}
	bzReader := bzip2.NewReader(respDemo.Body)
	decompressed, err := io.ReadAll(bzReader)
	if err != nil || len(decompressed) == 0 {
		t.Fatalf("failed to bzip2-decompress demo stream: err=%v len=%d", err, len(decompressed))
	}

	reqLease := httptest.NewRequest(http.MethodPost, "/api/v1/batch/lease", bytes.NewBufferString(`{"worker_id":"w-test","preferred_region":"EU","max_tasks":2}`))
	reqLease.Header.Set("Content-Type", "application/json")
	respLease, err := srv.App().Test(reqLease, -1)
	if err != nil || respLease.StatusCode != http.StatusOK {
		t.Fatalf("lease request failed: status=%d err=%v", respLease.StatusCode, err)
	}
	var leaseData models.LeaseResponse
	_ = json.NewDecoder(respLease.Body).Decode(&leaseData)

	targetMatchID := leaseData.Tasks[0].MatchID
	submitPayload := models.SubmitBatchRequest{
		BatchID:       leaseData.BatchID,
		WorkerID:      "w-test",
		ClientVersion: "1.2.0",
		Results: []models.TaskResult{
			{
				MatchID:       targetMatchID,
				Status:        models.StatusSuccess,
				ParserName:    "default",
				ParserVersion: "2.0.0",
				ParserSHA256:  parsers.DefaultParserSHA256,
				Payload:       json.RawMessage(`{"custom_metric":"anything_goes","wards":[{"x":120,"y":88}]}`),
			},
		},
	}
	rawJSON, _ := json.Marshal(submitPayload)
	var gzBuf bytes.Buffer
	gzWriter := gzip.NewWriter(&gzBuf)
	_, _ = gzWriter.Write(rawJSON)
	_ = gzWriter.Close()

	reqSub := httptest.NewRequest(http.MethodPost, "/api/v1/batch/submit", &gzBuf)
	reqSub.Header.Set("Content-Type", "application/json")
	reqSub.Header.Set("Content-Encoding", "gzip")
	respSub, err := srv.App().Test(reqSub, -1)
	if err != nil || respSub.StatusCode != http.StatusOK {
		t.Fatalf("gzipped submit failed: status=%d err=%v", respSub.StatusCode, err)
	}

	expectedFile := filepath.Join(storageDir, fmt.Sprintf("%d.json", targetMatchID))
	if _, err := os.Stat(expectedFile); err != nil {
		t.Fatalf("expected match JSON file at %s, got err=%v", expectedFile, err)
	}

	reqGet := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/matches/%d", targetMatchID), nil)
	respGet, err := srv.App().Test(reqGet, -1)
	if err != nil || respGet.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/matches/%d failed: status=%d err=%v", targetMatchID, respGet.StatusCode, err)
	}

	time.Sleep(150 * time.Millisecond)
	if webhookHits.Load() != 1 {
		t.Fatalf("expected external webhook to receive 1 POST, got %d", webhookHits.Load())
	}
}

func TestRankedRegionPriorityAndMultiWorkerAccountXP(t *testing.T) {
	ctx := context.Background()
	srv, leaseMgr, _ := setupTestServer(t, 10*time.Minute, "")

	// 1. Register user account
	reqReg := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewBufferString(`{"nickname":"dota_hero_1","password":"secret123"}`))
	reqReg.Header.Set("Content-Type", "application/json")
	respReg, err := srv.App().Test(reqReg, -1)
	if err != nil || respReg.StatusCode != http.StatusCreated {
		t.Fatalf("register failed: status=%d err=%v", respReg.StatusCode, err)
	}
	var regBody struct {
		Token string `json:"token"`
		User  struct {
			Nickname string `json:"nickname"`
			XP       uint64 `json:"xp"`
		} `json:"user"`
	}
	_ = json.NewDecoder(respReg.Body).Decode(&regBody)
	if regBody.Token == "" || regBody.User.XP != 0 {
		t.Fatalf("unexpected register response: %+v", regBody)
	}

	// 2. Verify /api/v1/user/xp rejects unauthenticated request
	reqUnauth := httptest.NewRequest(http.MethodGet, "/api/v1/user/xp", nil)
	respUnauth, _ := srv.App().Test(reqUnauth, -1)
	if respUnauth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth token, got %d", respUnauth.StatusCode)
	}

	// 3. Enqueue jobs in NA (2nd best) and SEA (3rd best) while leaving EU (1st best) empty
	_ = leaseMgr.EnqueueReady(ctx, models.MatchJob{MatchID: 800001, Cluster: 121, Region: models.RegionNA})
	_ = leaseMgr.EnqueueReady(ctx, models.MatchJob{MatchID: 800002, Cluster: 151, Region: models.RegionSEA})

	// Worker A leases with region_priority ["EU", "NA", "SEA"] -> should receive NA first, then SEA!
	leaseBody := `{"worker_id":"worker-pc-1","preferred_region":"AUTO","region_priority":["EU","NA","SEA"],"max_tasks":1}`
	reqL1 := httptest.NewRequest(http.MethodPost, "/api/v1/batch/lease", bytes.NewBufferString(leaseBody))
	reqL1.Header.Set("Content-Type", "application/json")
	reqL1.Header.Set("Authorization", "Bearer "+regBody.Token)
	respL1, _ := srv.App().Test(reqL1, -1)
	var l1 models.LeaseResponse
	_ = json.NewDecoder(respL1.Body).Decode(&l1)
	if len(l1.Tasks) != 1 || l1.Tasks[0].MatchID != 800001 {
		t.Fatalf("expected ranked priority to lease NA job 800001 first, got %+v", l1.Tasks)
	}

	// Worker B (second worker machine under the same user account!) leases SEA job 800002
	leaseBody2 := `{"worker_id":"worker-pc-2","preferred_region":"AUTO","region_priority":["EU","NA","SEA"],"max_tasks":1}`
	reqL2 := httptest.NewRequest(http.MethodPost, "/api/v1/batch/lease", bytes.NewBufferString(leaseBody2))
	reqL2.Header.Set("Content-Type", "application/json")
	reqL2.Header.Set("Authorization", "Bearer "+regBody.Token)
	respL2, _ := srv.App().Test(reqL2, -1)
	var l2 models.LeaseResponse
	_ = json.NewDecoder(respL2.Body).Decode(&l2)
	if len(l2.Tasks) != 1 || l2.Tasks[0].MatchID != 800002 {
		t.Fatalf("expected second worker to lease SEA job 800002, got %+v", l2.Tasks)
	}

	// Submit from both Worker A and Worker B using the user's session token -> +50 XP each = 100 XP!
	for _, pair := range []struct {
		batchID  string
		workerID string
		matchID  uint64
	}{
		{l1.BatchID, "worker-pc-1", 800001},
		{l2.BatchID, "worker-pc-2", 800002},
	} {
		sub, _ := json.Marshal(models.SubmitBatchRequest{
			BatchID:       pair.batchID,
			WorkerID:      pair.workerID,
			ClientVersion: "1.2.0",
			Results: []models.TaskResult{{
				MatchID:      pair.matchID,
				Status:       models.StatusSuccess,
				ParserName:   "default",
				ParserSHA256: parsers.DefaultParserSHA256,
				Payload:      json.RawMessage(`{"ok":true}`),
			}},
		})
		reqS := httptest.NewRequest(http.MethodPost, "/api/v1/batch/submit", bytes.NewReader(sub))
		reqS.Header.Set("Content-Type", "application/json")
		reqS.Header.Set("Authorization", "Bearer "+regBody.Token)
		respS, _ := srv.App().Test(reqS, -1)
		if respS.StatusCode != http.StatusOK {
			t.Fatalf("submit failed for %s: status=%d", pair.workerID, respS.StatusCode)
		}
	}

	// 4. Query GET /api/v1/user/xp and verify 100 XP & 2 matches parsed
	reqXP := httptest.NewRequest(http.MethodGet, "/api/v1/user/xp", nil)
	reqXP.Header.Set("Authorization", "Bearer "+regBody.Token)
	respXP, _ := srv.App().Test(reqXP, -1)
	if respXP.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/user/xp failed: %d", respXP.StatusCode)
	}
	var profile accounts.UserProfile
	_ = json.NewDecoder(respXP.Body).Decode(&profile)
	if profile.XP != 100 || profile.MatchesParsed != 2 || profile.Nickname != "dota_hero_1" {
		t.Fatalf("expected 100 XP and 2 matches for dota_hero_1, got %+v", profile)
	}
}

func registerTestUser(t *testing.T, srv *Server, body string) (int, string, accounts.UserProfile) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.App().Test(req, -1)
	if err != nil {
		t.Fatalf("register request failed: %v", err)
	}
	var out struct {
		Token string               `json:"token"`
		User  accounts.UserProfile `json:"user"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out.Token, out.User
}

func TestFactionLockedRegistrationAndStandings(t *testing.T) {
	ctx := context.Background()
	srv, leaseMgr, _ := setupTestServer(t, 10*time.Minute, "")

	if code, _, _ := registerTestUser(t, srv, `{"nickname":"bad_side","password":"secret123","faction":"neutral"}`); code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid faction, got %d", code)
	}

	code, radToken, radUser := registerTestUser(t, srv, `{"nickname":"rad_one","password":"secret123","faction":"Radiant"}`)
	if code != http.StatusCreated || radUser.Faction != accounts.FactionRadiant {
		t.Fatalf("expected radiant registration, got status=%d user=%+v", code, radUser)
	}
	code, _, direUser := registerTestUser(t, srv, `{"nickname":"dire_one","password":"secret123","faction":"dire"}`)
	if code != http.StatusCreated || direUser.Faction != accounts.FactionDire {
		t.Fatalf("expected dire registration, got status=%d user=%+v", code, direUser)
	}
	code, neutralToken, neutralUser := registerTestUser(t, srv, `{"nickname":"no_side","password":"secret123"}`)
	if code != http.StatusCreated || neutralUser.Faction != "" {
		t.Fatalf("expected neutral registration, got status=%d user=%+v", code, neutralUser)
	}

	// Radiant user parses one accepted match end-to-end; neutral user parses one too (must not count).
	for i, tok := range []string{radToken, neutralToken} {
		matchID := uint64(810001 + i)
		workerID := fmt.Sprintf("faction-worker-%d", i)
		_ = leaseMgr.EnqueueReady(ctx, models.MatchJob{MatchID: matchID, Cluster: 133, Region: models.RegionEU})
		leaseReq := httptest.NewRequest(http.MethodPost, "/api/v1/batch/lease",
			bytes.NewBufferString(fmt.Sprintf(`{"worker_id":"%s","preferred_region":"EU","max_tasks":1}`, workerID)))
		leaseReq.Header.Set("Content-Type", "application/json")
		leaseReq.Header.Set("Authorization", "Bearer "+tok)
		leaseResp, _ := srv.App().Test(leaseReq, -1)
		var lease models.LeaseResponse
		_ = json.NewDecoder(leaseResp.Body).Decode(&lease)
		if len(lease.Tasks) != 1 {
			t.Fatalf("expected 1 leased task for %s, got %+v", workerID, lease.Tasks)
		}
		sub, _ := json.Marshal(models.SubmitBatchRequest{
			BatchID:  lease.BatchID,
			WorkerID: workerID,
			Results: []models.TaskResult{{
				MatchID:      lease.Tasks[0].MatchID,
				Status:       models.StatusSuccess,
				ParserName:   "default",
				ParserSHA256: parsers.DefaultParserSHA256,
				Payload:      json.RawMessage(`{"ok":true}`),
			}},
		})
		subReq := httptest.NewRequest(http.MethodPost, "/api/v1/batch/submit", bytes.NewReader(sub))
		subReq.Header.Set("Content-Type", "application/json")
		subReq.Header.Set("Authorization", "Bearer "+tok)
		if subResp, _ := srv.App().Test(subReq, -1); subResp.StatusCode != http.StatusOK {
			t.Fatalf("submit failed for %s: %d", workerID, subResp.StatusCode)
		}
	}

	statsResp, _ := srv.App().Test(httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil), -1)
	var stats models.PublicStats
	_ = json.NewDecoder(statsResp.Body).Decode(&stats)
	if stats.Factions == nil {
		t.Fatalf("expected faction standings on /api/v1/stats")
	}
	if stats.Factions.Radiant.XP != accounts.XPPerMatch || stats.Factions.Radiant.Members != 1 {
		t.Fatalf("expected radiant 50 XP / 1 member, got %+v", stats.Factions.Radiant)
	}
	if stats.Factions.Dire.XP != 0 || stats.Factions.Dire.Members != 1 {
		t.Fatalf("expected dire 0 XP / 1 member (neutral XP excluded), got %+v", stats.Factions.Dire)
	}

	// Faction persists across logins and is returned on the user's own profile.
	loginReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login",
		bytes.NewBufferString(`{"nickname":"rad_one","password":"secret123","faction":"dire"}`))
	loginReq.Header.Set("Content-Type", "application/json")
	loginResp, _ := srv.App().Test(loginReq, -1)
	var loginBody struct {
		User accounts.UserProfile `json:"user"`
	}
	_ = json.NewDecoder(loginResp.Body).Decode(&loginBody)
	if loginBody.User.Faction != accounts.FactionRadiant {
		t.Fatalf("faction must be immutable across logins, got %q", loginBody.User.Faction)
	}
}

func TestLegacyAccountsDBMigratesToNeutralFaction(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	_, err = legacy.Exec(`
CREATE TABLE users (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	nickname TEXT NOT NULL UNIQUE COLLATE NOCASE,
	pass_hash TEXT NOT NULL,
	salt TEXT NOT NULL,
	xp INTEGER NOT NULL DEFAULT 0,
	matches_parsed INTEGER NOT NULL DEFAULT 0,
	other_data TEXT NOT NULL DEFAULT '{}',
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL
);
INSERT INTO users (nickname, pass_hash, salt, xp, matches_parsed, created_at, updated_at)
VALUES ('old_timer', 'x', 'y', 500, 10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`)
	_ = legacy.Close()
	if err != nil {
		t.Fatalf("seed legacy db: %v", err)
	}

	store, err := accounts.NewStore(dbPath)
	if err != nil {
		t.Fatalf("migrate legacy db: %v", err)
	}
	defer store.Close()

	user, err := store.GetUserByID(context.Background(), 1)
	if err != nil || user.Faction != "" || user.XP != 500 {
		t.Fatalf("expected migrated neutral user with 500 XP, got %+v err=%v", user, err)
	}
	standings, err := store.GetFactionStandings(context.Background())
	if err != nil || standings.Radiant.XP != 0 || standings.Dire.XP != 0 {
		t.Fatalf("neutral legacy XP must not count toward factions, got %+v err=%v", standings, err)
	}
}

func TestServerManagedParsersAndStaleParseDismissal(t *testing.T) {
	ctx := context.Background()
	srv, leaseMgr, storageDir := setupTestServer(t, 10*time.Minute, "")

	// Register an authenticated contributor to verify 0 XP on dismissed stale parses
	reqReg := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewBufferString(`{"nickname":"parser_tester","password":"secret123"}`))
	reqReg.Header.Set("Content-Type", "application/json")
	respReg, _ := srv.App().Test(reqReg, -1)
	var regBody struct {
		Token string `json:"token"`
	}
	_ = json.NewDecoder(respReg.Body).Decode(&regBody)

	// Upload and activate a custom JS parser via JSON upload API
	uploadBody, _ := json.Marshal(map[string]any{
		"name":     "ward-filter",
		"filename": "ward_filter.js",
		"content":  `JSON.stringify({mode: "ward-filter"})`,
		"activate": true,
	})
	reqUp := httptest.NewRequest(http.MethodPost, "/api/v1/admin/parsers/upload", bytes.NewReader(uploadBody))
	reqUp.Header.Set("Content-Type", "application/json")
	respUp, err := srv.App().Test(reqUp, -1)
	if err != nil || respUp.StatusCode != http.StatusCreated {
		t.Fatalf("upload custom parser failed: status=%d err=%v", respUp.StatusCode, err)
	}
	var upDesc models.ParserDescriptor
	_ = json.NewDecoder(respUp.Body).Decode(&upDesc)
	if upDesc.ReportedName != "custom(ward-filter)" || upDesc.SHA256 == "" {
		t.Fatalf("unexpected uploaded parser descriptor: %+v", upDesc)
	}

	// Lease a job while custom(ward-filter) is active
	_ = leaseMgr.EnqueueReady(ctx, models.MatchJob{MatchID: 880001, Cluster: 133, Region: models.RegionEU})
	reqL := httptest.NewRequest(http.MethodPost, "/api/v1/batch/lease", bytes.NewBufferString(`{"worker_id":"w-sync","preferred_region":"EU"}`))
	reqL.Header.Set("Content-Type", "application/json")
	respL, _ := srv.App().Test(reqL, -1)
	var leaseResp models.LeaseResponse
	_ = json.NewDecoder(respL.Body).Decode(&leaseResp)
	if leaseResp.ActiveParser == nil || leaseResp.ActiveParser.ReportedName != "custom(ward-filter)" {
		t.Fatalf("expected active_parser custom(ward-filter) in lease response, got %+v", leaseResp.ActiveParser)
	}

	// Worker submits using stale "default" parser -> MUST be dismissed, 0 XP, NOT stored, and re-queued!
	subStale, _ := json.Marshal(models.SubmitBatchRequest{
		BatchID:       leaseResp.BatchID,
		WorkerID:      "w-sync",
		ClientVersion: "1.2.0",
		Results: []models.TaskResult{{
			MatchID:      880001,
			Status:       models.StatusSuccess,
			ParserName:   "default",
			ParserSHA256: parsers.DefaultParserSHA256,
			Payload:      json.RawMessage(`{"stale":true}`),
		}},
	})
	reqS1 := httptest.NewRequest(http.MethodPost, "/api/v1/batch/submit", bytes.NewReader(subStale))
	reqS1.Header.Set("Content-Type", "application/json")
	reqS1.Header.Set("Authorization", "Bearer "+regBody.Token)
	respS1, _ := srv.App().Test(reqS1, -1)
	var subResp1 models.SubmitBatchResponse
	_ = json.NewDecoder(respS1.Body).Decode(&subResp1)
	if subResp1.MatchesDismissed != 1 || subResp1.MatchesAccepted != 0 || subResp1.XPAwarded != 0 {
		t.Fatalf("expected stale parse dismissed with 0 XP, got %+v", subResp1)
	}
	if _, err := os.Stat(filepath.Join(storageDir, "880001.json")); err == nil {
		t.Fatalf("dismissed match 880001 must NOT be saved to disk")
	}

	// Because 880001 was re-queued, worker can immediately lease it again and submit with custom(ward-filter) + exact SHA256!
	reqL2 := httptest.NewRequest(http.MethodPost, "/api/v1/batch/lease", bytes.NewBufferString(`{"worker_id":"w-sync","preferred_region":"EU"}`))
	reqL2.Header.Set("Content-Type", "application/json")
	respL2, _ := srv.App().Test(reqL2, -1)
	var leaseResp2 models.LeaseResponse
	_ = json.NewDecoder(respL2.Body).Decode(&leaseResp2)
	if len(leaseResp2.Tasks) != 1 || leaseResp2.Tasks[0].MatchID != 880001 {
		t.Fatalf("expected dismissed match 880001 to be re-queued, got %+v", leaseResp2.Tasks)
	}

	subValid, _ := json.Marshal(models.SubmitBatchRequest{
		BatchID:       leaseResp2.BatchID,
		WorkerID:      "w-sync",
		ClientVersion: "1.2.0",
		Results: []models.TaskResult{{
			MatchID:      880001,
			Status:       models.StatusSuccess,
			ParserName:   "custom(ward-filter)",
			ParserSHA256: upDesc.SHA256,
			Payload:      json.RawMessage(`{"mode":"ward-filter"}`),
		}},
	})
	reqS2 := httptest.NewRequest(http.MethodPost, "/api/v1/batch/submit", bytes.NewReader(subValid))
	reqS2.Header.Set("Content-Type", "application/json")
	reqS2.Header.Set("Authorization", "Bearer "+regBody.Token)
	respS2, _ := srv.App().Test(reqS2, -1)
	var subResp2 models.SubmitBatchResponse
	_ = json.NewDecoder(respS2.Body).Decode(&subResp2)
	if subResp2.MatchesAccepted != 1 || subResp2.MatchesDismissed != 0 || subResp2.XPAwarded != 50 {
		t.Fatalf("expected matching custom parser submission accepted for 50 XP, got %+v", subResp2)
	}
}

func TestUploadSampleParsersAllLanguages(t *testing.T) {
	srv, _, _ := setupTestServer(t, 10*time.Minute, "")

	// Provide a stub valid WASM runtime so .py bundling works offline in unit tests.
	stubPyWasm := filepath.Join(t.TempDir(), "stub_python.wasm")
	if err := os.WriteFile(stubPyWasm, []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PYTHON_WASM_PATH", stubPyWasm)

	samples := []struct {
		name     string
		filename string
		kind     string
		fallback string
	}{
		{name: "sample-go", filename: "simple_go_parser.go", kind: "go", fallback: "package main\nimport (\"fmt\"; \"io\"; \"os\")\nfunc main() { b, _ := io.ReadAll(os.Stdin); fmt.Printf(\"{\\\"dem_bytes\\\":%d}\", len(b)) }\n"},
		{name: "sample-lua", filename: "simple_lua_parser.lua", kind: "lua", fallback: "print(string.format('{\"dem_bytes\":%d}', stdin_len()))\n"},
		{name: "sample-py", filename: "simple_py_parser.py", kind: "py", fallback: "import json, sys\nprint(json.dumps({\"dem_bytes\": len(sys.stdin.buffer.read())}))\n"},
		{name: "sample-js", filename: "simple_js_parser.js", kind: "js", fallback: "console.log(JSON.stringify({dem_bytes: stdin_len()}));\n"},
	}

	sampleDir := filepath.Join("..", "..", "..", "sample_parsers")
	for _, sc := range samples {
		raw, err := os.ReadFile(filepath.Join(sampleDir, sc.filename))
		if err != nil {
			raw = []byte(sc.fallback)
		}

		var body bytes.Buffer
		mw := multipart.NewWriter(&body)
		_ = mw.WriteField("name", sc.name)
		_ = mw.WriteField("activate", "true")
		fw, err := mw.CreateFormFile("file", sc.filename)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fw.Write(raw)
		_ = mw.Close()

		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/parsers/upload", &body)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		resp, err := srv.App().Test(req, -1)
		if err != nil {
			t.Fatalf("upload %s request error: %v", sc.filename, err)
		}
		if resp.StatusCode != http.StatusCreated {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("upload %s returned %d: %s", sc.filename, resp.StatusCode, string(b))
		}

		var desc models.ParserDescriptor
		if err := json.NewDecoder(resp.Body).Decode(&desc); err != nil {
			t.Fatalf("decode upload response for %s: %v", sc.filename, err)
		}
		if desc.Kind != sc.kind || desc.SHA256 == "" || desc.ArtifactSize == 0 {
			t.Fatalf("unexpected uploaded parser descriptor for %s: %+v", sc.filename, desc)
		}

		// Verify artifact is downloadable via /api/v1/parser/artifact/:sha256
		reqArt := httptest.NewRequest(http.MethodGet, desc.DownloadURL, nil)
		respArt, err := srv.App().Test(reqArt, -1)
		if err != nil || respArt.StatusCode != http.StatusOK {
			t.Fatalf("artifact download for %s failed: status=%d err=%v", sc.filename, respArt.StatusCode, err)
		}
	}
}

func TestAdminConfigPersistence(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run failed: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	leaseMgr := queue.NewLeaseManager(rdb, 5*time.Minute)
	filterEngine := harvester.NewFilterEngine(models.DefaultFilterConfig())
	if filterEngine.GetConfig().Enabled {
		t.Fatalf("expected harvester Enabled=false by default at startup")
	}

	harv := harvester.NewHarvester("", filterEngine, leaseMgr, nil)
	tmpDir := t.TempDir()
	store := storage.NewAnalyticsStore(filepath.Join(tmpDir, "matches"), "")
	accStore, err := accounts.NewStore(filepath.Join(tmpDir, "accounts.db"))
	if err != nil {
		t.Fatalf("accounts.NewStore failed: %v", err)
	}
	defer accStore.Close()

	srv := NewServer(leaseMgr, filterEngine, harv, store, accStore, nil, "")
	srv.SetAPIKeysPath(filepath.Join(tmpDir, "api_keys.json"))

	// 1. Update filters with Enabled=true -> filter rules should persist, but Enabled must be saved as false.
	filterBody := `{"enabled":true,"poll_interval_ms":2000,"max_queue_size":10000,"cdn_delay_seconds":180,"allowed_lobby_types":[7,0],"allowed_game_modes":[22],"min_duration_seconds":1200,"max_duration_seconds":7200,"require_10_humans":true,"allowed_regions":["EU","NA"]}`
	reqF := httptest.NewRequest(http.MethodPut, "/api/v1/admin/filters", bytes.NewBufferString(filterBody))
	reqF.Header.Set("Content-Type", "application/json")
	respF, err := srv.App().Test(reqF, -1)
	if err != nil || respF.StatusCode != http.StatusOK {
		t.Fatalf("PUT /api/v1/admin/filters failed: status=%d err=%v", respF.StatusCode, err)
	}

	loadedFilter, ok := LoadPersistedFilterConfig(filepath.Join(tmpDir, "filter_config.json"))
	if !ok {
		t.Fatalf("expected filter_config.json to be persisted")
	}
	if loadedFilter.Enabled {
		t.Fatalf("expected persisted filter_config.json to keep Enabled=false")
	}
	if loadedFilter.MinDurationSeconds != 1200 || len(loadedFilter.AllowedLobbyTypes) != 2 {
		t.Fatalf("unexpected persisted filter config: %+v", loadedFilter)
	}

	// 2. Update sink and file storage -> both should persist to sink_config.json.
	reqSink := httptest.NewRequest(http.MethodPut, "/api/v1/admin/sink", bytes.NewBufferString(`{"webhook_url":"https://example.com/hook","webhook_enabled":true}`))
	reqSink.Header.Set("Content-Type", "application/json")
	respSink, err := srv.App().Test(reqSink, -1)
	if err != nil || respSink.StatusCode != http.StatusOK {
		t.Fatalf("PUT /api/v1/admin/sink failed: status=%d err=%v", respSink.StatusCode, err)
	}

	reqFS := httptest.NewRequest(http.MethodPut, "/api/v1/admin/file-storage", bytes.NewBufferString(`{"enabled":false}`))
	reqFS.Header.Set("Content-Type", "application/json")
	respFS, err := srv.App().Test(reqFS, -1)
	if err != nil || respFS.StatusCode != http.StatusOK {
		t.Fatalf("PUT /api/v1/admin/file-storage failed: status=%d err=%v", respFS.StatusCode, err)
	}

	whURL, whEnabled, fsEnabled, ok := LoadPersistedSinkConfig(filepath.Join(tmpDir, "sink_config.json"))
	if !ok {
		t.Fatalf("expected sink_config.json to be persisted")
	}
	if whURL != "https://example.com/hook" || !whEnabled || fsEnabled {
		t.Fatalf("unexpected persisted sink config: url=%q wh=%v fs=%v", whURL, whEnabled, fsEnabled)
	}
}

func TestRawReplayPassthroughMode(t *testing.T) {
	srv, leaseMgr, storageDir := setupTestServer(t, 5*time.Minute, "")

	// Activate "raw" mode via Admin API.
	reqAct := httptest.NewRequest(http.MethodPut, "/api/v1/admin/parsers/active", bytes.NewBufferString(`{"mode":"raw","id":"raw"}`))
	reqAct.Header.Set("Content-Type", "application/json")
	respAct, err := srv.App().Test(reqAct, -1)
	if err != nil || respAct.StatusCode != http.StatusOK {
		t.Fatalf("PUT /api/v1/admin/parsers/active failed: status=%d err=%v", respAct.StatusCode, err)
	}
	var activeDesc models.ParserDescriptor
	if err := json.NewDecoder(respAct.Body).Decode(&activeDesc); err != nil {
		t.Fatalf("decode active parser: %v", err)
	}
	if activeDesc.Mode != "raw" || activeDesc.ReportedName != "raw" || activeDesc.SHA256 != parsers.RawParserSHA256 {
		t.Fatalf("unexpected raw active descriptor: %+v", activeDesc)
	}

	// Enqueue and lease a match; verify ActiveParser on lease is "raw".
	if err := leaseMgr.EnqueueReady(context.Background(), models.MatchJob{
		MatchID: 881001, Cluster: 133, ReplaySalt: 11111, Region: models.RegionEU,
		URL: "http://example.com/881001.dem.bz2", ReadyAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("EnqueueReady failed: %v", err)
	}
	reqLease := httptest.NewRequest(http.MethodPost, "/api/v1/batch/lease", bytes.NewBufferString(`{"worker_id":"w-raw","preferred_region":"EU","max_tasks":1}`))
	reqLease.Header.Set("Content-Type", "application/json")
	respLease, err := srv.App().Test(reqLease, -1)
	if err != nil || respLease.StatusCode != http.StatusOK {
		t.Fatalf("lease failed: status=%d err=%v", respLease.StatusCode, err)
	}
	var leaseResp models.LeaseResponse
	_ = json.NewDecoder(respLease.Body).Decode(&leaseResp)
	if leaseResp.ActiveParser == nil || leaseResp.ActiveParser.Mode != "raw" {
		t.Fatalf("expected lease ActiveParser mode=raw, got %+v", leaseResp.ActiveParser)
	}

	// Submit raw .dem.bz2 bytes from worker.
	rawSample := []byte("BZh91AY&SYraw-test-replay-bytes")
	rawPayload, _ := json.Marshal(map[string]any{
		"mode":                  "raw",
		"match_id":              881001,
		"format":                "dem.bz2",
		"compressed_size_bytes": len(rawSample),
		"data_base64":           base64.StdEncoding.EncodeToString(rawSample),
	})
	subBody, _ := json.Marshal(models.SubmitBatchRequest{
		BatchID:       leaseResp.BatchID,
		WorkerID:      "w-raw",
		ClientVersion: "1.2.0",
		Results: []models.TaskResult{{
			MatchID:       881001,
			Status:        models.StatusSuccess,
			ParserName:    "raw",
			ParserVersion: "1.0.0",
			ParserSHA256:  parsers.RawParserSHA256,
			Payload:       rawPayload,
		}},
	})
	reqSub := httptest.NewRequest(http.MethodPost, "/api/v1/batch/submit", bytes.NewReader(subBody))
	reqSub.Header.Set("Content-Type", "application/json")
	respSub, err := srv.App().Test(reqSub, -1)
	if err != nil || respSub.StatusCode != http.StatusOK {
		t.Fatalf("submit failed: status=%d err=%v", respSub.StatusCode, err)
	}
	var subAck models.SubmitBatchResponse
	_ = json.NewDecoder(respSub.Body).Decode(&subAck)
	if subAck.MatchesAccepted != 1 {
		t.Fatalf("expected MatchesAccepted=1, got %+v", subAck)
	}

	// Verify .dem.bz2 file on disk AND downloadable endpoint.
	diskRaw, err := os.ReadFile(filepath.Join(storageDir, "881001.dem.bz2"))
	if err != nil || !bytes.Equal(diskRaw, rawSample) {
		t.Fatalf("expected 881001.dem.bz2 on disk to match rawSample, got len=%d err=%v", len(diskRaw), err)
	}
	respDL, err := srv.App().Test(httptest.NewRequest(http.MethodGet, "/api/v1/matches/881001.dem.bz2", nil), -1)
	if err != nil || respDL.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/matches/881001.dem.bz2 failed: status=%d err=%v", respDL.StatusCode, err)
	}
	dlBytes, _ := io.ReadAll(respDL.Body)
	if !bytes.Equal(dlBytes, rawSample) {
		t.Fatalf("downloaded .dem.bz2 bytes mismatch: got %q want %q", dlBytes, rawSample)
	}

	// Verify /api/v1/admin/overview recent_results omits bloated data_base64.
	respOv, err := srv.App().Test(httptest.NewRequest(http.MethodGet, "/api/v1/admin/overview", nil), -1)
	if err != nil || respOv.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/admin/overview failed: status=%d err=%v", respOv.StatusCode, err)
	}
	ovBytes, _ := io.ReadAll(respOv.Body)
	if bytes.Contains(ovBytes, []byte("data_base64")) {
		t.Fatalf("expected admin overview recent_results to omit raw data_base64")
	}
}

func TestAdminPasswordAuthAndMaskedKeys(t *testing.T) {
	srv, _, storageDir := setupTestServer(t, 5*time.Minute, "")
	dataDir := filepath.Dir(storageDir)
	authPath := filepath.Join(dataDir, "admin_auth.json")
	srv.EnableAdminPasswordAuth(authPath)

	// Configure a Steam API key so we can test key masking too.
	srv.harvester.SetAPIKeys("STEAM-SECRET-9876", "STRATZ-TOKEN-4321", "")

	// 1. Before password is set, admin endpoints must return 401 and login must return 403.
	respUnauth, err := srv.App().Test(httptest.NewRequest(http.MethodGet, "/api/v1/admin/overview", nil), -1)
	if err != nil || respUnauth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 on /api/v1/admin/overview when unauthenticated, got status=%d err=%v", respUnauth.StatusCode, err)
	}
	reqNoPass := httptest.NewRequest(http.MethodPost, "/api/v1/admin/auth/login", bytes.NewBufferString(`{"password":"anything"}`))
	reqNoPass.Header.Set("Content-Type", "application/json")
	respNoPass, err := srv.App().Test(reqNoPass, -1)
	if err != nil || respNoPass.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 when trying to log in before CLI password is set, got status=%d err=%v", respNoPass.StatusCode, err)
	}

	// 2. Set admin password (CLI-only path) -> saved hashed as bcrypt (never plaintext).
	if err := srv.SetAdminPassword("strongpass123", true); err != nil {
		t.Fatalf("SetAdminPassword failed: %v", err)
	}
	authDisk, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatalf("expected admin_auth.json on disk: %v", err)
	}
	if bytes.Contains(authDisk, []byte("strongpass123")) || !bytes.Contains(authDisk, []byte("$2")) {
		t.Fatalf("expected bcrypt hash on disk without plaintext password, got %s", authDisk)
	}

	// 3. Wrong password returns 401 and triggers 429 lockout on the 5th failure.
	for i := 1; i <= 5; i++ {
		reqBad := httptest.NewRequest(http.MethodPost, "/api/v1/admin/auth/login", bytes.NewBufferString(`{"password":"wrong"}`))
		reqBad.Header.Set("Content-Type", "application/json")
		respBad, err := srv.App().Test(reqBad, -1)
		if err != nil {
			t.Fatalf("bad login request #%d error: %v", i, err)
		}
		wantCode := http.StatusUnauthorized
		if i == 5 {
			wantCode = http.StatusTooManyRequests
		}
		if respBad.StatusCode != wantCode {
			t.Fatalf("attempt %d: expected %d, got %d", i, wantCode, respBad.StatusCode)
		}
	}

	// Simulate lockout expiry on 127.0.0.1 for the test so we can verify valid login next.
	srv.adminAuthMu.Lock()
	srv.adminLoginAttempts = make(map[string]*loginAttemptWindow)
	srv.adminAuthMu.Unlock()

	// 4. Correct password returns 200 with HttpOnly session cookie.
	reqGood := httptest.NewRequest(http.MethodPost, "/api/v1/admin/auth/login", bytes.NewBufferString(`{"password":"strongpass123"}`))
	reqGood.Header.Set("Content-Type", "application/json")
	respGood, err := srv.App().Test(reqGood, -1)
	if err != nil || respGood.StatusCode != http.StatusOK {
		t.Fatalf("valid login failed: status=%d err=%v", respGood.StatusCode, err)
	}
	var sessionCookie *http.Cookie
	for _, ck := range respGood.Cookies() {
		if ck.Name == adminAuthCookieName {
			sessionCookie = ck
		}
	}
	if sessionCookie == nil || !sessionCookie.HttpOnly {
		t.Fatalf("expected HttpOnly %s cookie on login, got %+v", adminAuthCookieName, respGood.Cookies())
	}

	// 5. Authenticated /api/v1/admin/api-keys and /overview must return masked keys and NO plaintext secrets.
	reqKeys := httptest.NewRequest(http.MethodGet, "/api/v1/admin/api-keys", nil)
	reqKeys.AddCookie(sessionCookie)
	respKeys, err := srv.App().Test(reqKeys, -1)
	if err != nil || respKeys.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/admin/api-keys failed: status=%d err=%v", respKeys.StatusCode, err)
	}
	keysRaw, _ := io.ReadAll(respKeys.Body)
	if bytes.Contains(keysRaw, []byte("STEAM-SECRET-9876")) || bytes.Contains(keysRaw, []byte("STRATZ-TOKEN-4321")) {
		t.Fatalf("plaintext API key leaked in /api/v1/admin/api-keys response: %s", keysRaw)
	}
	var keysResp map[string]any
	_ = json.Unmarshal(keysRaw, &keysResp)
	if keysResp["steam_masked"] != "••••••••9876" || keysResp["stratz_masked"] != "••••••••4321" {
		t.Fatalf("expected masked keys in response, got %+v", keysResp)
	}

	// Updating only OpenDota key while leaving Steam blank preserves Steam key.
	reqUpd := httptest.NewRequest(http.MethodPut, "/api/v1/admin/api-keys", bytes.NewBufferString(`{"opendota_api_key":"OD-NEW-5555"}`))
	reqUpd.Header.Set("Content-Type", "application/json")
	reqUpd.AddCookie(sessionCookie)
	respUpd, err := srv.App().Test(reqUpd, -1)
	if err != nil || respUpd.StatusCode != http.StatusOK {
		t.Fatalf("PUT /api/v1/admin/api-keys failed: status=%d err=%v", respUpd.StatusCode, err)
	}
	gotSteam, gotStratz, gotOD := srv.harvester.GetAPIKeys()
	if gotSteam != "STEAM-SECRET-9876" || gotStratz != "STRATZ-TOKEN-4321" || gotOD != "OD-NEW-5555" {
		t.Fatalf("expected existing keys preserved on partial update, got steam=%q stratz=%q od=%q", gotSteam, gotStratz, gotOD)
	}
}


