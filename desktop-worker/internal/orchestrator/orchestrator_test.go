package orchestrator

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dota2-analytics/desktop-worker/internal/client"
	"github.com/dota2-analytics/desktop-worker/internal/config"
	"github.com/dota2-analytics/desktop-worker/internal/downloader"
	"github.com/dota2-analytics/desktop-worker/internal/netprobe"
	"github.com/dota2-analytics/desktop-worker/internal/parser"
	"github.com/dota2-analytics/desktop-worker/internal/sentinel"
)

const validTestBz2Base64 = "QlpoOTFBWSZTWVjtQx8AACqbgFAEfxAAAK5t3qogAFDGAAAAAGqeUGR6mmQGnqZpDu0A69qbZXc7EWt+8ZROWMQOH0FJkpjVwqMVcpBiRmRJm/uEKoGNUJzk5G4R+H1rfi7kinChILHahj4="

type customBlackBoxStub struct{}

func (c *customBlackBoxStub) Name() string    { return "future-blackbox-v2" }
func (c *customBlackBoxStub) Version() string { return "2.0.0" }

func (c *customBlackBoxStub) Parse(ctx context.Context, ctrl parser.ExecutionController, input parser.ParseInput) (*parser.ParseOutput, error) {
	stream := input.NewDecompressedStream()
	decompressedBytes, err := io.ReadAll(stream)
	if err != nil {
		return nil, err
	}
	if ctrl != nil {
		ctrl.ReportProgress(100.0)
	}
	customOut, _ := json.Marshal(map[string]any{
		"match_id":           input.MatchID,
		"decompressed_bytes": len(decompressedBytes),
		"arbitrary_metric":   "custom_blackbox_data_from_ram",
		"zero_disk_writes":   true,
	})
	return &parser.ParseOutput{
		ParserName:    c.Name(),
		ParserVersion: c.Version(),
		Payload:       customOut,
	}, nil
}

func TestSentinelCrossPlatformProbes(t *testing.T) {
	tmpCfg := filepath.Join(t.TempDir(), "config.json")
	cfgMgr := config.NewManager(tmpCfg)

	sent := sentinel.NewSentinel(cfgMgr)
	snap := sent.ProbeNow()

	if snap.CheckedAt.IsZero() {
		t.Fatalf("expected non-zero CheckedAt timestamp from Sentinel")
	}
	if snap.UserIdleSeconds < 0 {
		t.Fatalf("expected non-negative UserIdleSeconds from OS idle probe, got %d", snap.UserIdleSeconds)
	}
}

func TestInRAMDownloadAndPluggableBlackBoxParserEndToEnd(t *testing.T) {
	var submittedCount atomic.Int32
	var lastSubmittedPayload string

	bz2Bytes, err := base64.StdEncoding.DecodeString(validTestBz2Base64)
	if err != nil {
		t.Fatalf("failed to decode test bz2 fixture: %v", err)
	}

	var mockServer *httptest.Server
	mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stats":
			_ = json.NewEncoder(w).Encode(client.PublicStats{
				GlobalParsedTotal: 100,
				GlobalParsedToday: 10,
			})
		case "/api/v1/demo.dem.bz2":
			w.Header().Set("Content-Type", "application/x-bzip2")
			_, _ = w.Write(bz2Bytes)
		case "/api/v1/batch/submit":
			submittedCount.Add(1)
			raw, _ := io.ReadAll(r.Body)
			lastSubmittedPayload = string(raw)
			_ = json.NewEncoder(w).Encode(client.SubmitBatchResponse{
				Status:          "ACK",
				MatchesAccepted: 1,
				LeaseCompleted:  true,
			})
		}
	}))
	defer mockServer.Close()

	tmpCfg := filepath.Join(t.TempDir(), "config.json")
	cfgMgr := config.NewManager(tmpCfg)
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = mockServer.URL
		c.PauseOnGaming = false
	})

	sent := sentinel.NewSentinel(cfgMgr)
	apiClient := client.NewAPIClient()
	dl := downloader.NewDownloader()
	orch := NewOrchestrator(cfgMgr, sent, apiClient, dl)

	// 1. Test with default BoilerplateMantaParser in RAM
	ctx := context.Background()
	orch.processSingleTask(ctx, "batch-1", client.LeasedTask{
		MatchID:         7891112221,
		Cluster:         133,
		Region:          "EU",
		URL:             mockServer.URL + "/api/v1/demo.dem.bz2",
		DurationHintSec: 2100,
	})

	if submittedCount.Load() != 1 {
		t.Fatalf("expected 1 submitted batch with default parser, got %d", submittedCount.Load())
	}

	// 2. Attach a custom Black-Box Parser and verify it processes in RAM and submits arbitrary JSON
	orch.SetCustomBlackBoxParser(&customBlackBoxStub{})
	orch.processSingleTask(ctx, "batch-2", client.LeasedTask{
		MatchID:         7891112222,
		Cluster:         133,
		Region:          "EU",
		URL:             mockServer.URL + "/api/v1/demo.dem.bz2",
		DurationHintSec: 1850,
	})

	if submittedCount.Load() != 2 {
		t.Fatalf("expected 2 submitted batches after custom black-box parser, got %d", submittedCount.Load())
	}
	if len(lastSubmittedPayload) == 0 {
		t.Fatalf("expected non-empty gzipped submission payload")
	}

	// Verify RAM buffer was released back to 0 after completion
	snap := orch.GetStatusSnapshot()
	if snap.RAMBufferBytes != 0 {
		t.Fatalf("expected RAMBufferBytes to be 0 after task completion, got %d", snap.RAMBufferBytes)
	}
	if snap.YourParsedToday != 2 {
		t.Fatalf("expected YourParsedToday == 2, got %d", snap.YourParsedToday)
	}
	if snap.ContributorXP != 100 {
		t.Fatalf("expected flat 50 XP per match (100 XP total), got %d", snap.ContributorXP)
	}
}

func TestAutoRegionProbeAndRankedLeaseRequest(t *testing.T) {
	var leaseBody struct {
		PreferredRegion string   `json:"preferred_region"`
		RegionPriority  []string `json:"region_priority"`
	}
	var leaseCalled atomic.Bool

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stats":
			_ = json.NewEncoder(w).Encode(client.PublicStats{})
		case "/api/v1/batch/lease":
			_ = json.NewDecoder(r.Body).Decode(&leaseBody)
			leaseCalled.Store(true)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer mockServer.Close()

	tmpCfg := filepath.Join(t.TempDir(), "config.json")
	cfgMgr := config.NewManager(tmpCfg)
	if cfgMgr.Get().PreferredRegion != "AUTO" {
		t.Fatalf("expected default PreferredRegion AUTO, got %q", cfgMgr.Get().PreferredRegion)
	}
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = mockServer.URL
		c.PauseOnGaming = false
	})

	sent := sentinel.NewSentinel(cfgMgr)
	orch := NewOrchestrator(cfgMgr, sent, client.NewAPIClient(), downloader.NewDownloader())
	orch.SetRegionProber(func(ctx context.Context) ([]netprobe.RegionLatency, []string) {
		lat := []netprobe.RegionLatency{
			{Region: "EU", POP: "fra", Host: "155.133.226.68", LatencyMs: 28, Reachable: true},
			{Region: "NA", POP: "iad", Host: "162.254.192.88", LatencyMs: 112, Reachable: true},
			{Region: "CN", POP: "hkg", Host: "103.28.54.163", LatencyMs: 170, Reachable: true},
		}
		return lat, []string{"EU", "NA", "CN"}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	orch.Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(cfgMgr.Get().RegionPriority) > 0 && leaseCalled.Load() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if !reflect.DeepEqual(cfgMgr.Get().RegionPriority, []string{"EU", "NA", "CN"}) {
		t.Fatalf("expected persisted RegionPriority [EU NA CN], got %v", cfgMgr.Get().RegionPriority)
	}
	if !leaseCalled.Load() {
		t.Fatalf("expected workerLoop to call /api/v1/batch/lease")
	}
	if leaseBody.PreferredRegion != "AUTO" {
		t.Fatalf("expected lease PreferredRegion AUTO, got %q", leaseBody.PreferredRegion)
	}
	if !reflect.DeepEqual(leaseBody.RegionPriority, []string{"EU", "NA", "CN"}) {
		t.Fatalf("expected lease RegionPriority [EU NA CN], got %v", leaseBody.RegionPriority)
	}

	reloaded := config.NewManager(tmpCfg).Get()
	if !reflect.DeepEqual(reloaded.RegionPriority, []string{"EU", "NA", "CN"}) {
		t.Fatalf("expected reloaded RegionPriority [EU NA CN], got %v", reloaded.RegionPriority)
	}
}

func TestAccountAuthServerXPAndAnonymousIsolation(t *testing.T) {
	bz2Bytes, err := base64.StdEncoding.DecodeString(validTestBz2Base64)
	if err != nil {
		t.Fatalf("failed to decode test bz2 fixture: %v", err)
	}

	var submitAuthHeaders []string
	var userXPQueries atomic.Int32
	var serverUserXP atomic.Uint64
	serverUserXP.Store(200)

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/demo.dem.bz2":
			w.Header().Set("Content-Type", "application/x-bzip2")
			_, _ = w.Write(bz2Bytes)
		case "/api/v1/auth/register":
			var reqBody map[string]string
			_ = json.NewDecoder(r.Body).Decode(&reqBody)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(client.AuthResponse{
				Token: "sess-token-xyz",
				User: client.UserProfile{
					Nickname:      "Tester",
					Faction:       reqBody["faction"],
					XP:            200,
					MatchesParsed: 4,
				},
			})
		case "/api/v1/user/xp":
			userXPQueries.Add(1)
			if r.Header.Get("Authorization") != "Bearer sess-token-xyz" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(client.UserProfile{
				Nickname:      "Tester",
				Faction:       "radiant",
				XP:            serverUserXP.Load(),
				MatchesParsed: 5,
			})
		case "/api/v1/batch/submit":
			authHdr := r.Header.Get("Authorization")
			submitAuthHeaders = append(submitAuthHeaders, authHdr)
			if authHdr == "Bearer sess-token-xyz" {
				newXP := serverUserXP.Add(50)
				_ = json.NewEncoder(w).Encode(client.SubmitBatchResponse{
					Status:          "ACK",
					MatchesAccepted: 1,
					LeaseCompleted:  true,
					XPAwarded:       50,
					UserXP:          &newXP,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(client.SubmitBatchResponse{
				Status:          "ACK",
				MatchesAccepted: 1,
				LeaseCompleted:  true,
				XPAwarded:       0,
			})
		}
	}))
	defer mockServer.Close()

	tmpCfg := filepath.Join(t.TempDir(), "config.json")
	cfgMgr := config.NewManager(tmpCfg)
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = mockServer.URL
		c.PauseOnGaming = false
	})

	orch := NewOrchestrator(cfgMgr, sentinel.NewSentinel(cfgMgr), client.NewAPIClient(), downloader.NewDownloader())
	ctx := context.Background()

	// 1. Anonymous match submission must NOT send Authorization and updates local XP only
	orch.processSingleTask(ctx, "batch-anon", client.LeasedTask{
		MatchID: 7892000001, Cluster: 133, Region: "EU", URL: mockServer.URL + "/api/v1/demo.dem.bz2",
	})
	orch.syncUserXP(ctx)

	if len(submitAuthHeaders) != 1 || submitAuthHeaders[0] != "" {
		t.Fatalf("expected empty Authorization header for anonymous submit, got %v", submitAuthHeaders)
	}
	if userXPQueries.Load() != 0 {
		t.Fatalf("expected 0 /api/v1/user/xp calls while anonymous, got %d", userXPQueries.Load())
	}
	if got := orch.GetStatusSnapshot().ContributorXP; got != 50 {
		t.Fatalf("expected anonymous local XP 50, got %d", got)
	}

	// 2. Registering signs in with chosen permanent faction, adopts server XP, and scrubs token from StatusSnapshot.Config
	if err := orch.RegisterAccount(ctx, "Tester", "secret123", "radiant"); err != nil {
		t.Fatalf("RegisterAccount failed: %v", err)
	}
	snap := orch.GetStatusSnapshot()
	if !snap.Authenticated || snap.AccountNickname != "Tester" || snap.AccountFaction != "radiant" || snap.ContributorXP != 200 {
		t.Fatalf("unexpected authenticated snapshot: %+v", snap)
	}
	if snap.Config.AuthToken != "" {
		t.Fatalf("expected AuthToken scrubbed from StatusSnapshot.Config")
	}

	// 3. Authenticated submit sends Bearer token and updates XP from server UserXP
	orch.processSingleTask(ctx, "batch-auth", client.LeasedTask{
		MatchID: 7892000002, Cluster: 133, Region: "EU", URL: mockServer.URL + "/api/v1/demo.dem.bz2",
	})
	if len(submitAuthHeaders) != 2 || submitAuthHeaders[1] != "Bearer sess-token-xyz" {
		t.Fatalf("expected Bearer sess-token-xyz on authenticated submit, got %v", submitAuthHeaders)
	}
	if got := orch.GetStatusSnapshot().ContributorXP; got != 250 {
		t.Fatalf("expected synced server XP 250 after submit, got %d", got)
	}

	// 4. Periodic syncUserXP pulls external server XP gains (e.g., from another worker)
	serverUserXP.Store(350)
	orch.syncUserXP(ctx)
	if userXPQueries.Load() != 1 || orch.GetStatusSnapshot().ContributorXP != 350 {
		t.Fatalf("expected syncUserXP to update ContributorXP to 350, got %d (queries=%d)",
			orch.GetStatusSnapshot().ContributorXP, userXPQueries.Load())
	}

	// 5. Logout restores anonymous local XP and neutral theme cleanly
	orch.LogoutAccount(ctx)
	if snapOut := orch.GetStatusSnapshot(); snapOut.Authenticated || snapOut.AccountFaction != "" || snapOut.ContributorXP != 50 {
		t.Fatalf("expected logout to revert to anonymous neutral XP 50, got authenticated=%v faction=%q xp=%d",
			snapOut.Authenticated, snapOut.AccountFaction, snapOut.ContributorXP)
	}
}

func TestUnifiedWorkerStatusTransitions(t *testing.T) {
	tmpCfg := filepath.Join(t.TempDir(), "config.json")
	cfgMgr := config.NewManager(tmpCfg)
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.PauseOnGaming = false
		c.IdlePolicy = config.IdlePolicyAlwaysRun
	})

	orch := NewOrchestrator(cfgMgr, sentinel.NewSentinel(cfgMgr), client.NewAPIClient(), downloader.NewDownloader())

	// Initially offline -> Not Connected
	if snap := orch.GetStatusSnapshot(); snap.State != StateDisconnected || snap.StateLabel != "Not Connected" {
		t.Fatalf("expected StateDisconnected / Not Connected when offline, got %s (%q)", snap.State, snap.StateLabel)
	}

	// Connected and idle -> Ready
	orch.mu.Lock()
	orch.connected = true
	orch.mu.Unlock()
	if snap := orch.GetStatusSnapshot(); snap.State != StateReady || snap.StateLabel != "Ready" {
		t.Fatalf("expected StateReady / Ready when online and idle, got %s (%q)", snap.State, snap.StateLabel)
	}

	// Connected and actively parsing -> Active · Parsing
	orch.mu.Lock()
	orch.currentMatchID = 8123456
	orch.currentStage = "PARSING"
	orch.mu.Unlock()
	if snap := orch.GetStatusSnapshot(); snap.State != StateActive && snap.State != StateActiveTurboIdle {
		t.Fatalf("expected active state while parsing, got %s (%q)", snap.State, snap.StateLabel)
	}

	// User pause overrides active -> Paused (User)
	orch.SetUserPaused(true)
	if snap := orch.GetStatusSnapshot(); snap.State != StatePausedByUser || snap.StateLabel != "Paused (User)" {
		t.Fatalf("expected Paused (User), got %s (%q)", snap.State, snap.StateLabel)
	}
}

func decodeMockSubmitBody(r *http.Request) (*client.SubmitBatchRequest, error) {
	var reader io.Reader = r.Body
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = gz
	}
	var req client.SubmitBatchRequest
	if err := json.NewDecoder(reader).Decode(&req); err != nil {
		return nil, err
	}
	return &req, nil
}

func TestServerManagedCustomJSParserAutoSyncAndCache(t *testing.T) {
	bz2Bytes, err := base64.StdEncoding.DecodeString(validTestBz2Base64)
	if err != nil {
		t.Fatalf("failed to decode bz2 fixture: %v", err)
	}

	jsScript := []byte(`var n = stdin_len(); console.log(JSON.stringify({custom_ok: true, byte_len: n, match_id: DOTA_MATCH_ID}));`)
	sum := sha256.Sum256(jsScript)
	shaHex := hex.EncodeToString(sum[:])

	var dlHits atomic.Int32
	var lastSubmit atomic.Value

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stats":
			_ = json.NewEncoder(w).Encode(client.PublicStats{
				ActiveWorkers: 1,
				ActiveParser: &client.ParserDescriptor{
					Mode:         "custom",
					ID:           "wards-plus",
					Name:         "wards-plus",
					ReportedName: "custom(wards-plus)",
					Kind:         "js",
					Revision:     1,
					SHA256:       shaHex,
					DownloadURL:  "/api/v1/parser/artifact/" + shaHex,
				},
			})
		case "/api/v1/parser/artifact/" + shaHex:
			dlHits.Add(1)
			_, _ = w.Write(jsScript)
		case "/api/v1/demo.dem.bz2":
			w.Header().Set("Content-Type", "application/x-bzip2")
			_, _ = w.Write(bz2Bytes)
		case "/api/v1/batch/submit":
			req, decErr := decodeMockSubmitBody(r)
			if decErr != nil {
				http.Error(w, decErr.Error(), http.StatusBadRequest)
				return
			}
			lastSubmit.Store(req)
			_ = json.NewEncoder(w).Encode(client.SubmitBatchResponse{
				Status:          "ACK",
				MatchesAccepted: len(req.Results),
				XPAwarded:       XPPerMatch,
			})
		}
	}))
	defer srv.Close()

	cfgMgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = srv.URL
		c.PauseOnGaming = false
	})

	orch1 := NewOrchestrator(cfgMgr, sentinel.NewSentinel(cfgMgr), client.NewAPIClient(), downloader.NewDownloader())
	orch1.refreshServerStats(context.Background())

	if dlHits.Load() != 1 {
		t.Fatalf("expected 1 artifact download on initial sync, got %d", dlHits.Load())
	}
	cachedPath := filepath.Join(cfgMgr.ParserCacheDir(), shaHex+".js")
	if _, statErr := os.Stat(cachedPath); statErr != nil {
		t.Fatalf("expected downloaded artifact cached at %s: %v", cachedPath, statErr)
	}

	// Second orchestrator instance sharing ParserCacheDir must hit local SHA-256 cache cleanly.
	orch2 := NewOrchestrator(cfgMgr, sentinel.NewSentinel(cfgMgr), client.NewAPIClient(), downloader.NewDownloader())
	orch2.refreshServerStats(context.Background())
	if dlHits.Load() != 1 {
		t.Fatalf("expected cache hit with no extra download, got %d downloads", dlHits.Load())
	}

	orch1.processBatch(context.Background(), &client.LeaseResponse{
		BatchID: "batch-custom-js-01",
		Region:  "EU",
		Tasks: []client.LeasedTask{
			{MatchID: 910001, Cluster: 133, Region: "EU", ReplaySalt: 12345, URL: srv.URL + "/api/v1/demo.dem.bz2"},
		},
	})

	rawSub := lastSubmit.Load()
	if rawSub == nil {
		t.Fatalf("expected batch submit request from custom JS sandbox")
	}
	subReq := rawSub.(*client.SubmitBatchRequest)
	if len(subReq.Results) != 1 {
		t.Fatalf("expected 1 submitted result, got %d", len(subReq.Results))
	}
	res := subReq.Results[0]
	if res.ParserName != "custom(wards-plus)" || res.ParserVersion != "r1" || res.ParserSHA256 != shaHex {
		t.Fatalf("unexpected reported parser identity: %+v", res)
	}
	var payloadMap map[string]any
	if err := json.Unmarshal(res.Payload, &payloadMap); err != nil {
		t.Fatalf("invalid sandbox JSON payload: %v", err)
	}
	if okVal, _ := payloadMap["custom_ok"].(bool); !okVal {
		t.Fatalf("expected custom_ok=true in payload, got %s", string(res.Payload))
	}
	if cfgMgr.Get().ContributorXP != XPPerMatch {
		t.Fatalf("expected ContributorXP=%d after accepted parse, got %d", XPPerMatch, cfgMgr.Get().ContributorXP)
	}
	if snap := orch1.GetStatusSnapshot(); snap.ActiveParser == nil || snap.ActiveParser.ReportedName != "custom(wards-plus)" {
		t.Fatalf("expected StatusSnapshot.ActiveParser to report custom(wards-plus), got %+v", snap.ActiveParser)
	}
}

func TestServerDismissalZeroXPAndStaleBatchAbort(t *testing.T) {
	bz2Bytes, err := base64.StdEncoding.DecodeString(validTestBz2Base64)
	if err != nil {
		t.Fatalf("failed to decode bz2 fixture: %v", err)
	}

	jsV1 := []byte(`console.log(JSON.stringify({v: 1, len: stdin_len()}));`)
	sumV1 := sha256.Sum256(jsV1)
	shaV1 := hex.EncodeToString(sumV1[:])

	jsV2 := []byte(`console.log(JSON.stringify({v: 2, len: stdin_len()}));`)
	sumV2 := sha256.Sum256(jsV2)
	shaV2 := hex.EncodeToString(sumV2[:])

	descV1 := &client.ParserDescriptor{
		Mode:         "custom",
		ID:           "v1-old",
		Name:         "v1-old",
		ReportedName: "custom(v1-old)",
		Kind:         "js",
		Revision:     1,
		SHA256:       shaV1,
		DownloadURL:  "/api/v1/parser/artifact/" + shaV1,
	}
	descV2 := &client.ParserDescriptor{
		Mode:         "custom",
		ID:           "v2-new",
		Name:         "v2-new",
		ReportedName: "custom(v2-new)",
		Kind:         "js",
		Revision:     2,
		SHA256:       shaV2,
		DownloadURL:  "/api/v1/parser/artifact/" + shaV2,
	}

	var submitCount atomic.Int32
	var lastAcceptedParser atomic.Value

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stats":
			_ = json.NewEncoder(w).Encode(client.PublicStats{ActiveParser: descV1})
		case "/api/v1/parser/artifact/" + shaV1:
			_, _ = w.Write(jsV1)
		case "/api/v1/parser/artifact/" + shaV2:
			_, _ = w.Write(jsV2)
		case "/api/v1/demo.dem.bz2":
			w.Header().Set("Content-Type", "application/x-bzip2")
			_, _ = w.Write(bz2Bytes)
		case "/api/v1/batch/submit":
			req, decErr := decodeMockSubmitBody(r)
			if decErr != nil {
				http.Error(w, decErr.Error(), http.StatusBadRequest)
				return
			}
			submitCount.Add(1)
			res := req.Results[0]
			if res.ParserSHA256 != shaV2 {
				_ = json.NewEncoder(w).Encode(client.SubmitBatchResponse{
					Status:           "ACK",
					MatchesAccepted:  0,
					MatchesDismissed: 1,
					XPAwarded:        0,
					ActiveParser:     descV2,
				})
				return
			}
			lastAcceptedParser.Store(res.ParserName + "@" + res.ParserSHA256)
			_ = json.NewEncoder(w).Encode(client.SubmitBatchResponse{
				Status:           "ACK",
				MatchesAccepted:  1,
				MatchesDismissed: 0,
				XPAwarded:        XPPerMatch,
				ActiveParser:     descV2,
			})
		}
	}))
	defer srv.Close()

	cfgMgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = srv.URL
		c.PauseOnGaming = false
	})

	orch := NewOrchestrator(cfgMgr, sentinel.NewSentinel(cfgMgr), client.NewAPIClient(), downloader.NewDownloader())
	orch.refreshServerStats(context.Background())

	// First batch has 2 tasks, starting under v1. Task 1 is dismissed with descV2; Task 2 must abort.
	orch.processBatch(context.Background(), &client.LeaseResponse{
		BatchID: "batch-stale-01",
		Region:  "EU",
		Tasks: []client.LeasedTask{
			{MatchID: 920001, Cluster: 133, Region: "EU", ReplaySalt: 12345, URL: srv.URL + "/api/v1/demo.dem.bz2"},
			{MatchID: 920002, Cluster: 133, Region: "EU", ReplaySalt: 12345, URL: srv.URL + "/api/v1/demo.dem.bz2"},
		},
	})

	if got := submitCount.Load(); got != 1 {
		t.Fatalf("expected stale batch to abort after 1 dismissed submission, got %d submits", got)
	}
	if gotXP := cfgMgr.Get().ContributorXP; gotXP != 0 {
		t.Fatalf("expected 0 XP after server dismissed stale parse, got %d", gotXP)
	}
	if gotTotal := cfgMgr.Get().TotalParsedAllTime; gotTotal != 0 {
		t.Fatalf("expected 0 TotalParsedAllTime after dismissed parse, got %d", gotTotal)
	}
	snap := orch.GetStatusSnapshot()
	if snap.ActiveParser == nil || snap.ActiveParser.ReportedName != "custom(v2-new)" || snap.ActiveParser.SHA256 != shaV2 {
		t.Fatalf("expected worker to immediately sync to descV2, got %+v", snap.ActiveParser)
	}

	// Next batch executes under v2 and succeeds.
	orch.processBatch(context.Background(), &client.LeaseResponse{
		BatchID: "batch-fresh-02",
		Region:  "EU",
		Tasks: []client.LeasedTask{
			{MatchID: 920003, Cluster: 133, Region: "EU", ReplaySalt: 12345, URL: srv.URL + "/api/v1/demo.dem.bz2"},
		},
	})
	if got := submitCount.Load(); got != 2 {
		t.Fatalf("expected second batch to submit under v2, total submits=%d", got)
	}
	if lastAcceptedParser.Load() != "custom(v2-new)@"+shaV2 {
		t.Fatalf("expected accepted match reported with custom(v2-new), got %v", lastAcceptedParser.Load())
	}
	if gotXP := cfgMgr.Get().ContributorXP; gotXP != XPPerMatch {
		t.Fatalf("expected %d XP after v2 accepted parse, got %d", XPPerMatch, gotXP)
	}
}

func TestWorkerStopsBatchOnConnectionLoss(t *testing.T) {
	bz2Bytes, err := base64.StdEncoding.DecodeString(validTestBz2Base64)
	if err != nil {
		t.Fatalf("failed to decode bz2 fixture: %v", err)
	}

	var downloadCount atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stats":
			_ = json.NewEncoder(w).Encode(client.PublicStats{})
		case "/api/v1/demo.dem.bz2":
			downloadCount.Add(1)
			w.Header().Set("Content-Type", "application/x-bzip2")
			_, _ = w.Write(bz2Bytes)
		case "/api/v1/batch/submit":
			// Simulate connection drop when submitting task 1.
			srv.CloseClientConnections()
		}
	}))
	defer srv.Close()

	cfgMgr := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = srv.URL
		c.PauseOnGaming = false
	})

	orch := NewOrchestrator(cfgMgr, sentinel.NewSentinel(cfgMgr), client.NewAPIClient(), downloader.NewDownloader())
	orch.SetCustomBlackBoxParser(&customBlackBoxStub{})
	orch.refreshServerStats(context.Background())

	if snap := orch.GetStatusSnapshot(); !snap.Connected {
		t.Fatalf("expected initial Connected=true")
	}

	// Batch has 2 tasks. Task 1's SubmitResults fails due to dropped connection; Task 2 must not run.
	orch.processBatch(context.Background(), &client.LeaseResponse{
		BatchID: "batch-disconnect-01",
		Region:  "EU",
		Tasks: []client.LeasedTask{
			{MatchID: 930001, Cluster: 133, Region: "EU", ReplaySalt: 12345, URL: srv.URL + "/api/v1/demo.dem.bz2"},
			{MatchID: 930002, Cluster: 133, Region: "EU", ReplaySalt: 12345, URL: srv.URL + "/api/v1/demo.dem.bz2"},
		},
	})

	if got := downloadCount.Load(); got != 1 {
		t.Fatalf("expected batch to stop after connection loss on task 1 (1 download), got %d downloads", got)
	}
	snap := orch.GetStatusSnapshot()
	if snap.Connected {
		t.Fatalf("expected Connected=false after connection loss")
	}
	if snap.State != StateDisconnected {
		t.Fatalf("expected State=%s after connection loss, got %s", StateDisconnected, snap.State)
	}

	// Also verify refreshServerStats cancels an in-flight gated batch immediately when server becomes unreachable.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stats":
			_ = json.NewEncoder(w).Encode(client.PublicStats{})
		case "/api/v1/demo.dem.bz2":
			downloadCount.Add(1)
			w.Header().Set("Content-Type", "application/x-bzip2")
			_, _ = w.Write(bz2Bytes)
		}
	}))
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = srv2.URL
	})
	orch.refreshServerStats(context.Background())
	orch.SetUserPaused(true)

	done := make(chan struct{})
	go func() {
		orch.processBatch(context.Background(), &client.LeaseResponse{
			BatchID: "batch-disconnect-02",
			Region:  "EU",
			Tasks: []client.LeasedTask{
				{MatchID: 930003, Cluster: 133, Region: "EU", ReplaySalt: 12345, URL: srv2.URL + "/api/v1/demo.dem.bz2"},
			},
		})
		close(done)
	}()

	time.Sleep(80 * time.Millisecond)
	srv2.Close()
	orch.refreshServerStats(context.Background())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected paused processBatch to stop immediately when connection was lost")
	}
}


