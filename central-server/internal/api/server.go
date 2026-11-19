package api

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dota2-analytics/central-server/internal/accounts"
	"github.com/dota2-analytics/central-server/internal/harvester"
	"github.com/dota2-analytics/central-server/internal/models"
	"github.com/dota2-analytics/central-server/internal/parsers"
	"github.com/dota2-analytics/central-server/internal/queue"
	"github.com/dota2-analytics/central-server/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

//go:embed pictures/*.png
var embeddedPictures embed.FS

const sampleBz2Base64 = "QlpoOTFBWSZTWVjtQx8AACqbgFAEfxAAAK5t3qogAFDGAAAAAGqeUGR6mmQGnqZpDu0A69qbZXc7EWt+8ZROWMQOH0FJkpjVwqMVcpBiRmRJm/uEKoGNUJzk5G4R+H1rfi7kinChILHahj4="

// factionCacheTTL bounds how often the faction SUM query runs, since every worker polls /stats every 3s.
const factionCacheTTL = 2 * time.Second

// Server wraps the Fiber HTTP server and coordinates API handlers.
type Server struct {
	app          *fiber.App
	leaseMgr     *queue.LeaseManager
	filterEngine *harvester.FilterEngine
	harvester    *harvester.Harvester
	store        *storage.AnalyticsStore
	accounts     *accounts.Store
	parsers      *parsers.Manager
	adminHTML    string
	apiKeysPath  string

	factionMu       sync.Mutex
	factionCache    *models.FactionStandings
	factionCachedAt time.Time
}

// NewServer creates and configures the Central Server REST API Gateway and Admin UI.
func NewServer(
	leaseMgr *queue.LeaseManager,
	filterEngine *harvester.FilterEngine,
	harv *harvester.Harvester,
	store *storage.AnalyticsStore,
	accStore *accounts.Store,
	parserMgr *parsers.Manager,
	adminHTMLPath string,
) *Server {
	app := fiber.New(fiber.Config{
		AppName:               "Dota2ReplayCourier Coordinator v1.2.0",
		DisableStartupMessage: false,
		BodyLimit:             50 * 1024 * 1024,
	})

	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept, Authorization, Content-Encoding, X-Region-Override, CF-IPCountry",
		AllowMethods: "GET, POST, PUT, DELETE, OPTIONS",
	}))

	s := &Server{
		app:          app,
		leaseMgr:     leaseMgr,
		filterEngine: filterEngine,
		harvester:    harv,
		store:        store,
		accounts:     accStore,
		parsers:      parserMgr,
		adminHTML:    loadAdminHTML(adminHTMLPath),
	}

	s.registerRoutes()
	return s
}

func loadAdminHTML(customPath string) string {
	candidates := []string{
		customPath,
		filepath.Join("web", "admin", "index.html"),
		filepath.Join("central-server", "web", "admin", "index.html"),
	}
	for _, p := range candidates {
		if p == "" {
			continue
		}
		if raw, err := os.ReadFile(p); err == nil && len(raw) > 0 {
			return string(raw)
		}
	}
	return "<html><body><h1>Dota2ReplayCourier Coordinator</h1></body></html>"
}

// App returns the underlying Fiber application.
func (s *Server) App() *fiber.App {
	return s.app
}

// Listen starts the HTTP server on the given address.
func (s *Server) Listen(addr string) error {
	return s.app.Listen(addr)
}

func (s *Server) registerRoutes() {
	s.app.Get("/", func(c *fiber.Ctx) error {
		return c.Redirect("/admin")
	})

	s.app.Get("/admin", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return c.SendString(s.adminHTML)
	})

	s.app.Get("/pictures/:name", func(c *fiber.Ctx) error {
		name := filepath.Base(c.Params("name"))
		if raw, err := embeddedPictures.ReadFile("pictures/" + name); err == nil {
			c.Set(fiber.HeaderContentType, "image/png")
			return c.Send(raw)
		}
		dirs := []string{
			"pictures",
			filepath.Join("..", "pictures"),
			filepath.Join("..", "..", "pictures"),
		}
		if exe, err := os.Executable(); err == nil {
			exeDir := filepath.Dir(exe)
			dirs = append(dirs,
				filepath.Join(exeDir, "pictures"),
				filepath.Join(exeDir, "..", "pictures"),
			)
		}
		for _, dir := range dirs {
			p := filepath.Join(dir, name)
			if _, err := os.Stat(p); err == nil {
				return c.SendFile(p)
			}
		}
		return c.SendStatus(http.StatusNotFound)
	})

	s.app.Get("/healthz", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "time": time.Now().UTC()})
	})

	// Worker, Auth & External Consumer API v1
	v1 := s.app.Group("/api/v1")
	v1.Get("/stats", s.handlePublicStats)
	v1.Post("/batch/lease", s.handleBatchLease)
	v1.Post("/batch/submit", s.handleBatchSubmit)
	v1.Get("/salt/:match_id", s.handleResolveSaltFallback)
	v1.Get("/demo/sample.dem.bz2", s.handleDemoReplayDownload)
	v1.Get("/parser/active", s.handleGetActiveParser)
	v1.Get("/parser/artifact/:sha256", s.handleGetParserArtifact)

	// User Account Portal & Self-Only XP Endpoints
	v1.Post("/auth/register", s.handleAuthRegister)
	v1.Post("/auth/login", s.handleAuthLogin)
	v1.Post("/auth/logout", s.handleAuthLogout)
	v1.Get("/user/xp", s.handleGetUserXP)

	// External Program Consumption Endpoints
	v1.Get("/matches", s.handleListMatches)
	v1.Get("/matches/:match_id", s.handleGetMatchByID)
	v1.Get("/stream/matches", s.handleStreamMatches)

	// Admin API v1
	admin := v1.Group("/admin")
	admin.Get("/overview", s.handleAdminOverview)
	admin.Get("/filters", s.handleGetFilters)
	admin.Put("/filters", s.handleUpdateFilters)
	admin.Put("/sink", s.handleUpdateSink)
	admin.Put("/file-storage", s.handleUpdateFileStorage)
	admin.Get("/api-keys", s.handleGetAPIKeys)
	admin.Put("/api-keys", s.handleUpdateAPIKeys)
	admin.Post("/tutorial/complete", s.handleCompleteTutorial)
	admin.Post("/inject", s.handleInjectJob)
	admin.Post("/inject-demo-batch", s.handleInjectDemoBatch)
	admin.Post("/poll-now", s.handlePollNow)
	admin.Get("/parsers", s.handleAdminListParsers)
	admin.Post("/parsers/upload", s.handleAdminUploadParser)
	admin.Put("/parsers/active", s.handleAdminActivateParser)
	admin.Delete("/parsers/:id", s.handleAdminDeleteParser)
}

func (s *Server) activeParserPtr() *models.ParserDescriptor {
	if s.parsers == nil {
		return nil
	}
	act := s.parsers.GetActive()
	return &act
}

// factionStandings returns the cached Radiant vs Dire standings (nil if accounts are unavailable).
func (s *Server) factionStandings(ctx context.Context) *models.FactionStandings {
	if s.accounts == nil {
		return nil
	}
	s.factionMu.Lock()
	defer s.factionMu.Unlock()
	if s.factionCache != nil && time.Since(s.factionCachedAt) < factionCacheTTL {
		cp := *s.factionCache
		return &cp
	}
	st, err := s.accounts.GetFactionStandings(ctx)
	if err != nil {
		if s.factionCache != nil {
			cp := *s.factionCache
			return &cp
		}
		return nil
	}
	s.factionCache = &models.FactionStandings{
		Radiant: models.FactionStanding{XP: st.Radiant.XP, Members: st.Radiant.Members},
		Dire:    models.FactionStanding{XP: st.Dire.XP, Members: st.Dire.Members},
	}
	s.factionCachedAt = time.Now()
	cp := *s.factionCache
	return &cp
}

func (s *Server) invalidateFactionCache() {
	s.factionMu.Lock()
	s.factionCachedAt = time.Time{}
	s.factionMu.Unlock()
}

func (s *Server) handlePublicStats(c *fiber.Ctx) error {
	ctx := c.UserContext()
	depths, delayedCount, _ := s.leaseMgr.GetQueueDepths(ctx)
	leases, _ := s.leaseMgr.ListActiveLeases(ctx)
	totalParsed, todayParsed, _ := s.store.GetCounters()

	return c.JSON(models.PublicStats{
		GlobalParsedTotal: totalParsed,
		GlobalParsedToday: todayParsed,
		ActiveWorkers:     len(leases),
		ActiveLeases:      len(leases),
		QueueDepthByReg:   depths,
		DelayedQueueCount: delayedCount,
		ActiveParser:      s.activeParserPtr(),
		Factions:          s.factionStandings(ctx),
		ServerTime:        time.Now().UTC(),
	})
}

func extractBearerToken(c *fiber.Ctx) string {
	auth := strings.TrimSpace(c.Get("Authorization"))
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return ""
}

func (s *Server) handleBatchLease(c *fiber.Ctx) error {
	ctx := c.UserContext()

	var req models.LeaseRequest
	if len(c.Body()) > 0 {
		_ = json.Unmarshal(c.Body(), &req)
	}

	region := req.PreferredRegion
	if override := strings.TrimSpace(c.Get("X-Region-Override")); override != "" {
		region = models.MacroRegion(strings.ToUpper(override))
	}
	if (region == "" || region == "AUTO") && len(req.RegionPriority) == 0 {
		if cfCountry := c.Get("CF-IPCountry"); cfCountry != "" {
			region = models.CountryToRegion(cfCountry)
		} else {
			region = models.RegionEU
		}
	}

	// worker_id identifies the worker machine independently of the user account token.
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		workerID = "anon-" + c.IP()
	}

	maxTasks := req.MaxTasks
	if maxTasks <= 0 {
		maxTasks = 5
	}

	leaseResp, err := s.leaseMgr.LeaseBatch(ctx, region, req.RegionPriority, workerID, c.IP(), maxTasks)
	if err != nil {
		if errors.Is(err, queue.ErrTooManyLeases) {
			return c.Status(http.StatusTooManyRequests).JSON(fiber.Map{
				"error":   "too_many_active_leases",
				"message": "Worker already holds maximum allowed active leases.",
			})
		}
		if errors.Is(err, queue.ErrEmptyQueues) {
			return c.Status(http.StatusNoContent).JSON(fiber.Map{
				"status": "EMPTY",
				"tasks":  []models.LeasedTask{},
			})
		}
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// If the worker notified us that it exhausted its local OpenDota/STRATZ rate limit,
	// use the Coordinator's reserve quota to resolve salts for this leased batch.
	if req.NeedsServerSalt {
		leaseResp.ServerAssistedSalt = true
		for i := range leaseResp.Tasks {
			t := &leaseResp.Tasks[i]
			if t.URL == "" && t.ReplaySalt == 0 {
				if saltRes, err := s.harvester.ResolveJobSalt(ctx, t.MatchID, t.Cluster); err == nil && saltRes.ReplaySalt > 0 {
					if saltRes.Cluster > 0 {
						t.Cluster = saltRes.Cluster
					}
					t.ReplaySalt = saltRes.ReplaySalt
					t.URL = models.BuildReplayURL(t.Cluster, t.MatchID, t.ReplaySalt)
				}
			}
		}
	}

	leaseResp.ActiveParser = s.activeParserPtr()
	return c.Status(http.StatusOK).JSON(leaseResp)
}

// handleResolveSaltFallback resolves a single match's replay_salt on behalf of a worker
// that hit its local rate limit mid-batch.
func (s *Server) handleResolveSaltFallback(c *fiber.Ctx) error {
	ctx := c.UserContext()
	matchID, err := strconv.ParseUint(c.Params("match_id"), 10, 64)
	if err != nil || matchID == 0 {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid match_id"})
	}
	clusterHint := uint32(c.QueryInt("cluster", 133))

	saltRes, err := s.harvester.ResolveJobSalt(ctx, matchID, clusterHint)
	if err != nil || saltRes.ReplaySalt == 0 {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "replay_salt not available"})
	}
	return c.JSON(fiber.Map{
		"match_id":    matchID,
		"cluster":     saltRes.Cluster,
		"replay_salt": saltRes.ReplaySalt,
		"provider":    saltRes.Provider,
		"url":         models.BuildReplayURL(saltRes.Cluster, matchID, saltRes.ReplaySalt),
	})
}

func (s *Server) handleBatchSubmit(c *fiber.Ctx) error {
	ctx := c.UserContext()
	rawBody := c.Body()

	if len(rawBody) >= 2 && rawBody[0] == 0x1f && rawBody[1] == 0x8b {
		gz, err := gzip.NewReader(bytes.NewReader(rawBody))
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid gzip body"})
		}
		defer gz.Close()
		decompressed, err := io.ReadAll(io.LimitReader(gz, 50*1024*1024))
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "failed to decompress gzip body"})
		}
		rawBody = decompressed
	}

	var req models.SubmitBatchRequest
	if err := json.Unmarshal(rawBody, &req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON payload: " + err.Error()})
	}

	if req.BatchID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "batch_id is required"})
	}
	if strings.TrimSpace(req.WorkerID) == "" {
		req.WorkerID = "anon-" + c.IP()
	}

	var validator func(string, string) bool
	if s.parsers != nil {
		validator = s.parsers.MatchesActive
	}
	outcome, err := s.leaseMgr.SubmitBatchResultsWithValidator(ctx, req, validator)
	if err != nil {
		if errors.Is(err, queue.ErrLeaseNotFound) {
			return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	if len(outcome.ValidRecords) > 0 {
		s.store.EnqueueRecords(outcome.ValidRecords)
	}

	// Calculate and credit flat +50 XP per verified accepted match ONLY if authenticated to a user account.
	if s.accounts != nil && outcome.Response.MatchesAccepted > 0 {
		if token := extractBearerToken(c); token != "" {
			if user, authErr := s.accounts.Authenticate(ctx, token); authErr == nil {
				if awarded, updated, xpErr := s.accounts.AwardMatchXP(ctx, user.ID, outcome.Response.MatchesAccepted); xpErr == nil && updated != nil {
					outcome.Response.XPAwarded = awarded
					xpVal := updated.XP
					outcome.Response.UserXP = &xpVal
					if updated.Faction != "" {
						s.invalidateFactionCache()
					}
				}
			}
		}
	}

	outcome.Response.ActiveParser = s.activeParserPtr()
	return c.Status(http.StatusOK).JSON(outcome.Response)
}

func (s *Server) handleAuthRegister(c *fiber.Ctx) error {
	if s.accounts == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "accounts store unavailable"})
	}
	var body struct {
		Nickname string `json:"nickname"`
		Password string `json:"password"`
		Faction  string `json:"faction"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}

	token, user, err := s.accounts.Register(c.UserContext(), body.Nickname, body.Password, body.Faction)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, accounts.ErrNicknameTaken) {
			status = http.StatusConflict
		}
		return c.Status(status).JSON(fiber.Map{"error": err.Error()})
	}
	if user != nil && user.Faction != "" {
		s.invalidateFactionCache()
	}

	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"token": token,
		"user":  user,
	})
}

func (s *Server) handleAuthLogin(c *fiber.Ctx) error {
	if s.accounts == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "accounts store unavailable"})
	}
	var body struct {
		Nickname string `json:"nickname"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}

	token, user, err := s.accounts.Login(c.UserContext(), body.Nickname, body.Password)
	if err != nil {
		return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(http.StatusOK).JSON(fiber.Map{
		"token": token,
		"user":  user,
	})
}

func (s *Server) handleAuthLogout(c *fiber.Ctx) error {
	if s.accounts == nil {
		return c.SendStatus(http.StatusNoContent)
	}
	if token := extractBearerToken(c); token != "" {
		_ = s.accounts.Logout(c.UserContext(), token)
	}
	return c.JSON(fiber.Map{"status": "logged_out"})
}

// handleGetUserXP returns ONLY the authenticated user's own fields (xp, matches_parsed, other_data, etc.).
func (s *Server) handleGetUserXP(c *fiber.Ctx) error {
	if s.accounts == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "accounts store unavailable"})
	}
	token := extractBearerToken(c)
	user, err := s.accounts.Authenticate(c.UserContext(), token)
	if err != nil {
		return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "authentication required"})
	}
	return c.JSON(user)
}

func (s *Server) handleListMatches(c *fiber.Ctx) error {
	limit := c.QueryInt("limit", 50)
	return c.JSON(s.store.GetRecentRecords(limit))
}

func (s *Server) handleGetMatchByID(c *fiber.Ctx) error {
	idStr := c.Params("match_id")
	idStr = strings.TrimSuffix(idStr, ".json")
	matchID, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid match_id"})
	}
	rec, err := s.store.GetMatchByID(matchID)
	if err != nil {
		if os.IsNotExist(err) {
			return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "match file not found"})
		}
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(rec)
}

// handleStreamMatches streams newly parsed matches in real time (SSE / NDJSON) to connected external programs.
func (s *Server) handleStreamMatches(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")

	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		ch, unsubscribe := s.store.SubscribeStream()
		defer unsubscribe()

		for rec := range ch {
			raw, err := json.Marshal(rec)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", string(raw)); err != nil {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}
		}
	})
	return nil
}

func (s *Server) handleDemoReplayDownload(c *fiber.Ctx) error {
	block, err := base64.StdEncoding.DecodeString(sampleBz2Base64)
	if err != nil {
		return c.SendStatus(http.StatusInternalServerError)
	}

	repeats := 1800
	buf := bytes.NewBuffer(make([]byte, 0, len(block)*repeats))
	for i := 0; i < repeats; i++ {
		buf.Write(block)
	}

	data := buf.Bytes()
	c.Set("Content-Type", "application/x-bzip2")
	c.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	return c.Send(data)
}

func (s *Server) handleAdminOverview(c *fiber.Ctx) error {
	ctx := c.UserContext()
	depths, delayedCount, _ := s.leaseMgr.GetQueueDepths(ctx)
	leases, _ := s.leaseMgr.ListActiveLeases(ctx)
	totalParsed, todayParsed, _ := s.store.GetCounters()

	var customParsers []models.ParserDescriptor
	var activeParser models.ParserDescriptor
	if s.parsers != nil {
		activeParser, customParsers = s.parsers.ListAll()
	}

	stats := models.PublicStats{
		GlobalParsedTotal: totalParsed,
		GlobalParsedToday: todayParsed,
		ActiveWorkers:     len(leases),
		ActiveLeases:      len(leases),
		QueueDepthByReg:   depths,
		DelayedQueueCount: delayedCount,
		ActiveParser:      s.activeParserPtr(),
		Factions:          s.factionStandings(ctx),
		ServerTime:        time.Now().UTC(),
	}

	payload := fiber.Map{
		"stats":              stats,
		"sink_stats":         s.store.GetSinkStats(),
		"filter_config":      s.filterEngine.GetConfig(),
		"filter_stats":       s.filterEngine.GetStats(),
		"active_leases":      leases,
		"recent_results":     s.store.GetRecentRecords(10),
		"active_parser":      activeParser,
		"custom_parsers":     customParsers,
		"api_keys":           s.apiKeysPayload(),
		"tutorial_completed": s.isTutorialCompleted(),
	}
	if c.Query("slim") != "1" {
		payload["lobby_catalog"] = models.LobbyTypeCatalog
		payload["game_mode_catalog"] = models.GameModeCatalog
	}
	return c.JSON(payload)
}

// SetAPIKeysPath configures the file path where runtime-configured API keys are persisted.
func (s *Server) SetAPIKeysPath(path string) {
	s.apiKeysPath = path
}

func (s *Server) uiStatePath() string {
	if s.apiKeysPath != "" {
		return filepath.Join(filepath.Dir(s.apiKeysPath), "admin_ui_state.json")
	}
	return filepath.Join("data", "admin_ui_state.json")
}

func (s *Server) filterConfigPath() string {
	if s.apiKeysPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(s.apiKeysPath), "filter_config.json")
}

func (s *Server) sinkConfigPath() string {
	if s.apiKeysPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(s.apiKeysPath), "sink_config.json")
}

type adminUIState struct {
	TutorialCompleted bool `json:"tutorial_completed"`
}

func (s *Server) isTutorialCompleted() bool {
	raw, err := os.ReadFile(s.uiStatePath())
	if err != nil {
		return false
	}
	var st adminUIState
	if err := json.Unmarshal(raw, &st); err != nil {
		return false
	}
	return st.TutorialCompleted
}

func (s *Server) handleCompleteTutorial(c *fiber.Ctx) error {
	p := s.uiStatePath()
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	data, _ := json.MarshalIndent(adminUIState{TutorialCompleted: true}, "", "  ")
	if err := os.WriteFile(p, data, 0644); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"tutorial_completed": true})
}

type persistedAPIKeys struct {
	SteamAPIKey    string `json:"steam_api_key"`
	StratzAPIKey   string `json:"stratz_api_key"`
	OpenDotaAPIKey string `json:"opendota_api_key"`
}

// LoadPersistedAPIKeys loads saved Steam, STRATZ, and OpenDota API keys from disk if present.
func LoadPersistedAPIKeys(path string) (steam, stratz, openDota string, ok bool) {
	if path == "" {
		return "", "", "", false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", "", false
	}
	var cfg persistedAPIKeys
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", "", "", false
	}
	return strings.TrimSpace(cfg.SteamAPIKey), strings.TrimSpace(cfg.StratzAPIKey), strings.TrimSpace(cfg.OpenDotaAPIKey), true
}

func savePersistedAPIKeys(path, steam, stratz, openDota string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(persistedAPIKeys{
		SteamAPIKey:    strings.TrimSpace(steam),
		StratzAPIKey:   strings.TrimSpace(stratz),
		OpenDotaAPIKey: strings.TrimSpace(openDota),
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func (s *Server) apiKeysPayload() fiber.Map {
	steam, stratz, openDota := s.harvester.GetAPIKeys()
	return fiber.Map{
		"steam_api_key":       steam,
		"stratz_api_key":      stratz,
		"opendota_api_key":    openDota,
		"steam_configured":    steam != "",
		"stratz_configured":   stratz != "",
		"opendota_configured": openDota != "",
	}
}

func (s *Server) handleGetAPIKeys(c *fiber.Ctx) error {
	return c.JSON(s.apiKeysPayload())
}

func (s *Server) handleUpdateAPIKeys(c *fiber.Ctx) error {
	var body persistedAPIKeys
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	prevSteam, _, _ := s.harvester.GetAPIKeys()
	steam := strings.TrimSpace(body.SteamAPIKey)
	stratz := strings.TrimSpace(body.StratzAPIKey)
	openDota := strings.TrimSpace(body.OpenDotaAPIKey)
	s.harvester.SetAPIKeys(steam, stratz, openDota)
	if err := savePersistedAPIKeys(s.apiKeysPath, steam, stratz, openDota); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if steam != prevSteam && s.filterEngine.GetConfig().Enabled {
		go func() {
			_ = s.harvester.PollOnce(context.Background())
		}()
	}
	return c.JSON(s.apiKeysPayload())
}

type persistedSinkConfig struct {
	WebhookURL         string `json:"webhook_url"`
	WebhookEnabled     bool   `json:"webhook_enabled"`
	FileStorageEnabled bool   `json:"file_storage_enabled"`
}

// LoadPersistedFilterConfig loads saved match filter settings from disk (always keeping Harvester off at startup).
func LoadPersistedFilterConfig(path string) (models.FilterConfig, bool) {
	if path == "" {
		return models.DefaultFilterConfig(), false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return models.DefaultFilterConfig(), false
	}
	var cfg models.FilterConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return models.DefaultFilterConfig(), false
	}
	cfg.Enabled = false
	return cfg, true
}

func savePersistedFilterConfig(path string, cfg models.FilterConfig) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	cfg.Enabled = false
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// LoadPersistedSinkConfig loads saved webhook and file storage settings from disk if present.
func LoadPersistedSinkConfig(path string) (webhookURL string, webhookEnabled bool, fileStorageEnabled bool, ok bool) {
	if path == "" {
		return "", false, true, false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false, true, false
	}
	var cfg persistedSinkConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", false, true, false
	}
	return strings.TrimSpace(cfg.WebhookURL), cfg.WebhookEnabled, cfg.FileStorageEnabled, true
}

func savePersistedSinkConfig(path, webhookURL string, webhookEnabled, fileStorageEnabled bool) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(persistedSinkConfig{
		WebhookURL:         strings.TrimSpace(webhookURL),
		WebhookEnabled:     webhookEnabled,
		FileStorageEnabled: fileStorageEnabled,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (s *Server) handleUpdateSink(c *fiber.Ctx) error {
	var body struct {
		WebhookURL     string `json:"webhook_url"`
		WebhookEnabled bool   `json:"webhook_enabled"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	cfg := s.store.UpdateSinkConfig(strings.TrimSpace(body.WebhookURL), body.WebhookEnabled)
	if err := savePersistedSinkConfig(s.sinkConfigPath(), cfg.WebhookURL, cfg.WebhookEnabled, cfg.FileStorageEnabled); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(cfg)
}

func (s *Server) handleUpdateFileStorage(c *fiber.Ctx) error {
	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	cfg := s.store.SetFileStorageEnabled(body.Enabled)
	if err := savePersistedSinkConfig(s.sinkConfigPath(), cfg.WebhookURL, cfg.WebhookEnabled, cfg.FileStorageEnabled); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(cfg)
}

func (s *Server) handleGetFilters(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"config":            s.filterEngine.GetConfig(),
		"stats":             s.filterEngine.GetStats(),
		"lobby_catalog":     models.LobbyTypeCatalog,
		"game_mode_catalog": models.GameModeCatalog,
	})
}

func (s *Server) handleUpdateFilters(c *fiber.Ctx) error {
	ctx := c.UserContext()
	var cfg models.FilterConfig
	if err := json.Unmarshal(c.Body(), &cfg); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	updated := s.filterEngine.UpdateConfig(cfg)
	_ = s.leaseMgr.SaveFilterConfig(ctx, updated)
	if err := savePersistedFilterConfig(s.filterConfigPath(), updated); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(updated)
}

func (s *Server) handleInjectJob(c *fiber.Ctx) error {
	ctx := c.UserContext()
	var req models.InjectJobRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	if req.MatchID == 0 {
		req.MatchID = uint64(7890000000 + rand.Int63n(9000000))
	}
	if req.Cluster == 0 {
		req.Cluster = 133
	}
	if req.Region == "" {
		req.Region = models.ResolveClusterRegion(req.Cluster)
	}
	if req.DurationSeconds <= 0 {
		req.DurationSeconds = 2145
	}

	url := strings.TrimSpace(req.CustomURL)
	if url == "" {
		if req.ReplaySalt == 0 {
			url = c.BaseURL() + fmt.Sprintf("/api/v1/demo/sample.dem.bz2?match_id=%d", req.MatchID)
		} else {
			url = models.BuildReplayURL(req.Cluster, req.MatchID, req.ReplaySalt)
		}
	} else if strings.HasPrefix(url, "/") {
		url = c.BaseURL() + url
	}

	job := models.MatchJob{
		MatchID:         req.MatchID,
		Cluster:         req.Cluster,
		ReplaySalt:      req.ReplaySalt,
		Region:          req.Region,
		URL:             url,
		DurationSeconds: req.DurationSeconds,
		LobbyType:       7,
		GameMode:        22,
		CreatedAt:       time.Now().UTC(),
		ReadyAt:         time.Now().UTC(),
	}

	var err error
	if req.BypassDelay {
		err = s.leaseMgr.EnqueueReady(ctx, job)
	} else {
		job.ReadyAt = time.Now().UTC().Add(3 * time.Minute)
		err = s.leaseMgr.EnqueueDelayed(ctx, job)
	}
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"status": "enqueued", "job": job})
}

func (s *Server) handleInjectDemoBatch(c *fiber.Ctx) error {
	ctx := c.UserContext()
	baseURL := c.BaseURL()
	regions := []models.MacroRegion{models.RegionEU, models.RegionEU, models.RegionNA, models.RegionSEA, models.RegionEU}

	for i, reg := range regions {
		matchID := uint64(7900100000 + time.Now().Unix()%100000 + int64(i))
		job := models.MatchJob{
			MatchID:         matchID,
			Cluster:         133,
			ReplaySalt:      uint64(1386881000 + i),
			Region:          reg,
			URL:             fmt.Sprintf("%s/api/v1/demo/sample.dem.bz2?match_id=%d", baseURL, matchID),
			DurationSeconds: 1980 + i*120,
			LobbyType:       7,
			GameMode:        22,
			CreatedAt:       time.Now().UTC(),
			ReadyAt:         time.Now().UTC(),
		}
		if err := s.leaseMgr.EnqueueReady(ctx, job); err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
	}

	return c.JSON(fiber.Map{"status": "ok", "injected": len(regions)})
}

func (s *Server) handlePollNow(c *fiber.Ctx) error {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = s.harvester.PollOnce(ctx)
	}()
	return c.JSON(fiber.Map{"status": "triggered"})
}

func (s *Server) checkOptionalAdminSecret(c *fiber.Ctx) error {
	secret := strings.TrimSpace(os.Getenv("ADMIN_SECRET"))
	if secret == "" {
		return nil
	}
	if extractBearerToken(c) != secret && strings.TrimSpace(c.Get("X-Admin-Secret")) != secret {
		return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "admin authorization required"})
	}
	return nil
}

func (s *Server) handleGetActiveParser(c *fiber.Ctx) error {
	if s.parsers == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "parser manager unavailable"})
	}
	return c.JSON(s.parsers.GetActive())
}

func (s *Server) handleGetParserArtifact(c *fiber.Ctx) error {
	if s.parsers == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "parser manager unavailable"})
	}
	sha := c.Params("sha256")
	raw, desc, err := s.parsers.GetArtifact(sha)
	if err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "parser artifact not found"})
	}
	c.Set("Content-Type", "application/octet-stream")
	c.Set("X-Parser-SHA256", sha)
	if desc != nil {
		c.Set("X-Parser-Kind", desc.Kind)
		c.Set("X-Parser-Name", desc.ReportedName)
	}
	return c.Send(raw)
}

func (s *Server) handleAdminListParsers(c *fiber.Ctx) error {
	if s.parsers == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "parser manager unavailable"})
	}
	active, custom := s.parsers.ListAll()
	return c.JSON(fiber.Map{
		"active_parser":  active,
		"custom_parsers": custom,
	})
}

func (s *Server) handleAdminUploadParser(c *fiber.Ctx) error {
	if err := s.checkOptionalAdminSecret(c); err != nil {
		return err
	}
	if s.parsers == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "parser manager unavailable"})
	}

	var name, filename string
	var sourceBytes []byte
	activate := true

	contentType := strings.ToLower(c.Get("Content-Type"))
	if strings.Contains(contentType, "multipart/form-data") {
		name = strings.TrimSpace(c.FormValue("name"))
		if actVal := strings.TrimSpace(c.FormValue("activate")); actVal == "false" || actVal == "0" {
			activate = false
		}
		fh, err := c.FormFile("file")
		if err != nil || fh == nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "multipart 'file' is required"})
		}
		filename = fh.Filename
		f, err := fh.Open()
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "failed to read uploaded file"})
		}
		sourceBytes, err = io.ReadAll(io.LimitReader(f, 40*1024*1024))
		_ = f.Close()
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "failed reading uploaded bytes"})
		}
	} else {
		var body struct {
			Name          string `json:"name"`
			Filename      string `json:"filename"`
			Content       string `json:"content"`
			ContentBase64 string `json:"content_base64"`
			Activate      *bool  `json:"activate"`
		}
		if err := json.Unmarshal(c.Body(), &body); err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid upload payload: " + err.Error()})
		}
		name = body.Name
		filename = body.Filename
		if body.Activate != nil {
			activate = *body.Activate
		}
		if body.ContentBase64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(body.ContentBase64)
			if err != nil {
				return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid content_base64"})
			}
			sourceBytes = decoded
		} else {
			sourceBytes = []byte(body.Content)
		}
	}

	desc, err := s.parsers.UploadCustom(c.UserContext(), name, filename, sourceBytes, activate)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	return c.Status(http.StatusCreated).JSON(desc)
}

func (s *Server) handleAdminActivateParser(c *fiber.Ctx) error {
	if err := s.checkOptionalAdminSecret(c); err != nil {
		return err
	}
	if s.parsers == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "parser manager unavailable"})
	}
	var body struct {
		Mode string `json:"mode"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}
	mode := strings.ToLower(strings.TrimSpace(body.Mode))
	if mode == "default" || body.ID == "default" {
		desc, err := s.parsers.ActivateDefault()
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(desc)
	}
	desc, err := s.parsers.ActivateCustom(body.ID)
	if err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(desc)
}

func (s *Server) handleAdminDeleteParser(c *fiber.Ctx) error {
	if err := s.checkOptionalAdminSecret(c); err != nil {
		return err
	}
	if s.parsers == nil {
		return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{"error": "parser manager unavailable"})
	}
	id := c.Params("id")
	active, err := s.parsers.DeleteCustom(id)
	if err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "deleted", "active_parser": active})
}
