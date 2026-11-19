package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dota2-analytics/desktop-worker/internal/client"
	"github.com/dota2-analytics/desktop-worker/internal/config"
	"github.com/dota2-analytics/desktop-worker/internal/downloader"
	"github.com/dota2-analytics/desktop-worker/internal/netprobe"
	"github.com/dota2-analytics/desktop-worker/internal/parser"
	"github.com/dota2-analytics/desktop-worker/internal/resolver"
	"github.com/dota2-analytics/desktop-worker/internal/sentinel"
)

// XPPerMatch matches the central server's flat per-match contributor XP rate.
const XPPerMatch uint64 = 50

// RawParserSHA256 identifies workers returning the raw downloaded .dem.bz2 replay without parsing.
const RawParserSHA256 = "builtin-raw-replay-v1"

type rawReplayPassthroughParser struct{}

func (r *rawReplayPassthroughParser) Name() string    { return "raw" }
func (r *rawReplayPassthroughParser) Version() string { return "1.0.0" }

func (r *rawReplayPassthroughParser) Parse(_ context.Context, ctrl parser.ExecutionController, input parser.ParseInput) (*parser.ParseOutput, error) {
	sum := sha256.Sum256(input.CompressedBz2)
	shaHex := hex.EncodeToString(sum[:])
	if ctrl != nil {
		ctrl.ReportProgress(100.0)
	}
	payload, err := json.Marshal(map[string]any{
		"mode":                  "raw",
		"match_id":              input.MatchID,
		"cluster":               input.Cluster,
		"region":                input.Region,
		"format":                "dem.bz2",
		"compressed_size_bytes": len(input.CompressedBz2),
		"sha256":                shaHex,
		"data_base64":           base64.StdEncoding.EncodeToString(input.CompressedBz2),
	})
	if err != nil {
		return nil, err
	}
	return &parser.ParseOutput{
		ParserName:    r.Name(),
		ParserVersion: r.Version(),
		ParserSHA256:  RawParserSHA256,
		Payload:       payload,
	}, nil
}

// WorkerState describes the current high-level state of the Desktop Worker.
type WorkerState string

const (
	StateActive            WorkerState = "ACTIVE"
	StateActiveTurboIdle   WorkerState = "ACTIVE_TURBO_IDLE"
	StateReady             WorkerState = "READY"
	StateProbingRegions    WorkerState = "PROBING_REGIONS"
	StateDrainingDownload  WorkerState = "DRAINING_DOWNLOAD_GAME_DETECTED"
	StateGamingHoldInRAM   WorkerState = "PAUSED_HOLDING_IN_RAM"
	StateGamingEcoSlowdown WorkerState = "GAMING_ECO_SLOWDOWN"
	StatePausedGameRunning WorkerState = "PAUSED_GAME_RUNNING"
	StateWaitingForIdle    WorkerState = "WAITING_FOR_PC_IDLE"
	StatePausedByUser      WorkerState = "PAUSED_BY_USER"
	StateQuotaReached      WorkerState = "DAILY_BANDWIDTH_QUOTA_REACHED"
	StateDisconnected      WorkerState = "SERVER_DISCONNECTED"
)

// ActivityEvent represents a single line in the live worker activity log.
type ActivityEvent struct {
	Timestamp string `json:"timestamp"`
	Category  string `json:"category"` // LEASE, RAM, PARSE, XP, GUARD, SYSTEM
	Message   string `json:"message"`
}

// RankTier describes a contributor rank tier.
type RankTier struct {
	Name  string
	Tier  string
	MinXP uint64
}

var contributorRanks = []RankTier{
	{Name: "Herald", Tier: "T1.1", MinXP: 0},
	{Name: "Herald", Tier: "T1.2", MinXP: 50},
	{Name: "Herald", Tier: "T1.3", MinXP: 250},
	{Name: "Herald", Tier: "T1.4", MinXP: 1000},
	{Name: "Herald", Tier: "T1.5", MinXP: 2500},
	{Name: "Guardian", Tier: "T2.1", MinXP: 5000},
	{Name: "Guardian", Tier: "T2.2", MinXP: 9000},
	{Name: "Guardian", Tier: "T2.3", MinXP: 13000},
	{Name: "Guardian", Tier: "T2.4", MinXP: 17000},
	{Name: "Guardian", Tier: "T2.5", MinXP: 21000},
	{Name: "Crusader", Tier: "T3.1", MinXP: 25000},
	{Name: "Crusader", Tier: "T3.2", MinXP: 40000},
	{Name: "Crusader", Tier: "T3.3", MinXP: 55000},
	{Name: "Crusader", Tier: "T3.4", MinXP: 70000},
	{Name: "Crusader", Tier: "T3.5", MinXP: 85000},
	{Name: "Archon", Tier: "T4.1", MinXP: 100000},
	{Name: "Archon", Tier: "T4.2", MinXP: 145000},
	{Name: "Archon", Tier: "T4.3", MinXP: 190000},
	{Name: "Archon", Tier: "T4.4", MinXP: 235000},
	{Name: "Archon", Tier: "T4.5", MinXP: 280000},
	{Name: "Legend", Tier: "T5.1", MinXP: 325000},
	{Name: "Legend", Tier: "T5.2", MinXP: 460000},
	{Name: "Legend", Tier: "T5.3", MinXP: 595000},
	{Name: "Legend", Tier: "T5.4", MinXP: 730000},
	{Name: "Legend", Tier: "T5.5", MinXP: 865000},
	{Name: "Ancient", Tier: "T6.1", MinXP: 1000000},
	{Name: "Ancient", Tier: "T6.2", MinXP: 1400000},
	{Name: "Ancient", Tier: "T6.3", MinXP: 1800000},
	{Name: "Ancient", Tier: "T6.4", MinXP: 2200000},
	{Name: "Ancient", Tier: "T6.5", MinXP: 2600000},
	{Name: "Divine", Tier: "T7.1", MinXP: 3000000},
	{Name: "Divine", Tier: "T7.2", MinXP: 4400000},
	{Name: "Divine", Tier: "T7.3", MinXP: 5800000},
	{Name: "Divine", Tier: "T7.4", MinXP: 7200000},
	{Name: "Divine", Tier: "T7.5", MinXP: 8600000},
	{Name: "Immortal", Tier: "T8.1", MinXP: 10000000},
	{Name: "Immortal", Tier: "T8.2", MinXP: 15000000},
	{Name: "Immortal", Tier: "T8.3", MinXP: 20000000},
	{Name: "Immortal", Tier: "T8.4", MinXP: 25000000},
	{Name: "Immortal", Tier: "T8.5", MinXP: 30000000},
}

// StatusSnapshot is sent to the Desktop UI to render live progress, telemetry, and status.
type StatusSnapshot struct {
	State                   WorkerState              `json:"state"`
	StateLabel              string                   `json:"state_label"`
	Connected               bool                     `json:"connected"`
	ServerURL               string                   `json:"server_url"`
	ServerLatencyMs         int64                    `json:"server_latency_ms"`
	ServerQueuedJobs        int64                    `json:"server_queued_jobs"`
	TargetRegion            string                   `json:"target_region"`
	RegionPriority          []string                 `json:"region_priority"`
	RegionLatencies         []netprobe.RegionLatency `json:"region_latencies"`
	ProbingRegions          bool                     `json:"probing_regions"`
	AccountNickname         string                   `json:"account_nickname"`
	AccountFaction          string                   `json:"account_faction"`
	Authenticated           bool                     `json:"authenticated"`
	PromptedAuth            bool                     `json:"prompted_auth"`
	Factions                *client.FactionStandings `json:"factions,omitempty"`
	CurrentBatchID          string                   `json:"current_batch_id"`
	CurrentMatchID          uint64                   `json:"current_match_id"`
	CurrentCluster          uint32                   `json:"current_cluster"`
	CurrentStage            string                   `json:"current_stage"` // IDLE, DOWNLOADING, PARSING, HOLDING_RAM
	ProgressPercent         float64                  `json:"progress_percent"`
	TicksPerSecond          int                      `json:"ticks_per_second"`
	DownloadedBytes         int64                    `json:"downloaded_bytes"`
	TotalDownloadBytes      int64                    `json:"total_download_bytes"`
	RAMBufferBytes          int64                    `json:"ram_buffer_bytes"`
	YourParsedToday         uint64                   `json:"your_parsed_today"`
	TotalParsedAllTime      uint64                   `json:"total_parsed_all_time"`
	WardsExtractedTotal     uint64                   `json:"wards_extracted_total"`
	ContributorXP           uint64                   `json:"contributor_xp"`
	ContributorLevel        int                      `json:"contributor_level"`
	ContributorRank         string                   `json:"contributor_rank"`
	ContributorRankIcon     string                   `json:"contributor_rank_icon"`
	NextRankName            string                   `json:"next_rank_name"`
	NextRankXP              uint64                   `json:"next_rank_xp"`
	RankProgressPercent     float64                  `json:"rank_progress_percent"`
	StreakDays              int                      `json:"streak_days"`
	SessionUptimeSeconds    int                      `json:"session_uptime_seconds"`
	GlobalParsedTotal       uint64                   `json:"global_parsed_total"`
	GlobalParsedToday       uint64                   `json:"global_parsed_today"`
	ActiveWorkersGlobal     int                      `json:"active_workers_global"`
	UsedBandwidthTodayBytes uint64                   `json:"used_bandwidth_today_bytes"`
	MaxDailyBandwidthMB     int                      `json:"max_daily_bandwidth_mb"`
	UserPaused              bool                     `json:"user_paused"`
	GameRunning             bool                     `json:"game_running"`
	DetectedGame            string                   `json:"detected_game"`
	UserIdleSeconds         int                      `json:"user_idle_seconds"`
	IdleThresholdSeconds    int                      `json:"idle_threshold_seconds"`
	ActiveAlertBanner       string                   `json:"active_alert_banner"`
	LastError               string                   `json:"last_error"`
	ActiveParserName        string                   `json:"active_parser_name"`
	ActiveParser            *client.ParserDescriptor `json:"active_parser,omitempty"`
	DownloadMBps            float64                  `json:"download_mbps"`
	MatchMetrics            []MatchMetricPoint       `json:"match_metrics"`
	ParsedTimeline          []ParsedTimePoint        `json:"parsed_timeline"`
	SaltQuota               resolver.QuotaStatus     `json:"salt_quota"`
	ActivityFeed            []ActivityEvent          `json:"activity_feed"`
	Config                  config.WorkerConfig      `json:"config"`
}

// MatchMetricPoint records per-match parser throughput and download speed for live charts.
type MatchMetricPoint struct {
	Index          int     `json:"index"`
	MatchID        uint64  `json:"match_id"`
	TicksPerSecond int     `json:"ticks_per_second"`
	DownloadMBps   float64 `json:"download_mbps"`
}

// ParsedTimePoint records matches parsed per minute bucket for the time-series chart.
type ParsedTimePoint struct {
	Label string `json:"label"`
	Count uint64 `json:"count"`
}

// RegionProberFunc measures and ranks latency across Valve regional relays.
type RegionProberFunc func(ctx context.Context) ([]netprobe.RegionLatency, []string)

// Orchestrator manages the worker lifecycle: Lease -> Resolve Salt -> Download to RAM -> Black-Box Parse in RAM -> Submit.
type Orchestrator struct {
	cfgMgr        *config.Manager
	sentinel      *sentinel.Sentinel
	apiClient     *client.APIClient
	downloader    *downloader.Downloader
	saltResolver  *resolver.ClientResolver
	regionProber  RegionProberFunc
	regionProbeMu sync.Mutex
	startedAt     time.Time

	mu                  sync.RWMutex
	parserMu            sync.Mutex
	customParser        parser.BlackBoxParser
	serverCustomParser  parser.BlackBoxParser
	serverParserDesc    *client.ParserDescriptor
	parserGen           int64
	defaultManta        parser.BlackBoxParser
	userPaused          bool
	connected           bool
	batchCancel         context.CancelFunc
	probingRegions      bool
	lastXPSync          time.Time
	serverLatencyMs     int64
	serverQueuedJobs    int64
	currentBatchID      string
	currentMatchID      uint64
	currentCluster      uint32
	currentStage        string
	progressPct         float64
	ticksPerSec         int
	downloadMBps        float64
	downloadedBytes     int64
	totalDLBytes        int64
	ramBufferBytes      int64
	globalParsedTotal   uint64
	globalParsedToday   uint64
	activeWorkersGlobal int
	factionStandings    *client.FactionStandings
	matchCounter        int
	matchMetrics        []MatchMetricPoint
	parsedTimeline      []ParsedTimePoint
	activeAlertBanner   string
	lastError           string
	activityFeed        []ActivityEvent
}

// NewOrchestrator creates a new desktop worker Orchestrator.
func NewOrchestrator(
	cfgMgr *config.Manager,
	sent *sentinel.Sentinel,
	apiClient *client.APIClient,
	dl *downloader.Downloader,
) *Orchestrator {
	o := &Orchestrator{
		cfgMgr:       cfgMgr,
		sentinel:     sent,
		apiClient:    apiClient,
		downloader:   dl,
		saltResolver: resolver.NewClientResolver(),
		regionProber: netprobe.ProbeAllRegions,
		startedAt:    time.Now(),
		defaultManta: parser.NewBoilerplateMantaParser(),
		serverParserDesc: &client.ParserDescriptor{
			Mode:         "default",
			ID:           "default",
			Name:         "default",
			ReportedName: "default",
			Kind:         "default",
			SHA256:       parser.DefaultParserSHA256,
		},
		currentStage:   "IDLE",
		activityFeed:   make([]ActivityEvent, 0, 40),
		matchMetrics:   make([]MatchMetricPoint, 0, 32),
		parsedTimeline: []ParsedTimePoint{{Label: time.Now().Format("15:04"), Count: 0}},
	}

	o.appendLog("SYSTEM", "In-memory replay pipeline initialized")
	o.appendLog("GUARD", "Gamer Guard active")
	sent.OnChange(o.onSentinelChange)
	return o
}

// SetRegionProber overrides the regional latency prober (useful for offline unit tests).
func (o *Orchestrator) SetRegionProber(fn RegionProberFunc) {
	if fn == nil {
		return
	}
	o.regionProbeMu.Lock()
	o.regionProber = fn
	o.regionProbeMu.Unlock()
}

// ResetSaltQuota resets local salt quota exhaustion state (e.g. when a new STRATZ key is saved).
func (o *Orchestrator) ResetSaltQuota() {
	o.saltResolver.ResetQuotaState()
}

func (o *Orchestrator) appendLog(category, msg string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.appendLogLocked(category, msg)
}

func (o *Orchestrator) appendLogLocked(category, msg string) {
	entry := ActivityEvent{
		Timestamp: time.Now().Format("15:04:05"),
		Category:  category,
		Message:   msg,
	}
	o.activityFeed = append([]ActivityEvent{entry}, o.activityFeed...)
	if len(o.activityFeed) > 35 {
		o.activityFeed = o.activityFeed[:35]
	}
}

// SetCustomBlackBoxParser allows attaching any custom Go BlackBoxParser implementation directly.
func (o *Orchestrator) SetCustomBlackBoxParser(p parser.BlackBoxParser) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.customParser = p
}

func (o *Orchestrator) resolveActiveParser(_ config.WorkerConfig) parser.BlackBoxParser {
	o.mu.RLock()
	manualCustom := o.customParser
	serverCustom := o.serverCustomParser
	o.mu.RUnlock()
	if manualCustom != nil {
		return manualCustom
	}
	if serverCustom != nil {
		return serverCustom
	}
	return o.defaultManta
}

func (o *Orchestrator) getParserGeneration() int64 {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.parserGen
}

func cloneParserDescriptor(d *client.ParserDescriptor) *client.ParserDescriptor {
	if d == nil {
		return nil
	}
	cp := *d
	return &cp
}

func isHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func (o *Orchestrator) loadOrDownloadParserArtifact(ctx context.Context, serverURL string, desc *client.ParserDescriptor) ([]byte, error) {
	shaHex := strings.ToLower(strings.TrimSpace(desc.SHA256))
	ext := ".wasm"
	if strings.EqualFold(desc.Kind, "js") {
		ext = ".js"
	}

	var cachePath string
	if isHexSHA256(shaHex) {
		cachePath = filepath.Join(o.cfgMgr.ParserCacheDir(), shaHex+ext)
		if cached, err := os.ReadFile(cachePath); err == nil && len(cached) > 0 {
			sum := sha256.Sum256(cached)
			if hex.EncodeToString(sum[:]) == shaHex {
				return cached, nil
			}
		}
	}

	dlCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	artifact, err := o.apiClient.DownloadParserArtifact(dlCtx, serverURL, desc.DownloadURL, desc.SHA256)
	if err != nil {
		return nil, err
	}
	if cachePath != "" {
		if err := os.MkdirAll(o.cfgMgr.ParserCacheDir(), 0755); err == nil {
			_ = os.WriteFile(cachePath, artifact, 0644)
		}
	}
	return artifact, nil
}

func (o *Orchestrator) syncActiveParser(ctx context.Context, serverURL string, desc *client.ParserDescriptor) {
	if desc == nil {
		return
	}

	o.parserMu.Lock()
	defer o.parserMu.Unlock()

	mode := strings.ToLower(strings.TrimSpace(desc.Mode))
	if mode == "" {
		mode = "default"
	}

	o.mu.RLock()
	cur := cloneParserDescriptor(o.serverParserDesc)
	hasServerCustom := o.serverCustomParser != nil
	o.mu.RUnlock()

	if mode == "default" || strings.EqualFold(desc.ID, "default") || strings.EqualFold(desc.ReportedName, "default") {
		wasCustom := hasServerCustom || (cur != nil && (strings.EqualFold(cur.Mode, "custom") || strings.EqualFold(cur.Mode, "raw")))
		o.mu.Lock()
		o.serverCustomParser = nil
		o.serverParserDesc = cloneParserDescriptor(desc)
		if wasCustom {
			o.parserGen++
			o.appendLogLocked("PARSE", "Active parser switched to server default")
		}
		o.mu.Unlock()
		return
	}

	if mode == "raw" || strings.EqualFold(desc.ID, "raw") || strings.EqualFold(desc.ReportedName, "raw") {
		wasRaw := hasServerCustom && cur != nil && strings.EqualFold(cur.Mode, "raw")
		o.mu.Lock()
		o.serverCustomParser = &rawReplayPassthroughParser{}
		o.serverParserDesc = cloneParserDescriptor(desc)
		if !wasRaw {
			o.parserGen++
			o.appendLogLocked("PARSE", "Active parser switched to raw replay passthrough (no parsing)")
		}
		o.mu.Unlock()
		return
	}

	if cur != nil &&
		strings.EqualFold(cur.Mode, "custom") &&
		strings.EqualFold(cur.SHA256, desc.SHA256) &&
		cur.ReportedName == desc.ReportedName &&
		cur.Revision == desc.Revision &&
		hasServerCustom {
		return
	}

	artifact, err := o.loadOrDownloadParserArtifact(ctx, serverURL, desc)
	if err != nil {
		o.appendLog("PARSE", fmt.Sprintf("Failed to sync parser %s: %v", desc.ReportedName, err))
		return
	}

	rawName := strings.TrimSpace(desc.Name)
	if rawName == "" {
		rawName = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(desc.ReportedName), "custom("), ")")
	}
	if rawName == "" {
		rawName = strings.TrimSpace(desc.ID)
	}
	reportedName := strings.TrimSpace(desc.ReportedName)
	if reportedName == "" {
		reportedName = fmt.Sprintf("custom(%s)", rawName)
	}
	ver := fmt.Sprintf("r%d", desc.Revision)
	if desc.Revision <= 0 {
		ver = "r1"
	}

	var sandboxed parser.BlackBoxParser
	if strings.EqualFold(desc.Kind, "js") {
		sandboxed, err = parser.NewSandboxJSParser(rawName, ver, desc.SHA256, artifact)
	} else {
		sandboxed, err = parser.NewSandboxWasmParser(rawName, ver, desc.SHA256, artifact)
	}
	if err != nil {
		o.appendLog("PARSE", fmt.Sprintf("Failed to initialize sandbox for %s: %v", reportedName, err))
		return
	}

	o.mu.Lock()
	o.serverCustomParser = sandboxed
	o.serverParserDesc = cloneParserDescriptor(desc)
	o.parserGen++
	o.appendLogLocked("PARSE", fmt.Sprintf("Synced active parser: %s (%s)", reportedName, ver))
	o.mu.Unlock()
}

func (o *Orchestrator) onSentinelChange(prev, curr sentinel.Snapshot) {
	o.mu.Lock()
	stage := o.currentStage
	matchID := o.currentMatchID
	cfg := o.cfgMgr.Get()

	if !prev.GameRunning && curr.GameRunning && cfg.PauseOnGaming {
		if stage == "DOWNLOADING" && matchID > 0 {
			msg := fmt.Sprintf(
				"%s detected. Completing active download (#%d) before pausing network.",
				curr.DetectedGame, matchID,
			)
			o.activeAlertBanner = msg
			o.appendLogLocked("GUARD", msg)
			o.mu.Unlock()
			o.sentinel.NotifyUser("Dota2ReplayCourier", msg)
			return
		} else if stage == "PARSING" && matchID > 0 {
			if cfg.GamingParseBehavior == config.BehaviorEcoSlowdown {
				msg := fmt.Sprintf("%s detected. Parser set to idle priority for match #%d.", curr.DetectedGame, matchID)
				o.activeAlertBanner = msg
				o.appendLogLocked("GUARD", msg)
			} else {
				msg := fmt.Sprintf("%s detected. Match #%d held in RAM until game exits.", curr.DetectedGame, matchID)
				o.activeAlertBanner = msg
				o.appendLogLocked("GUARD", msg)
			}
			o.mu.Unlock()
			return
		} else {
			o.appendLogLocked("GUARD", fmt.Sprintf("%s running. Worker on standby.", curr.DetectedGame))
		}
	}

	if prev.GameRunning && !curr.GameRunning {
		o.activeAlertBanner = ""
		o.appendLogLocked("GUARD", "Game closed. Resuming worker.")
	}
	o.mu.Unlock()
}

// SetUserPaused toggles manual user pause/resume.
func (o *Orchestrator) SetUserPaused(paused bool) {
	o.mu.Lock()
	o.userPaused = paused
	if !paused {
		o.lastError = ""
		o.appendLogLocked("SYSTEM", "Worker resumed")
	} else {
		o.appendLogLocked("SYSTEM", "Worker paused")
	}
	o.mu.Unlock()
}

// ToggleUserPaused flips the manual user pause state and returns the new value.
func (o *Orchestrator) ToggleUserPaused() bool {
	o.mu.Lock()
	o.userPaused = !o.userPaused
	val := o.userPaused
	if val {
		o.appendLogLocked("SYSTEM", "Worker paused")
	} else {
		o.appendLogLocked("SYSTEM", "Worker resumed")
	}
	o.mu.Unlock()
	return val
}

// Start runs the background stats/XP poller, initial regional latency probe (if uncached), and main worker loop.
func (o *Orchestrator) Start(ctx context.Context) {
	go o.statsLoop(ctx)
	go func() {
		if len(o.cfgMgr.Get().RegionPriority) == 0 {
			_ = o.ReprobeRegions(ctx)
		}
		o.workerLoop(ctx)
	}()
}

func (o *Orchestrator) statsLoop(ctx context.Context) {
	o.refreshServerStats(ctx)
	o.syncUserXP(ctx)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.refreshServerStats(ctx)
			o.mu.RLock()
			dueXP := time.Since(o.lastXPSync) >= 30*time.Second
			o.mu.RUnlock()
			if dueXP {
				o.syncUserXP(ctx)
			}
		}
	}
}

// ProbeServerNow synchronously checks connectivity and syncs user XP with the Central Server.
func (o *Orchestrator) ProbeServerNow() {
	o.refreshServerStats(context.Background())
	o.syncUserXP(context.Background())
}

// ProbeRegionsNow synchronously measures regional Valve relay latencies and updates RegionPriority.
func (o *Orchestrator) ProbeRegionsNow() {
	_ = o.ReprobeRegions(context.Background())
}

// ReprobeRegions measures latency to Valve regional relays, sorts them best-to-worst, and persists them.
func (o *Orchestrator) ReprobeRegions(ctx context.Context) error {
	o.regionProbeMu.Lock()
	probeFn := o.regionProber
	defer o.regionProbeMu.Unlock()

	o.mu.Lock()
	o.probingRegions = true
	o.appendLogLocked("SYSTEM", "Probing Valve regional relay latencies...")
	o.mu.Unlock()

	latencies, priority := probeFn(ctx)
	o.cfgMgr.SetRegionProbeResults(latencies, priority)

	summaryParts := make([]string, 0, 3)
	for _, r := range latencies {
		if r.Reachable && len(summaryParts) < 3 {
			summaryParts = append(summaryParts, fmt.Sprintf("%s (%dms)", r.Region, r.LatencyMs))
		}
	}
	summary := "none reachable"
	if len(summaryParts) > 0 {
		summary = strings.Join(summaryParts, " > ")
	}

	o.mu.Lock()
	o.probingRegions = false
	o.appendLogLocked("SYSTEM", "Region priority updated: "+summary)
	o.mu.Unlock()
	return nil
}

// RegisterAccount creates a new contributor account and binds this worker session to it.
func (o *Orchestrator) RegisterAccount(ctx context.Context, nickname, password, faction string) error {
	cfg := o.cfgMgr.Get()
	if strings.TrimSpace(cfg.ServerURL) == "" {
		return errors.New("configure and connect to a server first")
	}
	authResp, err := o.apiClient.RegisterAccount(ctx, cfg.ServerURL, nickname, password, faction)
	if err != nil {
		return err
	}
	o.cfgMgr.SetAuth(authResp.Token, authResp.User.Nickname, authResp.User.Faction, authResp.User.XP, authResp.User.MatchesParsed)
	o.mu.Lock()
	o.lastXPSync = time.Now()
	if authResp.User.Faction != "" {
		o.appendLogLocked("SYSTEM", fmt.Sprintf("Registered %s on %s", authResp.User.Nickname, strings.Title(authResp.User.Faction)))
	} else {
		o.appendLogLocked("SYSTEM", fmt.Sprintf("Registered and signed in as %s", authResp.User.Nickname))
	}
	o.mu.Unlock()
	return nil
}

// LoginAccount signs into an existing contributor account and syncs its authoritative server XP.
func (o *Orchestrator) LoginAccount(ctx context.Context, nickname, password string) error {
	cfg := o.cfgMgr.Get()
	if strings.TrimSpace(cfg.ServerURL) == "" {
		return errors.New("configure and connect to a server first")
	}
	authResp, err := o.apiClient.LoginAccount(ctx, cfg.ServerURL, nickname, password)
	if err != nil {
		return err
	}
	o.cfgMgr.SetAuth(authResp.Token, authResp.User.Nickname, authResp.User.Faction, authResp.User.XP, authResp.User.MatchesParsed)
	o.mu.Lock()
	o.lastXPSync = time.Now()
	o.appendLogLocked("SYSTEM", fmt.Sprintf("Signed in as %s (%d XP)", authResp.User.Nickname, authResp.User.XP))
	o.mu.Unlock()
	return nil
}

// LogoutAccount revokes the session on the server (best-effort) and switches to anonymous mode.
func (o *Orchestrator) LogoutAccount(ctx context.Context) {
	cfg := o.cfgMgr.Get()
	if cfg.AuthToken != "" {
		logoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_ = o.apiClient.LogoutAccount(logoutCtx, cfg.ServerURL, cfg.AuthToken)
		cancel()
	}
	o.cfgMgr.ClearAuth()
	o.appendLog("SYSTEM", "Signed out (running anonymously)")
}

// DismissAuthPrompt marks the first-startup account modal as acknowledged so the worker runs anonymously.
func (o *Orchestrator) DismissAuthPrompt() {
	o.cfgMgr.MarkAuthPrompted()
}

func (o *Orchestrator) syncUserXP(ctx context.Context) {
	cfg := o.cfgMgr.Get()
	if cfg.AuthToken == "" {
		return
	}

	syncCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	prof, err := o.apiClient.FetchUserXP(syncCtx, cfg.ServerURL, cfg.AuthToken)
	o.mu.Lock()
	o.lastXPSync = time.Now()
	o.mu.Unlock()

	if errors.Is(err, client.ErrUnauthorized) {
		o.cfgMgr.ClearAuth()
		o.appendLog("SYSTEM", "Session expired — switched to anonymous mode")
		return
	}
	if err != nil || prof == nil {
		return
	}
	o.cfgMgr.SyncServerProfile(prof.XP, prof.MatchesParsed, prof.Nickname, prof.Faction)
}

func (o *Orchestrator) cancelActiveBatchLocked() {
	if o.batchCancel != nil {
		o.batchCancel()
		o.batchCancel = nil
	}
}

func (o *Orchestrator) isConnected() bool {
	o.mu.RLock()
	connected := o.connected
	o.mu.RUnlock()
	return connected && strings.TrimSpace(o.cfgMgr.Get().ServerURL) != ""
}

func (o *Orchestrator) refreshServerStats(ctx context.Context) {
	cfg := o.cfgMgr.Get()
	if strings.TrimSpace(cfg.ServerURL) == "" {
		o.mu.Lock()
		o.connected = false
		o.serverLatencyMs = 0
		o.serverQueuedJobs = 0
		o.lastError = "No server configured"
		o.cancelActiveBatchLocked()
		o.mu.Unlock()
		return
	}
	statsCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	start := time.Now()
	stats, err := o.apiClient.FetchStats(statsCtx, cfg.ServerURL)
	latency := time.Since(start).Milliseconds()
	if latency < 1 {
		latency = 1
	}

	o.mu.Lock()
	if err != nil {
		if o.connected {
			o.appendLogLocked("SYSTEM", fmt.Sprintf("Disconnected from %s", cfg.ServerURL))
		}
		o.connected = false
		o.serverLatencyMs = 0
		o.serverQueuedJobs = 0
		o.lastError = err.Error()
		o.cancelActiveBatchLocked()
		o.mu.Unlock()
		return
	}
	if !o.connected {
		o.appendLogLocked("SYSTEM", fmt.Sprintf("Connected to %s (%d ms)", cfg.ServerURL, latency))
	}
	o.connected = true
	o.lastError = ""
	o.serverLatencyMs = latency
	var totalQ int64
	for _, count := range stats.QueueDepthByReg {
		totalQ += count
	}
	o.serverQueuedJobs = totalQ
	o.globalParsedTotal = stats.GlobalParsedTotal
	o.globalParsedToday = stats.GlobalParsedToday
	o.activeWorkersGlobal = stats.ActiveWorkers
	if stats.Factions != nil {
		cp := *stats.Factions
		o.factionStandings = &cp
	}
	activeDesc := stats.ActiveParser
	o.mu.Unlock()

	if activeDesc != nil {
		o.syncActiveParser(ctx, cfg.ServerURL, activeDesc)
	}
}

func (o *Orchestrator) canStartNewDownload() bool {
	o.mu.RLock()
	paused := o.userPaused
	o.mu.RUnlock()
	if paused {
		return false
	}

	if !o.cfgMgr.HasBandwidthQuota() {
		return false
	}

	cfg := o.cfgMgr.Get()
	snap := o.sentinel.GetSnapshot()

	if cfg.PauseOnGaming && snap.GameRunning {
		return false
	}

	if cfg.IdlePolicy == config.IdlePolicyOnlyWhenIdle && !snap.IsUserIdle {
		return false
	}

	return true
}

func (o *Orchestrator) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !o.canStartNewDownload() {
			time.Sleep(1 * time.Second)
			continue
		}

		cfg := o.cfgMgr.Get()
		if strings.TrimSpace(cfg.ServerURL) == "" {
			o.mu.Lock()
			o.connected = false
			o.lastError = "No server configured"
			o.cancelActiveBatchLocked()
			o.mu.Unlock()
			time.Sleep(2 * time.Second)
			continue
		}
		needsServerSalt := o.saltResolver.NeedsServerSalt(cfg.StratzAPIKey)
		leaseResp, err := o.apiClient.LeaseBatch(ctx, cfg.ServerURL, cfg.WorkerID, cfg.PreferredRegion, cfg.RegionPriority, 5, needsServerSalt)
		if err != nil {
			o.mu.Lock()
			o.connected = false
			o.lastError = err.Error()
			o.cancelActiveBatchLocked()
			o.mu.Unlock()
			time.Sleep(4 * time.Second)
			continue
		}

		o.mu.Lock()
		o.connected = true
		o.lastError = ""
		o.mu.Unlock()

		if leaseResp != nil && leaseResp.ActiveParser != nil {
			o.syncActiveParser(ctx, cfg.ServerURL, leaseResp.ActiveParser)
		}

		if leaseResp == nil || len(leaseResp.Tasks) == 0 {
			time.Sleep(3 * time.Second)
			continue
		}

		shortBatch := leaseResp.BatchID
		if len(shortBatch) > 8 {
			shortBatch = shortBatch[:8]
		}
		o.appendLog("LEASE", fmt.Sprintf("Leased batch %s (%d matches, %s)", shortBatch, len(leaseResp.Tasks), leaseResp.Region))
		o.processBatch(ctx, leaseResp)
	}
}

func (o *Orchestrator) processBatch(ctx context.Context, lease *client.LeaseResponse) {
	startGen := o.getParserGeneration()
	batchCtx, cancelBatch := context.WithCancel(ctx)

	o.mu.Lock()
	o.batchCancel = cancelBatch
	o.currentBatchID = lease.BatchID
	o.mu.Unlock()

	defer func() {
		cancelBatch()
		o.mu.Lock()
		o.batchCancel = nil
		o.currentBatchID = ""
		o.currentMatchID = 0
		o.currentCluster = 0
		o.currentStage = "IDLE"
		o.progressPct = 0
		o.ticksPerSec = 0
		o.downloadedBytes = 0
		o.totalDLBytes = 0
		o.ramBufferBytes = 0
		o.mu.Unlock()
	}()

	shortBatch := lease.BatchID
	if len(shortBatch) > 8 {
		shortBatch = shortBatch[:8]
	}

	for idx, task := range lease.Tasks {
		if batchCtx.Err() != nil || !o.isConnected() {
			o.appendLog("SYSTEM", fmt.Sprintf("Connection lost; stopping batch %s", shortBatch))
			return
		}
		if idx > 0 && o.getParserGeneration() != startGen {
			o.appendLog("PARSE", fmt.Sprintf("Active parser changed during batch %s; aborting remaining tasks", shortBatch))
			return
		}
		for !o.canStartNewDownload() {
			if batchCtx.Err() != nil || !o.isConnected() {
				o.appendLog("SYSTEM", fmt.Sprintf("Connection lost while paused; stopping batch %s", shortBatch))
				return
			}
			if o.getParserGeneration() != startGen {
				o.appendLog("PARSE", fmt.Sprintf("Active parser changed while paused; aborting remaining tasks in batch %s", shortBatch))
				return
			}
			select {
			case <-batchCtx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
		if batchCtx.Err() != nil || !o.isConnected() {
			o.appendLog("SYSTEM", fmt.Sprintf("Connection lost; stopping batch %s", shortBatch))
			return
		}
		if o.getParserGeneration() != startGen {
			o.appendLog("PARSE", fmt.Sprintf("Active parser changed during batch %s; aborting remaining tasks", shortBatch))
			return
		}

		o.processSingleTask(batchCtx, lease.BatchID, task)
	}
}

func (o *Orchestrator) submitTaskResult(ctx context.Context, cfg config.WorkerConfig, batchID string, result client.TaskResult) (*client.SubmitBatchResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	subResp, subErr := o.apiClient.SubmitResults(ctx, cfg.ServerURL, batchID, cfg.WorkerID, cfg.AuthToken, []client.TaskResult{result})
	if subErr != nil && ctx.Err() == nil {
		o.mu.Lock()
		if o.connected {
			o.appendLogLocked("SYSTEM", fmt.Sprintf("Disconnected from %s (%v)", cfg.ServerURL, subErr))
		}
		o.connected = false
		o.serverLatencyMs = 0
		o.serverQueuedJobs = 0
		o.lastError = subErr.Error()
		o.cancelActiveBatchLocked()
		o.mu.Unlock()
	}
	return subResp, subErr
}

func (o *Orchestrator) processSingleTask(ctx context.Context, batchID string, task client.LeasedTask) {
	cfg := o.cfgMgr.Get()

	o.mu.Lock()
	o.currentMatchID = task.MatchID
	o.currentCluster = task.Cluster
	o.currentStage = "RESOLVING_SALT"
	o.progressPct = 0
	o.ticksPerSec = 0
	o.downloadedBytes = 0
	o.totalDLBytes = 0
	o.ramBufferBytes = 0
	o.mu.Unlock()

	// Step 1: Resolve replay_salt using the worker's local STRATZ/OpenDota quota,
	// falling back to the Coordinator if the worker's daily quota is exhausted.
	resolved, justHitLimit, err := o.saltResolver.ResolveForTask(
		ctx,
		task.MatchID,
		task.Cluster,
		task.ReplaySalt,
		task.URL,
		cfg.StratzAPIKey,
		cfg.OpenDotaAPIKey,
		cfg.ServerURL,
	)
	if ctx.Err() != nil {
		return
	}
	if justHitLimit {
		o.appendLog("SYSTEM", "Local daily salt quota reached (3,000/day) — Coordinator is now resolving salts for this worker")
	}
	if err != nil {
		status := "TEMP_404"
		if errors.Is(err, resolver.ErrNoSaltForMatch) {
			status = "NO_SALT"
			o.appendLog("SYSTEM", fmt.Sprintf("Match #%d has no GC salt available (skipped)", task.MatchID))
		} else {
			o.appendLog("SYSTEM", fmt.Sprintf("Match #%d salt resolution skipped (%v)", task.MatchID, err))
		}
		_, _ = o.submitTaskResult(ctx, cfg, batchID, client.TaskResult{
			MatchID:      task.MatchID,
			Status:       status,
			ErrorMessage: err.Error(),
		})
		return
	}

	task.URL = resolved.URL
	if resolved.Cluster > 0 {
		task.Cluster = resolved.Cluster
	}

	o.mu.Lock()
	o.currentCluster = task.Cluster
	o.currentStage = "DOWNLOADING"
	o.mu.Unlock()

	dlStart := time.Now()
	dlRes, err := o.downloader.DownloadToRAM(
		ctx,
		task.URL,
		cfg.DownloadSpeedLimitKBps,
		func(p downloader.ProgressReport) {
			elapsed := time.Since(dlStart).Seconds()
			if elapsed < 0.05 {
				elapsed = 0.05
			}
			mbps := (float64(p.BytesDownloaded) / (1024.0 * 1024.0)) / elapsed
			o.mu.Lock()
			o.downloadedBytes = p.BytesDownloaded
			o.totalDLBytes = p.TotalBytes
			o.progressPct = p.Percent
			o.ramBufferBytes = p.BytesDownloaded
			o.downloadMBps = mbps
			o.mu.Unlock()
		},
		func(n uint64) {
			o.cfgMgr.AddBandwidthBytes(n)
		},
	)

	if ctx.Err() != nil {
		return
	}
	if err != nil {
		status := "CORRUPT_REPLAY"
		if errors.Is(err, downloader.ErrReplayNotFound404) {
			status = "TEMP_404"
		}
		o.appendLog("SYSTEM", fmt.Sprintf("Match #%d skipped (%s)", task.MatchID, status))
		_, _ = o.submitTaskResult(ctx, cfg, batchID, client.TaskResult{
			MatchID:      task.MatchID,
			Status:       status,
			ErrorMessage: err.Error(),
		})
		return
	}

	dlElapsed := math.Max(0.05, time.Since(dlStart).Seconds())
	avgDLMBps := (float64(dlRes.SizeBytes) / (1024.0 * 1024.0)) / dlElapsed

	compressedRAM := dlRes.CompressedBz2
	defer func() {
		compressedRAM = nil
		o.mu.Lock()
		o.ramBufferBytes = 0
		o.ticksPerSec = 0
		o.downloadMBps = 0
		o.mu.Unlock()
	}()

	o.appendLog("RAM", fmt.Sprintf("Match #%d buffered in RAM (%.1f KB)", task.MatchID, float64(dlRes.SizeBytes)/1024.0))

	snap := o.sentinel.GetSnapshot()
	o.mu.Lock()
	if snap.GameRunning && cfg.PauseOnGaming && cfg.GamingParseBehavior == config.BehaviorHoldInRAM {
		o.currentStage = "HOLDING_RAM"
		o.activeAlertBanner = fmt.Sprintf(
			"Match #%d download finished. Held in RAM while %s is running.",
			task.MatchID, snap.DetectedGame,
		)
	} else {
		o.currentStage = "PARSING"
		if !snap.GameRunning {
			o.activeAlertBanner = ""
		}
	}
	o.progressPct = 0
	o.mu.Unlock()

	if err := o.CheckGate(ctx); err != nil {
		return
	}

	o.mu.Lock()
	o.currentStage = "PARSING"
	o.ticksPerSec = 19800
	o.mu.Unlock()

	parseStart := time.Now()
	activeParser := o.resolveActiveParser(o.cfgMgr.Get())
	parseOut, err := activeParser.Parse(ctx, o, parser.ParseInput{
		MatchID:         task.MatchID,
		Cluster:         task.Cluster,
		Region:          task.Region,
		DurationHintSec: task.DurationHintSec,
		CompressedBz2:   compressedRAM,
		Metadata:        task.Metadata,
	})

	if ctx.Err() != nil {
		return
	}
	if err != nil {
		log.Printf("[Orchestrator] Parser error on match %d: %v", task.MatchID, err)
		o.appendLog("PARSE", fmt.Sprintf("Match #%d parse error: %v", task.MatchID, err))
		_, _ = o.submitTaskResult(ctx, cfg, batchID, client.TaskResult{
			MatchID:      task.MatchID,
			Status:       "CORRUPT_REPLAY",
			ErrorMessage: err.Error(),
		})
		return
	}

	parseElapsed := math.Max(0.02, time.Since(parseStart).Seconds())
	estTicks := 60000.0
	if task.DurationHintSec > 0 {
		estTicks = float64(task.DurationHintSec) * 30.0
	}
	actualTPS := int(estTicks / parseElapsed)
	if actualTPS < 1000 {
		actualTPS = 1000
	}
	o.mu.Lock()
	o.ticksPerSec = actualTPS
	o.mu.Unlock()

	subResp, subErr := o.submitTaskResult(ctx, cfg, batchID, client.TaskResult{
		MatchID:       task.MatchID,
		Status:        "SUCCESS",
		ParserName:    parseOut.ParserName,
		ParserVersion: parseOut.ParserVersion,
		ParserSHA256:  parseOut.ParserSHA256,
		Payload:       parseOut.Payload,
	})
	if subErr != nil {
		o.appendLog("SYSTEM", fmt.Sprintf("Match #%d submit failed: %v", task.MatchID, subErr))
		return
	}

	if subResp != nil && subResp.ActiveParser != nil {
		o.syncActiveParser(ctx, cfg.ServerURL, subResp.ActiveParser)
	}
	if subResp != nil && subResp.MatchesDismissed > 0 && subResp.MatchesAccepted == 0 {
		if cfg.AuthToken != "" && subResp.UserXP != nil {
			o.cfgMgr.SyncServerProfile(*subResp.UserXP, 0, cfg.AccountNickname, cfg.AccountFaction)
		}
		o.appendLog("PARSE", fmt.Sprintf("Match #%d dismissed by server (parser %s outdated — 0 XP)", task.MatchID, parseOut.ParserName))
		return
	}

	xpEarned := XPPerMatch
	o.cfgMgr.RecordMatchParsed(xpEarned, 8)
	if cfg.AuthToken != "" && subResp != nil && subResp.UserXP != nil {
		o.cfgMgr.SyncServerProfile(*subResp.UserXP, 0, cfg.AccountNickname, cfg.AccountFaction)
	}
	o.appendLog("PARSE", fmt.Sprintf("Submitted match #%d via %s", task.MatchID, parseOut.ParserName))
	o.appendLog("XP", fmt.Sprintf("+%d XP (match #%d)", xpEarned, task.MatchID))

	o.mu.Lock()
	o.globalParsedTotal++
	o.globalParsedToday++
	o.recordMatchTelemetryLocked(task.MatchID, actualTPS, avgDLMBps)
	o.mu.Unlock()
}

func (o *Orchestrator) recordMatchTelemetryLocked(matchID uint64, tps int, dlMBps float64) {
	o.matchCounter++
	o.matchMetrics = append(o.matchMetrics, MatchMetricPoint{
		Index:          o.matchCounter,
		MatchID:        matchID,
		TicksPerSecond: tps,
		DownloadMBps:   math.Round(dlMBps*100) / 100,
	})
	if len(o.matchMetrics) > 30 {
		o.matchMetrics = o.matchMetrics[len(o.matchMetrics)-30:]
	}

	nowLabel := time.Now().Format("15:04")
	n := len(o.parsedTimeline)
	if n > 0 && o.parsedTimeline[n-1].Label == nowLabel {
		o.parsedTimeline[n-1].Count++
	} else {
		o.parsedTimeline = append(o.parsedTimeline, ParsedTimePoint{
			Label: nowLabel,
			Count: 1,
		})
		if len(o.parsedTimeline) > 20 {
			o.parsedTimeline = o.parsedTimeline[len(o.parsedTimeline)-20:]
		}
	}
}

// CheckGate implements parser.ExecutionController.
func (o *Orchestrator) CheckGate(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		o.mu.RLock()
		userPaused := o.userPaused
		o.mu.RUnlock()

		cfg := o.cfgMgr.Get()
		snap := o.sentinel.GetSnapshot()

		if userPaused {
			o.mu.Lock()
			o.currentStage = "HOLDING_RAM"
			o.ticksPerSec = 0
			o.mu.Unlock()
			time.Sleep(400 * time.Millisecond)
			continue
		}

		if cfg.PauseOnGaming && snap.GameRunning {
			if cfg.GamingParseBehavior == config.BehaviorHoldInRAM {
				o.mu.Lock()
				o.currentStage = "HOLDING_RAM"
				o.ticksPerSec = 0
				o.mu.Unlock()
				time.Sleep(500 * time.Millisecond)
				continue
			}
			o.mu.Lock()
			o.currentStage = "PARSING"
			o.ticksPerSec = 4200
			o.mu.Unlock()
			time.Sleep(25 * time.Millisecond)
			return nil
		}

		o.mu.Lock()
		if o.currentStage == "HOLDING_RAM" {
			o.currentStage = "PARSING"
		}
		if snap.IsUserIdle {
			o.ticksPerSec = 24600
		} else {
			o.ticksPerSec = 18900
		}
		o.mu.Unlock()
		return nil
	}
}

// ReportProgress implements parser.ExecutionController.
func (o *Orchestrator) ReportProgress(percent float64) {
	o.mu.Lock()
	o.progressPct = percent
	o.mu.Unlock()
}

func computeRankProgress(xp uint64) (level int, rankName, tierCode, nextName string, nextXP uint64, pct float64) {
	currIdx := 0
	for i, r := range contributorRanks {
		if xp >= r.MinXP {
			currIdx = i
		}
	}
	level = currIdx + 1
	curr := contributorRanks[currIdx]
	if currIdx+1 < len(contributorRanks) {
		next := contributorRanks[currIdx+1]
		span := next.MinXP - curr.MinXP
		pos := xp - curr.MinXP
		if span > 0 {
			pct = (float64(pos) / float64(span)) * 100.0
		}
		return level, curr.Name, curr.Tier, next.Tier, next.MinXP, pct
	}
	return level, curr.Name, curr.Tier, "Max Rank", curr.MinXP, 100.0
}

// GetStatusSnapshot builds a complete StatusSnapshot for the Desktop UI.
func (o *Orchestrator) GetStatusSnapshot() StatusSnapshot {
	cfg := o.cfgMgr.Get()
	snap := o.sentinel.GetSnapshot()
	activeParser := o.resolveActiveParser(cfg)

	o.mu.RLock()
	defer o.mu.RUnlock()

	state, label := o.computeWorkerStateLocked(cfg, snap)
	lvl, rName, rTier, nextName, nextXP, rPct := computeRankProgress(cfg.ContributorXP)

	feedCopy := make([]ActivityEvent, len(o.activityFeed))
	copy(feedCopy, o.activityFeed)

	metricsCopy := make([]MatchMetricPoint, len(o.matchMetrics))
	copy(metricsCopy, o.matchMetrics)

	timelineCopy := make([]ParsedTimePoint, len(o.parsedTimeline))
	copy(timelineCopy, o.parsedTimeline)

	var factionsCopy *client.FactionStandings
	if o.factionStandings != nil {
		cp := *o.factionStandings
		factionsCopy = &cp
	}

	safeCfg := cfg
	safeCfg.AuthToken = ""

	return StatusSnapshot{
		State:                   state,
		StateLabel:              label,
		Connected:               o.connected,
		ServerURL:               cfg.ServerURL,
		ServerLatencyMs:         o.serverLatencyMs,
		ServerQueuedJobs:        o.serverQueuedJobs,
		TargetRegion:            cfg.PreferredRegion,
		RegionPriority:          append([]string(nil), cfg.RegionPriority...),
		RegionLatencies:         append([]netprobe.RegionLatency(nil), cfg.RegionLatencies...),
		ProbingRegions:          o.probingRegions,
		AccountNickname:         cfg.AccountNickname,
		AccountFaction:          cfg.AccountFaction,
		Authenticated:           cfg.AuthToken != "",
		PromptedAuth:            cfg.PromptedAuth,
		Factions:                factionsCopy,
		CurrentBatchID:          o.currentBatchID,
		CurrentMatchID:          o.currentMatchID,
		CurrentCluster:          o.currentCluster,
		CurrentStage:            o.currentStage,
		ProgressPercent:         o.progressPct,
		TicksPerSecond:          o.ticksPerSec,
		DownloadMBps:            o.downloadMBps,
		MatchMetrics:            metricsCopy,
		ParsedTimeline:          timelineCopy,
		DownloadedBytes:         o.downloadedBytes,
		TotalDownloadBytes:      o.totalDLBytes,
		RAMBufferBytes:          o.ramBufferBytes,
		YourParsedToday:         cfg.ParsedTodayCount,
		TotalParsedAllTime:      cfg.TotalParsedAllTime,
		WardsExtractedTotal:     cfg.WardsExtractedTotal,
		ContributorXP:           cfg.ContributorXP,
		ContributorLevel:        lvl,
		ContributorRank:         rName,
		ContributorRankIcon:     rTier,
		NextRankName:            nextName,
		NextRankXP:              nextXP,
		RankProgressPercent:     rPct,
		StreakDays:              cfg.StreakDays,
		SessionUptimeSeconds:    int(time.Since(o.startedAt).Seconds()),
		GlobalParsedTotal:       o.globalParsedTotal,
		GlobalParsedToday:       o.globalParsedToday,
		ActiveWorkersGlobal:     o.activeWorkersGlobal,
		UsedBandwidthTodayBytes: cfg.UsedBandwidthTodayBytes,
		MaxDailyBandwidthMB:     cfg.MaxDailyBandwidthMB,
		UserPaused:              o.userPaused,
		GameRunning:             snap.GameRunning,
		DetectedGame:            snap.DetectedGame,
		UserIdleSeconds:         snap.UserIdleSeconds,
		IdleThresholdSeconds:    cfg.IdleThresholdSeconds,
		ActiveAlertBanner:       o.activeAlertBanner,
		LastError:               o.lastError,
		ActiveParserName:        fmt.Sprintf("%s (%s)", activeParser.Name(), activeParser.Version()),
		ActiveParser:            cloneParserDescriptor(o.serverParserDesc),
		SaltQuota:               o.saltResolver.GetQuotaStatus(cfg.StratzAPIKey, cfg.OpenDotaAPIKey),
		ActivityFeed:            feedCopy,
		Config:                  safeCfg,
	}
}

func (o *Orchestrator) computeWorkerStateLocked(cfg config.WorkerConfig, snap sentinel.Snapshot) (WorkerState, string) {
	if o.userPaused {
		return StatePausedByUser, "Paused (User)"
	}

	if cfg.PauseOnGaming && snap.GameRunning {
		if o.currentStage == "DOWNLOADING" {
			return StateDrainingDownload, fmt.Sprintf("Draining Download (Gamer Guard · %s)", snap.DetectedGame)
		}
		if o.currentStage == "HOLDING_RAM" {
			return StateGamingHoldInRAM, fmt.Sprintf("Paused (Gamer Guard · %s)", snap.DetectedGame)
		}
		if o.currentStage == "PARSING" && cfg.GamingParseBehavior == config.BehaviorEcoSlowdown {
			return StateGamingEcoSlowdown, fmt.Sprintf("Active · Eco Slowdown (%s)", snap.DetectedGame)
		}
		return StatePausedGameRunning, fmt.Sprintf("Paused (Gamer Guard · %s)", snap.DetectedGame)
	}

	if !o.cfgMgr.HasBandwidthQuota() {
		return StateQuotaReached, "Paused (Daily Bandwidth Limit Reached)"
	}

	if cfg.IdlePolicy == config.IdlePolicyOnlyWhenIdle && !snap.IsUserIdle && o.currentMatchID == 0 {
		return StateWaitingForIdle, fmt.Sprintf("Paused (Waiting for Idle %ds/%ds)", snap.UserIdleSeconds, cfg.IdleThresholdSeconds)
	}

	if o.probingRegions {
		return StateProbingRegions, "Probing Latency"
	}

	if !o.connected {
		return StateDisconnected, "Not Connected"
	}

	if o.currentMatchID > 0 {
		switch o.currentStage {
		case "RESOLVING_SALT":
			return StateActive, fmt.Sprintf("Active · Resolving Salt (#%d)", o.currentMatchID)
		case "DOWNLOADING":
			return StateActive, fmt.Sprintf("Active · Downloading (#%d)", o.currentMatchID)
		case "PARSING":
			if snap.IsUserIdle {
				return StateActiveTurboIdle, "Active · Parsing (Idle Turbo)"
			}
			return StateActive, fmt.Sprintf("Active · Parsing (#%d)", o.currentMatchID)
		default:
			return StateActive, "Active"
		}
	}

	if cfg.IdlePolicy == config.IdlePolicyTurboWhenIdle && snap.IsUserIdle {
		return StateReady, "Ready (Idle Turbo)"
	}

	return StateReady, "Ready"
}
