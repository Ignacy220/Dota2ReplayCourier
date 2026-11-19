package models

import (
	"encoding/json"
	"time"
)

// MacroRegion represents a geographic macro-region for replay routing.
type MacroRegion string

const (
	RegionEU     MacroRegion = "EU"
	RegionNA     MacroRegion = "NA"
	RegionSEA    MacroRegion = "SEA"
	RegionSA     MacroRegion = "SA"
	RegionCN     MacroRegion = "CN"
	RegionOCE    MacroRegion = "OCE"
	RegionGlobal MacroRegion = "GLOBAL"
)

// AllRegions lists all valid regional queues in priority order.
var AllRegions = []MacroRegion{
	RegionEU,
	RegionNA,
	RegionSEA,
	RegionSA,
	RegionCN,
	RegionOCE,
}

// JobStatus represents the outcome of a worker processing a replay job.
type JobStatus string

const (
	StatusSuccess       JobStatus = "SUCCESS"
	StatusTemp404       JobStatus = "TEMP_404"
	StatusExpired404    JobStatus = "EXPIRED_404"
	StatusCorruptReplay JobStatus = "CORRUPT_REPLAY"
	StatusNoSalt        JobStatus = "NO_SALT"
)

// RawMatchCandidate represents a match discovered from Steam's GetMatchHistoryBySequenceNum.
type RawMatchCandidate struct {
	MatchID         uint64    `json:"match_id"`
	MatchSeqNum     uint64    `json:"match_seq_num"`
	Cluster         uint32    `json:"cluster"`
	LobbyType       int       `json:"lobby_type"`
	GameMode        int       `json:"game_mode"`
	HumanPlayers    int       `json:"human_players"`
	DurationSeconds int       `json:"duration_seconds"`
	RadiantWin      bool      `json:"radiant_win"`
	HeroIDs         []int     `json:"hero_ids,omitempty"`
	StartTime       int64     `json:"start_time"`
	DiscoveredAt    time.Time `json:"discovered_at"`
}

// MatchJob represents a resolved, ready-to-lease replay parsing job stored in Redis.
type MatchJob struct {
	MatchID         uint64      `json:"match_id"`
	MatchSeqNum     uint64      `json:"match_seq_num,omitempty"`
	Cluster         uint32      `json:"cluster"`
	ReplaySalt      uint64      `json:"replay_salt,omitempty"`
	Region          MacroRegion `json:"region"`
	URL             string      `json:"url"`
	DurationSeconds int         `json:"duration_seconds,omitempty"`
	RadiantWin      *bool       `json:"radiant_win,omitempty"`
	HeroIDs         []int       `json:"hero_ids,omitempty"`
	LobbyType       int         `json:"lobby_type,omitempty"`
	GameMode        int         `json:"game_mode,omitempty"`
	AttemptCount    int         `json:"attempt_count"`
	CreatedAt       time.Time   `json:"created_at"`
	ReadyAt         time.Time   `json:"ready_at"`
}

// FilterConfig defines the live-editable rules for filtering incoming matches before salt resolution.
type FilterConfig struct {
	Enabled            bool          `json:"enabled"`
	PollIntervalMs     int           `json:"poll_interval_ms"`
	MaxQueueSize       int           `json:"max_queue_size"`
	CDNDelaySeconds    int           `json:"cdn_delay_seconds"`
	AllowedLobbyTypes  []int         `json:"allowed_lobby_types"`
	AllowedGameModes   []int         `json:"allowed_game_modes"`
	MinDurationSeconds int           `json:"min_duration_seconds"`
	MaxDurationSeconds int           `json:"max_duration_seconds"`
	Require10Humans    bool          `json:"require_10_humans"`
	AllowedRegions     []MacroRegion `json:"allowed_regions"`
	UpdatedAt          time.Time     `json:"updated_at"`
}

// DefaultFilterConfig returns the default production match filter configuration.
func DefaultFilterConfig() FilterConfig {
	return FilterConfig{
		Enabled:            false,
		PollIntervalMs:     2000,
		MaxQueueSize:       10000,
		CDNDelaySeconds:    180, // 3 minutes delay for Valve CDN upload completion
		AllowedLobbyTypes:  []int{7}, // 7 = Ranked
		AllowedGameModes:   []int{22, 2}, // 22 = Ranked All Pick, 2 = Captains Mode
		MinDurationSeconds: 900,  // 15 minutes minimum
		MaxDurationSeconds: 7200, // 120 minutes maximum
		Require10Humans:    true,
		AllowedRegions: []MacroRegion{
			RegionEU,
			RegionNA,
			RegionSEA,
			RegionSA,
			RegionOCE,
		},
		UpdatedAt: time.Now().UTC(),
	}
}

// FilterStats tracks how many matches were accepted or rejected by each rule.
type FilterStats struct {
	TotalScanned       uint64 `json:"total_scanned"`
	TotalAccepted      uint64 `json:"total_accepted"`
	DroppedByLobby     uint64 `json:"dropped_by_lobby"`
	DroppedByGameMode  uint64 `json:"dropped_by_game_mode"`
	DroppedByDuration  uint64 `json:"dropped_by_duration"`
	DroppedByHumans    uint64 `json:"dropped_by_humans"`
	DroppedByRegion    uint64 `json:"dropped_by_region"`
	LastMatchSeqNum    uint64 `json:"last_match_seq_num"`
}

// LeasedTask is the lightweight job descriptor sent to a volunteer desktop worker.
type LeasedTask struct {
	MatchID         uint64         `json:"match_id"`
	Cluster         uint32         `json:"cluster"`
	ReplaySalt      uint64         `json:"replay_salt,omitempty"`
	Region          MacroRegion    `json:"region"`
	URL             string         `json:"url,omitempty"`
	DurationHintSec int            `json:"duration_hint_sec,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

// LeaseRequest can be optionally sent by the worker in the POST body of /api/v1/batch/lease.
type LeaseRequest struct {
	WorkerID        string        `json:"worker_id"`
	PreferredRegion MacroRegion   `json:"preferred_region,omitempty"`
	RegionPriority  []MacroRegion `json:"region_priority,omitempty"`
	MaxTasks        int           `json:"max_tasks,omitempty"`
	NeedsServerSalt bool          `json:"needs_server_salt,omitempty"`
	ClientVersion   string        `json:"client_version,omitempty"`
	OS              string        `json:"os,omitempty"`
}

// ParserDescriptor describes the coordinator's active or saved replay parser configuration.
type ParserDescriptor struct {
	Mode         string    `json:"mode"`          // "default" | "custom"
	ID           string    `json:"id"`            // "default" or sanitized custom id
	Name         string    `json:"name"`          // "default" or custom name
	ReportedName string    `json:"reported_name"` // "default" or "custom(<name>)"
	Kind         string    `json:"kind"`          // "default" | "wasm" | "go" | "lua" | "py" | "js"
	Filename     string    `json:"filename,omitempty"`
	Revision     int       `json:"revision"`
	SHA256       string    `json:"sha256"`
	ArtifactSize int64     `json:"artifact_size,omitempty"`
	DownloadURL  string    `json:"download_url,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// LeaseResponse is returned by POST /api/v1/batch/lease.
type LeaseResponse struct {
	BatchID            string            `json:"batch_id"`
	Region             MacroRegion       `json:"region"`
	ExpiresInSeconds   int               `json:"expires_in_seconds"`
	SchemaVersion      int               `json:"schema_version"`
	ServerAssistedSalt bool              `json:"server_assisted_salt"`
	ActiveParser       *ParserDescriptor `json:"active_parser,omitempty"`
	Tasks              []LeasedTask      `json:"tasks"`
}

// TaskResult represents the outcome of a single match inside POST /api/v1/batch/submit.
type TaskResult struct {
	MatchID       uint64          `json:"match_id"`
	Status        JobStatus       `json:"status"`
	ParserName    string          `json:"parser_name,omitempty"`
	ParserVersion string          `json:"parser_version,omitempty"`
	ParserSHA256  string          `json:"parser_sha256,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"` // Arbitrary JSON output from Black-Box Parser
	ErrorMessage  string          `json:"error_message,omitempty"`
}

// SubmitBatchRequest is sent (optionally gzipped) to POST /api/v1/batch/submit.
type SubmitBatchRequest struct {
	BatchID       string       `json:"batch_id"`
	WorkerID      string       `json:"worker_id"`
	ClientVersion string       `json:"client_version"`
	Results       []TaskResult `json:"results"`
}

// SubmitBatchResponse is returned by POST /api/v1/batch/submit.
type SubmitBatchResponse struct {
	Status           string            `json:"status"`
	BatchID          string            `json:"batch_id"`
	MatchesAccepted  int               `json:"matches_accepted"`
	MatchesRetried   int               `json:"matches_retried"`
	MatchesFailed    int               `json:"matches_failed"`
	MatchesDismissed int               `json:"matches_dismissed,omitempty"`
	LeaseCompleted   bool              `json:"lease_completed"`
	XPAwarded        uint64            `json:"xp_awarded,omitempty"`
	UserXP           *uint64           `json:"user_xp,omitempty"`
	ActiveParser     *ParserDescriptor `json:"active_parser,omitempty"`
}

// ActiveLease represents an in-flight lease tracked in Redis.
type ActiveLease struct {
	BatchID       string              `json:"batch_id"`
	WorkerID      string              `json:"worker_id"`
	WorkerIP      string              `json:"worker_ip"`
	Region        MacroRegion         `json:"region"`
	LeasedAt      time.Time           `json:"leased_at"`
	ExpiresAt     time.Time           `json:"expires_at"`
	Jobs          map[uint64]MatchJob `json:"jobs"`
	CompletedIDs  map[uint64]bool     `json:"completed_ids"`
}

// StoredMatchRecord represents a row persisted in ClickHouse / Storage.
type StoredMatchRecord struct {
	MatchID       uint64          `json:"match_id"`
	Cluster       uint32          `json:"cluster"`
	Region        string          `json:"region"`
	SchemaVersion uint16          `json:"schema_version"`
	ParserName    string          `json:"parser_name"`
	ParserVersion string          `json:"parser_version"`
	WorkerID      string          `json:"worker_id"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}

// FactionStanding aggregates one faction's total member XP and member count.
type FactionStanding struct {
	XP      uint64 `json:"xp"`
	Members int64  `json:"members"`
}

// FactionStandings is the Radiant vs Dire war score. Neutral accounts and anonymous workers are excluded.
type FactionStandings struct {
	Radiant FactionStanding `json:"radiant"`
	Dire    FactionStanding `json:"dire"`
}

// PublicStats is returned by GET /api/v1/stats for the desktop worker dashboard.
type PublicStats struct {
	GlobalParsedTotal uint64                 `json:"global_parsed_total"`
	GlobalParsedToday uint64                 `json:"global_parsed_today"`
	ActiveWorkers     int                    `json:"active_workers"`
	ActiveLeases      int                    `json:"active_leases"`
	QueueDepthByReg   map[MacroRegion]int64  `json:"queue_depth_by_region"`
	DelayedQueueCount int64                  `json:"delayed_queue_count"`
	ActiveParser      *ParserDescriptor      `json:"active_parser,omitempty"`
	Factions          *FactionStandings      `json:"factions,omitempty"`
	ServerTime        time.Time              `json:"server_time"`
}

// InjectJobRequest allows an admin to manually enqueue a replay job via the Admin UI.
type InjectJobRequest struct {
	MatchID         uint64      `json:"match_id"`
	Cluster         uint32      `json:"cluster"`
	ReplaySalt      uint64      `json:"replay_salt,omitempty"`
	Region          MacroRegion `json:"region,omitempty"`
	CustomURL       string      `json:"custom_url,omitempty"`
	DurationSeconds int         `json:"duration_seconds,omitempty"`
	BypassDelay     bool        `json:"bypass_delay"`
}
