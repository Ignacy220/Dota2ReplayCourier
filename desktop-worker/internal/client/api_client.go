package client

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"
)

const ClientVersion = "1.2.0"

// ParserDescriptor mirrors the Central Server's configured active replay parser.
type ParserDescriptor struct {
	Mode         string `json:"mode"`          // "default" | "custom"
	ID           string `json:"id"`            // "default" or sanitized custom id
	Name         string `json:"name"`          // "default" or custom name
	ReportedName string `json:"reported_name"` // "default" or "custom(<name>)"
	Kind         string `json:"kind"`          // "default" | "wasm" | "go" | "lua" | "py" | "js"
	Filename     string `json:"filename,omitempty"`
	Revision     int    `json:"revision"`
	SHA256       string `json:"sha256"`
	ArtifactSize int64  `json:"artifact_size,omitempty"`
	DownloadURL  string `json:"download_url,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

// ErrUnauthorized indicates the stored session token is missing, invalid, or expired.
var ErrUnauthorized = errors.New("unauthorized")

// UserProfile represents the authenticated user's server-side profile and XP.
type UserProfile struct {
	Nickname      string         `json:"nickname"`
	Faction       string         `json:"faction"`
	XP            uint64         `json:"xp"`
	MatchesParsed uint64         `json:"matches_parsed"`
	OtherData     map[string]any `json:"other_data,omitempty"`
	CreatedAt     string         `json:"created_at,omitempty"`
	UpdatedAt     string         `json:"updated_at,omitempty"`
}

// FactionStanding aggregates one faction's total XP and member count.
type FactionStanding struct {
	XP      uint64 `json:"xp"`
	Members int64  `json:"members"`
}

// FactionStandings is the Radiant vs Dire faction war scoreboard.
type FactionStandings struct {
	Radiant FactionStanding `json:"radiant"`
	Dire    FactionStanding `json:"dire"`
}

// AuthResponse is returned by POST /api/v1/auth/register and /api/v1/auth/login.
type AuthResponse struct {
	Token string      `json:"token"`
	User  UserProfile `json:"user"`
}

// LeasedTask represents a single replay task leased from the Central Server.
type LeasedTask struct {
	MatchID         uint64         `json:"match_id"`
	Cluster         uint32         `json:"cluster"`
	ReplaySalt      uint64         `json:"replay_salt,omitempty"`
	Region          string         `json:"region"`
	URL             string         `json:"url"`
	DurationHintSec int            `json:"duration_hint_sec,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

// LeaseResponse is returned by POST /api/v1/batch/lease.
type LeaseResponse struct {
	BatchID            string            `json:"batch_id"`
	Region             string            `json:"region"`
	ExpiresInSeconds   int               `json:"expires_in_seconds"`
	SchemaVersion      int               `json:"schema_version"`
	ServerAssistedSalt bool              `json:"server_assisted_salt"`
	ActiveParser       *ParserDescriptor `json:"active_parser,omitempty"`
	Tasks              []LeasedTask      `json:"tasks"`
}

// TaskResult represents the result of processing a single replay task.
type TaskResult struct {
	MatchID       uint64          `json:"match_id"`
	Status        string          `json:"status"`
	ParserName    string          `json:"parser_name,omitempty"`
	ParserVersion string          `json:"parser_version,omitempty"`
	ParserSHA256  string          `json:"parser_sha256,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	ErrorMessage  string          `json:"error_message,omitempty"`
}

// SubmitBatchRequest is sent to POST /api/v1/batch/submit.
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
	XPAwarded        uint64            `json:"xp_awarded"`
	UserXP           *uint64           `json:"user_xp,omitempty"`
	ActiveParser     *ParserDescriptor `json:"active_parser,omitempty"`
}

// PublicStats is returned by GET /api/v1/stats.
type PublicStats struct {
	GlobalParsedTotal uint64            `json:"global_parsed_total"`
	GlobalParsedToday uint64            `json:"global_parsed_today"`
	ActiveWorkers     int               `json:"active_workers"`
	ActiveLeases      int               `json:"active_leases"`
	QueueDepthByReg   map[string]int64  `json:"queue_depth_by_region"`
	ActiveParser      *ParserDescriptor `json:"active_parser,omitempty"`
	Factions          *FactionStandings `json:"factions,omitempty"`
}

// APIClient communicates with the Central Server over HTTP/HTTPS.
type APIClient struct {
	httpClient *http.Client
}

// NewAPIClient creates a new APIClient.
func NewAPIClient() *APIClient {
	return &APIClient{
		httpClient: &http.Client{
			Timeout: 25 * time.Second,
		},
	}
}

func cleanBaseURL(serverURL string) string {
	s := strings.TrimSpace(serverURL)
	if s == "" {
		return "http://127.0.0.1:8080"
	}
	if !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
		s = "http://" + s
	}
	return strings.TrimRight(s, "/")
}

// LeaseBatch requests a batch of replay tasks from the Central Server.
func (c *APIClient) LeaseBatch(
	ctx context.Context,
	serverURL string,
	workerID string,
	preferredRegion string,
	regionPriority []string,
	maxTasks int,
	needsServerSalt bool,
) (*LeaseResponse, error) {
	endpoint := cleanBaseURL(serverURL) + "/api/v1/batch/lease"

	payload := map[string]any{
		"worker_id":         workerID,
		"preferred_region":  preferredRegion,
		"max_tasks":         maxTasks,
		"needs_server_salt": needsServerSalt,
		"client_version":    ClientVersion,
		"os":                runtime.GOOS,
	}
	if len(regionPriority) > 0 {
		payload["region_priority"] = regionPriority
	}
	reqBody, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "DotaParser-Client/"+ClientVersion)
	if preferredRegion != "" && preferredRegion != "AUTO" {
		req.Header.Set("X-Region-Override", preferredRegion)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return &LeaseResponse{Tasks: nil}, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("lease failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out LeaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SubmitResults compresses the batch results with gzip and submits them to the Central Server.
func (c *APIClient) SubmitResults(
	ctx context.Context,
	serverURL string,
	batchID string,
	workerID string,
	authToken string,
	results []TaskResult,
) (*SubmitBatchResponse, error) {
	endpoint := cleanBaseURL(serverURL) + "/api/v1/batch/submit"

	payload := SubmitBatchRequest{
		BatchID:       batchID,
		WorkerID:      workerID,
		ClientVersion: ClientVersion,
		Results:       results,
	}
	rawJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var gzBuf bytes.Buffer
	gz := gzip.NewWriter(&gzBuf)
	if _, err := gz.Write(rawJSON); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &gzBuf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	if tok := strings.TrimSpace(authToken); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("User-Agent", "DotaParser-Client/"+ClientVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("submit failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out SubmitBatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FetchStats retrieves community and queue statistics from the Central Server.
func (c *APIClient) FetchStats(ctx context.Context, serverURL string) (*PublicStats, error) {
	endpoint := cleanBaseURL(serverURL) + "/api/v1/stats"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "DotaParser-Client/"+ClientVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stats returned HTTP %d", resp.StatusCode)
	}

	var stats PublicStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, err
	}
	return &stats, nil
}

// RegisterAccount creates a new contributor account on the Central Server and returns its session token and profile.
func (c *APIClient) RegisterAccount(ctx context.Context, serverURL, nickname, password, faction string) (*AuthResponse, error) {
	body := map[string]string{
		"nickname": nickname,
		"password": password,
	}
	if f := strings.TrimSpace(faction); f != "" {
		body["faction"] = f
	}
	return c.postAuth(ctx, serverURL, "/api/v1/auth/register", body)
}

// LoginAccount authenticates an existing contributor account on the Central Server.
func (c *APIClient) LoginAccount(ctx context.Context, serverURL, nickname, password string) (*AuthResponse, error) {
	return c.postAuth(ctx, serverURL, "/api/v1/auth/login", map[string]string{
		"nickname": nickname,
		"password": password,
	})
}

func (c *APIClient) postAuth(ctx context.Context, serverURL, path string, payload map[string]string) (*AuthResponse, error) {
	endpoint := cleanBaseURL(serverURL) + path
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "DotaParser-Client/"+ClientVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, parseAPIError(resp.StatusCode, raw)
	}

	var out AuthResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// LogoutAccount revokes the active session token on the Central Server.
func (c *APIClient) LogoutAccount(ctx context.Context, serverURL, authToken string) error {
	tok := strings.TrimSpace(authToken)
	if tok == "" {
		return nil
	}
	endpoint := cleanBaseURL(serverURL) + "/api/v1/auth/logout"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("User-Agent", "DotaParser-Client/"+ClientVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		raw, _ := io.ReadAll(resp.Body)
		return parseAPIError(resp.StatusCode, raw)
	}
	return nil
}

// FetchUserXP retrieves the authenticated user's authoritative XP and stats from GET /api/v1/user/xp.
func (c *APIClient) FetchUserXP(ctx context.Context, serverURL, authToken string) (*UserProfile, error) {
	tok := strings.TrimSpace(authToken)
	if tok == "" {
		return nil, ErrUnauthorized
	}
	endpoint := cleanBaseURL(serverURL) + "/api/v1/user/xp"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("User-Agent", "DotaParser-Client/"+ClientVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, ErrUnauthorized
	}
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp.StatusCode, raw)
	}

	var out UserProfile
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func parseAPIError(statusCode int, raw []byte) error {
	var errBody struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &errBody); err == nil && strings.TrimSpace(errBody.Error) != "" {
		return fmt.Errorf("%s", errBody.Error)
	}
	msg := strings.TrimSpace(string(raw))
	if msg == "" {
		return fmt.Errorf("request failed with HTTP %d", statusCode)
	}
	return fmt.Errorf("HTTP %d: %s", statusCode, msg)
}

// FetchActiveParser retrieves the server's currently configured active replay parser descriptor.
func (c *APIClient) FetchActiveParser(ctx context.Context, serverURL string) (*ParserDescriptor, error) {
	endpoint := cleanBaseURL(serverURL) + "/api/v1/parser/active"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "DotaParser-Client/"+ClientVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp.StatusCode, raw)
	}

	var desc ParserDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return nil, err
	}
	return &desc, nil
}

// DownloadParserArtifact downloads a server-hosted sandbox parser binary/script and verifies its SHA-256 hash.
func (c *APIClient) DownloadParserArtifact(ctx context.Context, serverURL, downloadURL, expectedSHA256 string) ([]byte, error) {
	target := strings.TrimSpace(downloadURL)
	if target == "" {
		if expectedSHA256 == "" {
			return nil, fmt.Errorf("missing parser download URL and sha256")
		}
		target = cleanBaseURL(serverURL) + "/api/v1/parser/artifact/" + strings.TrimSpace(expectedSHA256)
	} else if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		if !strings.HasPrefix(target, "/") {
			target = "/" + target
		}
		target = cleanBaseURL(serverURL) + target
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "DotaParser-Client/"+ClientVersion)

	dlClient := &http.Client{Timeout: 120 * time.Second}
	resp, err := dlClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return nil, parseAPIError(resp.StatusCode, raw)
	}

	const maxArtifactBytes = 80 * 1024 * 1024
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxArtifactBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxArtifactBytes {
		return nil, fmt.Errorf("parser artifact exceeds 80 MiB limit")
	}

	if exp := strings.ToLower(strings.TrimSpace(expectedSHA256)); exp != "" {
		sum := sha256.Sum256(data)
		actual := hex.EncodeToString(sum[:])
		if actual != exp {
			return nil, fmt.Errorf("parser artifact sha256 mismatch: expected %s, got %s", exp, actual)
		}
	}
	return data, nil
}
