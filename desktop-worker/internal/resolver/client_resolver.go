package resolver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	OpenDotaDailyFreeLimit = 3000
	unblockProbeInterval   = 10 * time.Minute
)

var (
	ErrNoSaltForMatch     = errors.New("match has no replay_salt in GC/OpenDota")
	ErrLocalQuotaReached  = errors.New("worker local salt resolution quota reached")
)

// ResolvedReplay holds the resolved cluster, replay_salt, and canonical Valve CDN URL.
type ResolvedReplay struct {
	MatchID    uint64 `json:"match_id"`
	Cluster    uint32 `json:"cluster"`
	ReplaySalt uint64 `json:"replay_salt"`
	URL        string `json:"url"`
	Provider   string `json:"provider"` // "client-stratz", "client-opendota", "coordinator-fallback"
}

// QuotaStatus exposes live salt resolution telemetry for the Desktop Worker UI.
type QuotaStatus struct {
	Mode                 string `json:"mode"` // "CLIENT_OPENDOTA", "CLIENT_STRATZ", "SERVER_ASSISTED"
	OpenDotaRemainingDay int    `json:"opendota_remaining_day"`
	OpenDotaRemainingMin int    `json:"opendota_remaining_min"`
	StratzConfigured     bool   `json:"stratz_configured"`
	OpenDotaConfigured   bool   `json:"opendota_configured"`
	LocalQuotaExhausted  bool   `json:"local_quota_exhausted"`
	NextProbeSeconds     int    `json:"next_probe_seconds"`
}

// ClientResolver resolves replay_salt on the Desktop Worker using the user's optional STRATZ token
// and free residential OpenDota quota (60/min, 3000/day), automatically falling back to the Coordinator
// when rate-limited and probing 1 request periodically to detect when unblocked.
type ClientResolver struct {
	httpClient *http.Client

	mu                   sync.Mutex
	openDotaRemainingDay int
	openDotaRemainingMin int
	openDotaUsedToday    int
	quotaResetDate       string
	localQuotaExhausted  bool
	nextUnblockProbeAt   time.Time
	minuteCooldownUntil  time.Time
	stratzCooldownUntil  time.Time
}

// NewClientResolver creates a new worker-side salt resolver.
func NewClientResolver() *ClientResolver {
	return &ClientResolver{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		openDotaRemainingDay: OpenDotaDailyFreeLimit,
		openDotaRemainingMin: 60,
		quotaResetDate:       time.Now().UTC().Format("2006-01-02"),
	}
}

// BuildReplayURL constructs the canonical Valve CDN URL for a match.
func BuildReplayURL(cluster uint32, matchID uint64, replaySalt uint64) string {
	if cluster == 0 {
		cluster = 133
	}
	return fmt.Sprintf("http://replay%d.valve.net/570/%d_%d.dem.bz2", cluster, matchID, replaySalt)
}

// ResetQuotaState clears local exhaustion flags (e.g. when the user enters a new STRATZ API key).
func (r *ClientResolver) ResetQuotaState() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.localQuotaExhausted = false
	r.stratzCooldownUntil = time.Time{}
	r.minuteCooldownUntil = time.Time{}
}

// NeedsServerSalt returns true if the worker's local daily quota is currently exhausted
// and it is not yet time for the periodic 1-request unblock probe.
func (r *ClientResolver) NeedsServerSalt(stratzToken string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkDailyResetLocked()

	now := time.Now()
	// If user configured a personal STRATZ token and it is not on cooldown, worker can resolve locally
	if strings.TrimSpace(stratzToken) != "" && now.After(r.stratzCooldownUntil) {
		return false
	}

	if !r.localQuotaExhausted {
		return false
	}

	// Time to try 1 smart probe request locally to see if the rate limit has been unblocked
	if now.After(r.nextUnblockProbeAt) {
		return false
	}

	return true
}

// GetQuotaStatus returns a snapshot of the worker's salt resolution quota and mode.
func (r *ClientResolver) GetQuotaStatus(stratzToken, openDotaKey string) QuotaStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkDailyResetLocked()

	hasStratz := strings.TrimSpace(stratzToken) != ""
	hasOpenDota := strings.TrimSpace(openDotaKey) != ""
	now := time.Now()

	mode := "CLIENT_OPENDOTA"
	if hasStratz && now.After(r.stratzCooldownUntil) {
		mode = "CLIENT_STRATZ"
	} else if r.localQuotaExhausted {
		mode = "SERVER_ASSISTED"
	}

	probeSec := 0
	if r.localQuotaExhausted && r.nextUnblockProbeAt.After(now) {
		probeSec = int(r.nextUnblockProbeAt.Sub(now).Seconds())
	}

	return QuotaStatus{
		Mode:                 mode,
		OpenDotaRemainingDay: r.openDotaRemainingDay,
		OpenDotaRemainingMin: r.openDotaRemainingMin,
		StratzConfigured:     hasStratz,
		OpenDotaConfigured:   hasOpenDota,
		LocalQuotaExhausted:  r.localQuotaExhausted,
		NextProbeSeconds:     probeSec,
	}
}

// ResolveForTask resolves the replay URL for a leased task:
// 1. If the Coordinator already supplied the URL or replay_salt, uses it directly.
// 2. Otherwise prioritizes the worker's STRATZ key (if configured), then OpenDota (using API key if configured or free quota).
// 3. If the worker hits its rate limit (or is in Server-Assisted mode), falls back to the Coordinator's /api/v1/salt/:match_id.
func (r *ClientResolver) ResolveForTask(
	ctx context.Context,
	matchID uint64,
	clusterHint uint32,
	existingSalt uint64,
	existingURL string,
	stratzToken string,
	openDotaKey string,
	coordinatorBaseURL string,
) (*ResolvedReplay, bool, error) {
	if existingURL != "" {
		return &ResolvedReplay{
			MatchID:    matchID,
			Cluster:    clusterHint,
			ReplaySalt: existingSalt,
			URL:        existingURL,
			Provider:   "coordinator-pre-resolved",
		}, false, nil
	}
	if existingSalt > 0 {
		return &ResolvedReplay{
			MatchID:    matchID,
			Cluster:    clusterHint,
			ReplaySalt: existingSalt,
			URL:        BuildReplayURL(clusterHint, matchID, existingSalt),
			Provider:   "coordinator-pre-resolved",
		}, false, nil
	}

	stratzToken = strings.TrimSpace(stratzToken)
	openDotaKey = strings.TrimSpace(openDotaKey)
	now := time.Now()

	// 1. Try worker's personal STRATZ API Token first if configured
	r.mu.Lock()
	canTryStratz := stratzToken != "" && now.After(r.stratzCooldownUntil)
	r.mu.Unlock()

	if canTryStratz {
		res, rateLimited, err := r.resolveViaStratz(ctx, matchID, clusterHint, stratzToken)
		if err == nil && res != nil {
			return res, false, nil
		}
		if rateLimited {
			r.mu.Lock()
			r.stratzCooldownUntil = time.Now().Add(unblockProbeInterval)
			r.mu.Unlock()
		}
	}

	// 2. Try worker's OpenDota (with API key if configured, or local free quota / periodic probe)
	r.mu.Lock()
	r.checkDailyResetLocked()
	shouldTryOpenDota := !r.localQuotaExhausted || now.After(r.nextUnblockProbeAt)
	waitMinute := time.Duration(0)
	if shouldTryOpenDota && r.minuteCooldownUntil.After(now) {
		waitMinute = r.minuteCooldownUntil.Sub(now)
	}
	r.mu.Unlock()

	justHitDailyLimit := false
	if shouldTryOpenDota {
		// If we hit the 60/min soft cap, wait briefly if under 15s, otherwise use coordinator fallback for this match
		if waitMinute > 0 && waitMinute <= 15*time.Second {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-time.After(waitMinute):
			}
		}

		if waitMinute <= 15*time.Second {
			res, hitDailyLimit, err := r.resolveViaOpenDota(ctx, matchID, clusterHint, openDotaKey)
			if err == nil && res != nil {
				// Probe or normal call succeeded -> ensure localQuotaExhausted is cleared!
				r.mu.Lock()
				r.localQuotaExhausted = false
				r.mu.Unlock()
				return res, false, nil
			}
			if errors.Is(err, ErrNoSaltForMatch) {
				return nil, false, ErrNoSaltForMatch
			}
			if hitDailyLimit {
				r.mu.Lock()
				wasExhausted := r.localQuotaExhausted
				r.localQuotaExhausted = true
				r.nextUnblockProbeAt = time.Now().Add(unblockProbeInterval)
				justHitDailyLimit = !wasExhausted
				r.mu.Unlock()
			}
		}
	}

	// 3. Fallback to Coordinator's reserve quota (GET /api/v1/salt/:match_id)
	res, err := r.resolveViaCoordinatorFallback(ctx, coordinatorBaseURL, matchID, clusterHint)
	if err != nil {
		return nil, justHitDailyLimit, err
	}
	return res, justHitDailyLimit, nil
}

func (r *ClientResolver) resolveViaOpenDota(ctx context.Context, matchID uint64, clusterHint uint32, apiKey string) (*ResolvedReplay, bool, error) {
	endpoint := fmt.Sprintf("https://api.opendota.com/api/matches/%d", matchID)
	if apiKey != "" {
		endpoint += "?api_key=" + url.QueryEscape(apiKey)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("User-Agent", "Dota2ReplayCourier-Worker/1.2")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	// Inspect OpenDota rate-limit headers:
	// X-Rate-Limit-Remaining-Minute (60/min) and X-Rate-Limit-Remaining-Day (or X-Rate-Limit-Remaining-Month)
	remMinStr := resp.Header.Get("X-Rate-Limit-Remaining-Minute")
	remDayStr := resp.Header.Get("X-Rate-Limit-Remaining-Day")

	r.mu.Lock()
	r.openDotaUsedToday++
	if remMin, err := strconv.Atoi(remMinStr); err == nil {
		r.openDotaRemainingMin = remMin
		if remMin <= 1 {
			r.minuteCooldownUntil = time.Now().Add(12 * time.Second)
		}
	}
	if remDay, err := strconv.Atoi(remDayStr); err == nil {
		r.openDotaRemainingDay = remDay
	} else {
		rem := OpenDotaDailyFreeLimit - r.openDotaUsedToday
		if rem < 0 {
			rem = 0
		}
		r.openDotaRemainingDay = rem
	}
	dayRemaining := r.openDotaRemainingDay
	minRemaining := r.openDotaRemainingMin
	r.mu.Unlock()

	if resp.StatusCode == http.StatusTooManyRequests {
		// Distinguish 60/min limit vs 3,000/day limit
		if minRemaining <= 0 && dayRemaining > 10 {
			r.mu.Lock()
			r.minuteCooldownUntil = time.Now().Add(30 * time.Second)
			r.mu.Unlock()
			return nil, false, ErrLocalQuotaReached
		}
		return nil, true, ErrLocalQuotaReached
	}

	if dayRemaining <= 0 {
		return nil, true, ErrLocalQuotaReached
	}

	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("opendota status %d", resp.StatusCode)
	}

	var parsed struct {
		Cluster    uint32 `json:"cluster"`
		ReplaySalt uint64 `json:"replay_salt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, false, err
	}
	if parsed.ReplaySalt == 0 {
		return nil, false, ErrNoSaltForMatch
	}

	cluster := parsed.Cluster
	if cluster == 0 {
		cluster = clusterHint
	}
	return &ResolvedReplay{
		MatchID:    matchID,
		Cluster:    cluster,
		ReplaySalt: parsed.ReplaySalt,
		URL:        BuildReplayURL(cluster, matchID, parsed.ReplaySalt),
		Provider:   "client-opendota",
	}, false, nil
}

func (r *ClientResolver) resolveViaStratz(ctx context.Context, matchID uint64, clusterHint uint32, token string) (*ResolvedReplay, bool, error) {
	query := map[string]any{
		"query":     `query ($id: Long!) { match(id: $id) { id clusterId replaySalt } }`,
		"variables": map[string]any{"id": matchID},
	}
	body, err := json.Marshal(query)
	if err != nil {
		return nil, false, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.stratz.com/graphql", bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "STRATZ_API")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusForbidden {
		return nil, true, ErrLocalQuotaReached
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("stratz status %d", resp.StatusCode)
	}

	var parsed struct {
		Data struct {
			Match struct {
				ClusterID  uint32 `json:"clusterId"`
				ReplaySalt uint64 `json:"replaySalt"`
			} `json:"match"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, false, err
	}
	if parsed.Data.Match.ReplaySalt == 0 {
		return nil, false, ErrNoSaltForMatch
	}

	cluster := parsed.Data.Match.ClusterID
	if cluster == 0 {
		cluster = clusterHint
	}
	return &ResolvedReplay{
		MatchID:    matchID,
		Cluster:    cluster,
		ReplaySalt: parsed.Data.Match.ReplaySalt,
		URL:        BuildReplayURL(cluster, matchID, parsed.Data.Match.ReplaySalt),
		Provider:   "client-stratz",
	}, false, nil
}

func (r *ClientResolver) resolveViaCoordinatorFallback(ctx context.Context, baseURL string, matchID uint64, clusterHint uint32) (*ResolvedReplay, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if base == "" {
		base = "http://127.0.0.1:8080"
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}

	endpoint := fmt.Sprintf("%s/api/v1/salt/%d?cluster=%d", base, matchID, clusterHint)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNoSaltForMatch
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("coordinator salt fallback returned %d", resp.StatusCode)
	}

	var out struct {
		MatchID    uint64 `json:"match_id"`
		Cluster    uint32 `json:"cluster"`
		ReplaySalt uint64 `json:"replay_salt"`
		URL        string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.ReplaySalt == 0 && out.URL == "" {
		return nil, ErrNoSaltForMatch
	}
	if out.URL == "" {
		out.URL = BuildReplayURL(out.Cluster, matchID, out.ReplaySalt)
	}
	return &ResolvedReplay{
		MatchID:    matchID,
		Cluster:    out.Cluster,
		ReplaySalt: out.ReplaySalt,
		URL:        out.URL,
		Provider:   "coordinator-fallback",
	}, nil
}

func (r *ClientResolver) checkDailyResetLocked() {
	today := time.Now().UTC().Format("2006-01-02")
	if r.quotaResetDate != today {
		r.quotaResetDate = today
		r.openDotaUsedToday = 0
		r.openDotaRemainingDay = OpenDotaDailyFreeLimit
		r.localQuotaExhausted = false
	}
}
