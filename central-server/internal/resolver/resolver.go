package resolver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// SaltResult holds the resolved cluster and replay salt for a match.
type SaltResult struct {
	MatchID    uint64 `json:"match_id"`
	Cluster    uint32 `json:"cluster"`
	ReplaySalt uint64 `json:"replay_salt"`
	Provider   string `json:"provider"`
}

// SaltResolver resolves replay salts for Dota 2 match IDs.
type SaltResolver interface {
	Resolve(ctx context.Context, matchID uint64, clusterHint uint32) (*SaltResult, error)
}

// MultiResolver chains STRATZ (if token configured, Priority 1) and OpenDota (Priority 2, with optional API key).
type MultiResolver struct {
	httpClient *http.Client

	mu             sync.RWMutex
	stratzToken    string
	openDotaAPIKey string
}

// NewMultiResolver creates a new MultiResolver that prioritizes STRATZ (when configured), then OpenDota.
func NewMultiResolver(stratzToken, openDotaAPIKey string) *MultiResolver {
	return &MultiResolver{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		stratzToken:    strings.TrimSpace(stratzToken),
		openDotaAPIKey: strings.TrimSpace(openDotaAPIKey),
	}
}

// SetKeys updates the STRATZ and OpenDota API keys at runtime.
func (r *MultiResolver) SetKeys(stratzToken, openDotaAPIKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stratzToken = strings.TrimSpace(stratzToken)
	r.openDotaAPIKey = strings.TrimSpace(openDotaAPIKey)
}

// GetKeys returns the currently configured STRATZ and OpenDota API keys.
func (r *MultiResolver) GetKeys() (stratzToken, openDotaAPIKey string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stratzToken, r.openDotaAPIKey
}

// Resolve attempts STRATZ first (if configured), then OpenDota.
// If neither provider returns a valid replay_salt, an error is returned and the match is skipped.
func (r *MultiResolver) Resolve(ctx context.Context, matchID uint64, clusterHint uint32) (*SaltResult, error) {
	stratzToken, openDotaKey := r.GetKeys()

	if stratzToken != "" {
		if res, err := r.resolveViaStratz(ctx, matchID, clusterHint, stratzToken); err == nil && res.ReplaySalt > 0 {
			return res, nil
		}
	}

	if res, err := r.resolveViaOpenDota(ctx, matchID, clusterHint, openDotaKey); err == nil && res.ReplaySalt > 0 {
		return res, nil
	}

	return nil, fmt.Errorf("failed to resolve verified replay_salt for match %d", matchID)
}

func (r *MultiResolver) resolveViaStratz(ctx context.Context, matchID uint64, clusterHint uint32, token string) (*SaltResult, error) {
	query := map[string]any{
		"query": `query ($id: Long!) { match(id: $id) { id clusterId replaySalt } }`,
		"variables": map[string]any{
			"id": matchID,
		},
	}
	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.stratz.com/graphql", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "STRATZ_API")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stratz returned status %d", resp.StatusCode)
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
		return nil, err
	}
	if parsed.Data.Match.ReplaySalt == 0 {
		return nil, fmt.Errorf("stratz had no replaySalt for match %d", matchID)
	}

	cluster := parsed.Data.Match.ClusterID
	if cluster == 0 {
		cluster = clusterHint
	}
	return &SaltResult{
		MatchID:    matchID,
		Cluster:    cluster,
		ReplaySalt: parsed.Data.Match.ReplaySalt,
		Provider:   "stratz",
	}, nil
}

func (r *MultiResolver) resolveViaOpenDota(ctx context.Context, matchID uint64, clusterHint uint32, apiKey string) (*SaltResult, error) {
	endpoint := fmt.Sprintf("https://api.opendota.com/api/matches/%d", matchID)
	if apiKey != "" {
		endpoint += "?api_key=" + url.QueryEscape(apiKey)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Dota2ReplayCourier-CentralServer/1.2")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opendota returned status %d", resp.StatusCode)
	}

	var parsed struct {
		Cluster    uint32 `json:"cluster"`
		ReplaySalt uint64 `json:"replay_salt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	if parsed.ReplaySalt == 0 {
		return nil, fmt.Errorf("opendota had no replay_salt for match %d", matchID)
	}

	cluster := parsed.Cluster
	if cluster == 0 {
		cluster = clusterHint
	}
	return &SaltResult{
		MatchID:    matchID,
		Cluster:    cluster,
		ReplaySalt: parsed.ReplaySalt,
		Provider:   "opendota",
	}, nil
}
