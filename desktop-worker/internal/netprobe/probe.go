package netprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dota2-analytics/desktop-worker/internal/procutil"
)

const valveSDRConfigURL = "https://api.steampowered.com/ISteamApps/GetSDRConfig/v1?appid=570"

// RegionLatency stores the measured network RTT to a regional Dota 2 relay POP.
type RegionLatency struct {
	Region    string `json:"region"`
	POP       string `json:"pop,omitempty"`
	Host      string `json:"host,omitempty"`
	LatencyMs int64  `json:"latency_ms"`
	Reachable bool   `json:"reachable"`
}

type regionSpec struct {
	Region     string
	POPs       []string
	FallbackIP string
}

var defaultRegionSpecs = []regionSpec{
	{Region: "EU", POPs: []string{"fra", "vie", "sto", "lhr", "ams"}, FallbackIP: "155.133.226.68"},
	{Region: "NA", POPs: []string{"iad", "ord", "sea", "lax"}, FallbackIP: "162.254.192.88"},
	{Region: "SEA", POPs: []string{"sgp"}, FallbackIP: "103.10.124.116"},
	{Region: "SA", POPs: []string{"gru", "lim", "scl"}, FallbackIP: "155.133.227.35"},
	{Region: "CN", POPs: []string{"hkg", "sha", "can"}, FallbackIP: "103.28.54.163"},
	{Region: "OCE", POPs: []string{"syd"}, FallbackIP: "103.10.125.20"},
}

type sdrConfigResponse struct {
	Pops map[string]struct {
		Relays []struct {
			IPv4 string `json:"ipv4"`
		} `json:"relays"`
	} `json:"pops"`
}

type probeTarget struct {
	Region string
	POP    string
	Host   string
}

// HostProbeFunc measures RTT in milliseconds to a target host.
type HostProbeFunc func(ctx context.Context, host string) (int64, bool)

// ProbeAllRegions fetches current Valve SDR relay POP IPs (falling back to static POP IPs),
// pings each canonical macro-region concurrently, and returns results ranked best-to-worst.
func ProbeAllRegions(ctx context.Context) ([]RegionLatency, []string) {
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	targets := resolveTargetsFromSDR(probeCtx)
	return probeTargetsWithFunc(probeCtx, targets, probeHostICMPWithTCPFallback)
}

func probeTargetsWithFunc(ctx context.Context, targets []probeTarget, probeFn HostProbeFunc) ([]RegionLatency, []string) {
	results := make([]RegionLatency, len(targets))
	var wg sync.WaitGroup

	for i, target := range targets {
		wg.Add(1)
		go func(idx int, t probeTarget) {
			defer wg.Done()
			ms, ok := probeFn(ctx, t.Host)
			if !ok || ms <= 0 {
				results[idx] = RegionLatency{
					Region:    t.Region,
					POP:       t.POP,
					Host:      t.Host,
					LatencyMs: 9999,
					Reachable: false,
				}
				return
			}
			results[idx] = RegionLatency{
				Region:    t.Region,
				POP:       t.POP,
				Host:      t.Host,
				LatencyMs: ms,
				Reachable: true,
			}
		}(i, target)
	}

	wg.Wait()
	return RankRegionLatencies(results)
}

// RankRegionLatencies sorts latencies best-to-worst (reachable & lowest ms first)
// and returns the ordered slice alongside the sorted region code priority list.
func RankRegionLatencies(results []RegionLatency) ([]RegionLatency, []string) {
	sorted := make([]RegionLatency, len(results))
	copy(sorted, results)

	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Reachable != sorted[j].Reachable {
			return sorted[i].Reachable
		}
		if sorted[i].LatencyMs != sorted[j].LatencyMs {
			return sorted[i].LatencyMs < sorted[j].LatencyMs
		}
		return sorted[i].Region < sorted[j].Region
	})

	priority := make([]string, 0, len(sorted))
	for _, r := range sorted {
		priority = append(priority, r.Region)
	}
	return sorted, priority
}

func resolveTargetsFromSDR(ctx context.Context) []probeTarget {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, valveSDRConfigURL, nil)
	if err != nil {
		return defaultProbeTargets(nil)
	}

	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return defaultProbeTargets(nil)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return defaultProbeTargets(nil)
	}

	var parsed sdrConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return defaultProbeTargets(nil)
	}
	return defaultProbeTargets(&parsed)
}

func defaultProbeTargets(sdr *sdrConfigResponse) []probeTarget {
	targets := make([]probeTarget, 0, len(defaultRegionSpecs))
	for _, spec := range defaultRegionSpecs {
		chosenPOP := spec.POPs[0]
		chosenIP := spec.FallbackIP
		if sdr != nil {
			for _, pop := range spec.POPs {
				if popData, ok := sdr.Pops[pop]; ok {
					for _, relay := range popData.Relays {
						if ip := strings.TrimSpace(relay.IPv4); ip != "" {
							chosenPOP = pop
							chosenIP = ip
							break
						}
					}
					if chosenIP != spec.FallbackIP || pop == chosenPOP {
						if popData, exists := sdr.Pops[pop]; exists && len(popData.Relays) > 0 && popData.Relays[0].IPv4 != "" {
							break
						}
					}
				}
			}
		}
		targets = append(targets, probeTarget{
			Region: spec.Region,
			POP:    chosenPOP,
			Host:   chosenIP,
		})
	}
	return targets
}

func probeHostICMPWithTCPFallback(ctx context.Context, host string) (int64, bool) {
	if ms, ok := measurePingMs(ctx, host); ok {
		return ms, true
	}
	return measureTCPDialMs(ctx, host)
}

func buildPingCmd(ctx context.Context, host string) *exec.Cmd {
	var args []string
	switch runtime.GOOS {
	case "windows":
		args = []string{"-n", "2", "-w", "1500", host}
	case "darwin":
		args = []string{"-c", "2", "-t", "2", host}
	default:
		args = []string{"-c", "2", "-W", "2", host}
	}

	cmd := exec.CommandContext(ctx, "ping", args...)
	procutil.HideConsoleWindow(cmd)
	return cmd
}

func measurePingMs(ctx context.Context, host string) (int64, bool) {
	pingCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	cmd := buildPingCmd(pingCtx, host)
	out, err := cmd.CombinedOutput()
	ms, parsedOK := parsePingOutputMs(string(out))
	if parsedOK {
		return ms, true
	}
	if err != nil {
		return 0, false
	}
	return 0, false
}

func measureTCPDialMs(ctx context.Context, host string) (int64, bool) {
	ports := []string{"443", "80"}
	for _, port := range ports {
		dialer := net.Dialer{Timeout: 1500 * time.Millisecond}
		start := time.Now()
		conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
		if err == nil {
			_ = conn.Close()
			ms := time.Since(start).Milliseconds()
			if ms < 1 {
				ms = 1
			}
			return ms, true
		}
	}
	return 0, false
}

var (
	// Unix/macOS summary: rtt min/avg/max/mdev = 31.421/32.104/33.012/0.652 ms
	reUnixSummary = regexp.MustCompile(`min/avg/max[^=]*=\s*[\d.]+/([\d.]+)/[\d.]+`)
	// Windows English summary: Average = 32ms
	reWinSummary = regexp.MustCompile(`(?i)average\s*=\s*(\d+)\s*ms`)
	// Windows sub-1ms reply across locales: <1ms / <1 ms
	reSubMsReply = regexp.MustCompile(`<\s*1\s*ms`)
	// Generic per-reply latency token: time=32ms, time=32.4 ms, temps=32 ms, Zeit=32ms, czas=32ms
	rePacketMs = regexp.MustCompile(`[=<]\s*(\d+(?:\.\d+)?)\s*ms\b`)
)

func parsePingOutputMs(output string) (int64, bool) {
	if m := reUnixSummary.FindStringSubmatch(output); len(m) == 2 {
		if f, err := strconv.ParseFloat(m[1], 64); err == nil {
			ms := int64(f + 0.5)
			if ms < 1 {
				ms = 1
			}
			return ms, true
		}
	}

	if m := reWinSummary.FindStringSubmatch(output); len(m) == 2 {
		if v, err := strconv.ParseInt(m[1], 10, 64); err == nil {
			if v < 1 {
				v = 1
			}
			return v, true
		}
	}

	matches := rePacketMs.FindAllStringSubmatch(output, -1)
	if len(matches) > 0 {
		var sum float64
		var count float64
		for _, m := range matches {
			if f, err := strconv.ParseFloat(m[1], 64); err == nil {
				sum += f
				count++
			}
		}
		if count > 0 {
			ms := int64((sum / count) + 0.5)
			if ms < 1 {
				ms = 1
			}
			return ms, true
		}
	}

	if reSubMsReply.MatchString(output) {
		return 1, true
	}

	return 0, false
}

func (r RegionLatency) String() string {
	if !r.Reachable {
		return fmt.Sprintf("%s: unreachable", r.Region)
	}
	return fmt.Sprintf("%s (%s): %dms", r.Region, r.POP, r.LatencyMs)
}
