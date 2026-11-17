package netprobe

import (
	"context"
	"reflect"
	"testing"
)

func TestParsePingOutputMs(t *testing.T) {
	tests := []struct {
		name   string
		output string
		wantMs int64
		wantOK bool
	}{
		{
			name: "windows english",
			output: `Pinging 155.133.226.68 with 32 bytes of data:
Reply from 155.133.226.68: bytes=32 time=31ms TTL=56
Reply from 155.133.226.68: bytes=32 time=33ms TTL=56

Ping statistics for 155.133.226.68:
    Packets: Sent = 2, Received = 2, Lost = 0 (0% loss),
Approximate round trip times in milli-seconds:
    Minimum = 31ms, Maximum = 33ms, Average = 32ms`,
			wantMs: 32,
			wantOK: true,
		},
		{
			name: "windows localized polish",
			output: `Badanie 155.133.226.68 z 32 bajtami danych:
Odpowiedź z 155.133.226.68: bajtów=32 czas=28ms TTL=56
Odpowiedź z 155.133.226.68: bajtów=32 czas=30ms TTL=56`,
			wantMs: 29,
			wantOK: true,
		},
		{
			name: "linux iputils",
			output: `PING 162.254.192.88 (162.254.192.88) 56(84) bytes of data.
64 bytes from 162.254.192.88: icmp_seq=1 ttl=51 time=117 ms
64 bytes from 162.254.192.88: icmp_seq=2 ttl=51 time=119 ms

--- 162.254.192.88 ping statistics ---
2 packets transmitted, 2 received, 0% packet loss, time 1001ms
rtt min/avg/max/mdev = 117.200/118.400/119.600/1.200 ms`,
			wantMs: 118,
			wantOK: true,
		},
		{
			name: "macos bsd ping",
			output: `PING 103.10.124.116 (103.10.124.116): 56 data bytes
64 bytes from 103.10.124.116: icmp_seq=0 ttl=50 time=203.900 ms
round-trip min/avg/max/stddev = 203.900/204.200/204.500/0.300 ms`,
			wantMs: 204,
			wantOK: true,
		},
		{
			name:   "timeout",
			output: `Request timed out.`,
			wantMs: 0,
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotMs, gotOK := parsePingOutputMs(tc.output)
			if gotOK != tc.wantOK || gotMs != tc.wantMs {
				t.Fatalf("parsePingOutputMs() = (%d, %v), want (%d, %v)", gotMs, gotOK, tc.wantMs, tc.wantOK)
			}
		})
	}
}

func TestProbeTargetsBestToWorstRanking(t *testing.T) {
	targets := defaultProbeTargets(&sdrConfigResponse{
		Pops: map[string]struct {
			Relays []struct {
				IPv4 string `json:"ipv4"`
			} `json:"relays"`
		}{
			"fra": {Relays: []struct {
				IPv4 string `json:"ipv4"`
			}{{IPv4: "10.0.0.1"}}},
			"iad": {Relays: []struct {
				IPv4 string `json:"ipv4"`
			}{{IPv4: "10.0.0.2"}}},
			"sgp": {Relays: []struct {
				IPv4 string `json:"ipv4"`
			}{{IPv4: "10.0.0.3"}}},
			"gru": {Relays: []struct {
				IPv4 string `json:"ipv4"`
			}{{IPv4: "10.0.0.4"}}},
			"hkg": {Relays: []struct {
				IPv4 string `json:"ipv4"`
			}{{IPv4: "10.0.0.5"}}},
			"syd": {Relays: []struct {
				IPv4 string `json:"ipv4"`
			}{{IPv4: "10.0.0.6"}}},
		},
	})

	fakeRTT := map[string]struct {
		ms int64
		ok bool
	}{
		"10.0.0.1": {ms: 35, ok: true},  // EU
		"10.0.0.2": {ms: 115, ok: true}, // NA
		"10.0.0.3": {ms: 210, ok: true}, // SEA
		"10.0.0.4": {ms: 0, ok: false},  // SA unreachable
		"10.0.0.5": {ms: 165, ok: true}, // CN
		"10.0.0.6": {ms: 270, ok: true}, // OCE
	}

	results, priority := probeTargetsWithFunc(context.Background(), targets, func(_ context.Context, host string) (int64, bool) {
		r := fakeRTT[host]
		return r.ms, r.ok
	})

	wantPriority := []string{"EU", "NA", "CN", "SEA", "OCE", "SA"}
	if !reflect.DeepEqual(priority, wantPriority) {
		t.Fatalf("priority = %v, want %v", priority, wantPriority)
	}
	if results[0].Region != "EU" || results[0].LatencyMs != 35 || !results[0].Reachable {
		t.Fatalf("unexpected fastest result: %+v", results[0])
	}
	last := results[len(results)-1]
	if last.Region != "SA" || last.Reachable {
		t.Fatalf("expected unreachable SA last, got %+v", last)
	}
}
