package parser

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

const (
	// MaxDecompressedBytes caps in-RAM decompressed .dem streams at 650 MB to prevent decompression bomb attacks.
	MaxDecompressedBytes = 650 * 1024 * 1024
)

// ParseInput contains the in-RAM compressed replay buffer and metadata passed to any Black-Box Parser.
type ParseInput struct {
	MatchID         uint64         `json:"match_id"`
	Cluster         uint32         `json:"cluster"`
	Region          string         `json:"region"`
	DurationHintSec int            `json:"duration_hint_sec,omitempty"`
	CompressedBz2   []byte         `json:"-"` // Full compressed replay held in RAM (zero disk writes)
	Metadata        map[string]any `json:"metadata,omitempty"`
}

// TrackedStream wraps an in-memory decompressor (zstd, bzip2, gzip, or raw) and tracks how much of the
// compressed RAM buffer has been consumed so progress percentage (0% -> 100%) is always accurate.
type TrackedStream struct {
	reader          io.Reader
	closer          func()
	compressedTotal int64
	compressedRead  *atomic.Int64
}

func (ts *TrackedStream) Read(p []byte) (int, error) {
	return ts.reader.Read(p)
}

// Close releases any underlying decompressor resources (e.g. zstd decoder goroutines).
func (ts *TrackedStream) Close() {
	if ts.closer != nil {
		ts.closer()
	}
}

// ProgressPercent returns the exact percentage (0..100) of the compressed RAM buffer consumed so far.
func (ts *TrackedStream) ProgressPercent() float64 {
	if ts.compressedTotal <= 0 {
		return 0
	}
	read := ts.compressedRead.Load()
	pct := (float64(read) / float64(ts.compressedTotal)) * 100.0
	if pct > 100.0 {
		pct = 100.0
	}
	return pct
}

type countingReader struct {
	r     io.Reader
	count *atomic.Int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.count.Add(int64(n))
	}
	return n, err
}

// NewDecompressedStream inspects the magic header of in.CompressedBz2 and returns an in-memory
// decompression stream supporting Zstandard (28 B5 2F FD, used by modern Valve CDN .dem.bz2 URLs),
// Bzip2 (42 5A 68 / 'BZh'), Gzip (1F 8B), or uncompressed streams.
func (in *ParseInput) NewDecompressedStream() *TrackedStream {
	var counter atomic.Int64
	cr := &countingReader{
		r:     bytes.NewReader(in.CompressedBz2),
		count: &counter,
	}

	raw := in.CompressedBz2
	var decompressed io.Reader
	var closer func()

	switch {
	case len(raw) >= 4 && raw[0] == 0x28 && raw[1] == 0xB5 && raw[2] == 0x2F && raw[3] == 0xFD:
		// Zstandard (zstd) — modern Valve CDN replay compression
		zr, err := zstd.NewReader(cr, zstd.WithDecoderConcurrency(1))
		if err == nil {
			decompressed = zr
			closer = zr.Close
		} else {
			decompressed = cr
		}
	case len(raw) >= 3 && raw[0] == 'B' && raw[1] == 'Z' && raw[2] == 'h':
		// Bzip2 ('BZh')
		decompressed = bzip2.NewReader(cr)
	case len(raw) >= 2 && raw[0] == 0x1F && raw[1] == 0x8B:
		// Gzip
		gr, err := gzip.NewReader(cr)
		if err == nil {
			decompressed = gr
			closer = func() { _ = gr.Close() }
		} else {
			decompressed = cr
		}
	default:
		decompressed = cr
	}

	limited := io.LimitReader(decompressed, MaxDecompressedBytes)
	return &TrackedStream{
		reader:          limited,
		closer:          closer,
		compressedTotal: int64(len(in.CompressedBz2)),
		compressedRead:  &counter,
	}
}

const DefaultParserSHA256 = "builtin-default-v2"

// ParseOutput holds whatever arbitrary JSON output the Black-Box Parser produces.
type ParseOutput struct {
	ParserName    string          `json:"parser_name"`
	ParserVersion string          `json:"parser_version"`
	ParserSHA256  string          `json:"parser_sha256,omitempty"`
	Payload       json.RawMessage `json:"payload"` // Sent directly to Central Server without schema lock-in
}

// ExecutionController allows Gamer Guard to pause (hold in RAM) or throttle (eco slow down)
// an active parse job mid-flight.
type ExecutionController interface {
	// CheckGate blocks if the worker is paused / holding in RAM, or sleeps briefly if in Eco Slowdown mode.
	CheckGate(ctx context.Context) error
	// ReportProgress updates the worker UI with the current parse percentage (0..100).
	ReportProgress(percent float64)
}

// BlackBoxParser is the pluggable interface for any match replay parser.
type BlackBoxParser interface {
	Name() string
	Version() string
	Parse(ctx context.Context, ctrl ExecutionController, input ParseInput) (*ParseOutput, error)
}
