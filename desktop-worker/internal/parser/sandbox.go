package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing/fstest"
	"time"

	"github.com/dop251/goja"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const (
	pyCustomSectionName = "dota_py_entrypoint"
	maxSandboxStdout    = 50 * 1024 * 1024
	sandboxExecTimeout  = 120 * time.Second
	wasmMaxMemoryPages  = 8192 // 512 MiB ceiling inside WASM sandbox
)

type limitedBuffer struct {
	buf bytes.Buffer
	max int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	if l.buf.Len()+len(p) > l.max {
		return 0, errors.New("sandbox stdout exceeded maximum output limit")
	}
	return l.buf.Write(p)
}

// SandboxWasmParser runs any custom WASI .wasm parser (compiled from .go/.lua/.wasm, or CPython+custom section for .py)
// inside a zero-filesystem, zero-network wazero WASI sandbox across Windows, Linux, and macOS.
type SandboxWasmParser struct {
	name     string // clean custom name (ReportedName: custom(<name>))
	version  string
	sha256   string
	wasmData []byte
	pyScript []byte
}

// NewSandboxWasmParser constructs a sandboxed WASI runner for an uploaded/cached artifact.
func NewSandboxWasmParser(name, version, sha256Hex string, wasmBytes []byte) (*SandboxWasmParser, error) {
	if len(wasmBytes) < 8 || !bytes.Equal(wasmBytes[:8], []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}) {
		return nil, errors.New("invalid WASM module binary header")
	}
	pyScript, _ := ExtractWasmCustomSection(wasmBytes, pyCustomSectionName)
	return &SandboxWasmParser{
		name:     name,
		version:  version,
		sha256:   sha256Hex,
		wasmData: append([]byte(nil), wasmBytes...),
		pyScript: pyScript,
	}, nil
}

func (p *SandboxWasmParser) Name() string {
	return fmt.Sprintf("custom(%s)", p.name)
}

func (p *SandboxWasmParser) Version() string {
	if p.version != "" {
		return p.version
	}
	return "1.0.0"
}

func (p *SandboxWasmParser) Parse(ctx context.Context, ctrl ExecutionController, input ParseInput) (*ParseOutput, error) {
	if len(input.CompressedBz2) == 0 {
		return nil, fmt.Errorf("empty compressed replay buffer for match %d", input.MatchID)
	}

	// Decompress .dem completely in RAM before entering isolated WASI stdin stream.
	stream := input.NewDecompressedStream()
	demBytes, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		return nil, fmt.Errorf("decompress replay stream: %w", err)
	}
	if ctrl != nil {
		if err := ctrl.CheckGate(ctx); err != nil {
			return nil, err
		}
		ctrl.ReportProgress(35.0)
	}

	execCtx, cancel := context.WithTimeout(ctx, sandboxExecTimeout)
	defer cancel()

	rtCfg := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(wasmMaxMemoryPages).
		WithCloseOnContextDone(true)
	rt := wazero.NewRuntimeWithConfig(execCtx, rtCfg)
	defer rt.Close(context.Background())

	wasi_snapshot_preview1.MustInstantiate(execCtx, rt)

	stdoutBuf := &limitedBuffer{max: maxSandboxStdout}
	var stderrBuf bytes.Buffer

	modCfg := wazero.NewModuleConfig().
		WithName("custom_parser").
		WithStdin(bytes.NewReader(demBytes)).
		WithStdout(stdoutBuf).
		WithStderr(&stderrBuf).
		WithEnv("DOTA_MATCH_ID", fmt.Sprintf("%d", input.MatchID)).
		WithEnv("DOTA_CLUSTER", fmt.Sprintf("%d", input.Cluster)).
		WithEnv("DOTA_REGION", input.Region)

	if len(p.pyScript) > 0 {
		// Python WASI bundle: mount ONLY an in-memory read-only MapFS containing /app/parser.py (zero host disk access).
		memFS := fstest.MapFS{
			"app/parser.py": &fstest.MapFile{
				Data: p.pyScript,
				Mode: 0o444,
			},
		}
		modCfg = modCfg.
			WithFSConfig(wazero.NewFSConfig().WithFSMount(memFS, "/")).
			WithArgs("python", "-B", "/app/parser.py")
	} else {
		modCfg = modCfg.WithArgs("parser")
	}

	compiled, err := rt.CompileModule(execCtx, p.wasmData)
	if err != nil {
		return nil, fmt.Errorf("compile WASM parser: %w", err)
	}
	mod, err := rt.InstantiateModule(execCtx, compiled, modCfg)
	if err != nil {
		errText := strings.TrimSpace(stderrBuf.String())
		if errText != "" {
			return nil, fmt.Errorf("WASM parser execution failed (%v): %s", err, errText)
		}
		return nil, fmt.Errorf("WASM parser execution failed: %w", err)
	}
	_ = mod.Close(context.Background())

	if ctrl != nil {
		ctrl.ReportProgress(100.0)
	}

	rawOut := bytes.TrimSpace(stdoutBuf.buf.Bytes())
	return finalizeCustomParseOutput(p.Name(), p.Version(), p.sha256, input.MatchID, rawOut), nil
}

// SandboxJSParser executes an uploaded JavaScript parser inside a zero-FS, zero-network goja VM.
type SandboxJSParser struct {
	name    string
	version string
	sha256  string
	program *goja.Program
}

// NewSandboxJSParser compiles and prepares a strict JS sandbox runner.
func NewSandboxJSParser(name, version, sha256Hex string, jsBytes []byte) (*SandboxJSParser, error) {
	prog, err := goja.Compile(name+".js", string(jsBytes), true)
	if err != nil {
		return nil, fmt.Errorf("JS syntax error: %w", err)
	}
	return &SandboxJSParser{
		name:    name,
		version: version,
		sha256:  sha256Hex,
		program: prog,
	}, nil
}

func (p *SandboxJSParser) Name() string {
	return fmt.Sprintf("custom(%s)", p.name)
}

func (p *SandboxJSParser) Version() string {
	if p.version != "" {
		return p.version
	}
	return "1.0.0"
}

func (p *SandboxJSParser) Parse(ctx context.Context, ctrl ExecutionController, input ParseInput) (*ParseOutput, error) {
	if len(input.CompressedBz2) == 0 {
		return nil, fmt.Errorf("empty compressed replay buffer for match %d", input.MatchID)
	}

	stream := input.NewDecompressedStream()
	demBytes, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		return nil, fmt.Errorf("decompress replay stream: %w", err)
	}
	if ctrl != nil {
		if err := ctrl.CheckGate(ctx); err != nil {
			return nil, err
		}
		ctrl.ReportProgress(40.0)
	}

	execCtx, cancel := context.WithTimeout(ctx, sandboxExecTimeout)
	defer cancel()

	vm := goja.New()
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		select {
		case <-execCtx.Done():
			vm.Interrupt(execCtx.Err())
		case <-doneCh:
		}
	}()

	var stdoutBuf bytes.Buffer
	_ = vm.Set("DOTA_MATCH_ID", fmt.Sprintf("%d", input.MatchID))
	_ = vm.Set("DOTA_CLUSTER", input.Cluster)
	_ = vm.Set("DOTA_REGION", input.Region)

	_ = vm.Set("stdin_len", func() int { return len(demBytes) })
	_ = vm.Set("stdin_bytes", func() goja.ArrayBuffer {
		cp := append([]byte(nil), demBytes...)
		return vm.NewArrayBuffer(cp)
	})
	_ = vm.Set("stdin_slice", func(start, length int) string {
		if start < 0 {
			start = 0
		}
		if start >= len(demBytes) {
			return ""
		}
		end := start + length
		if length < 0 || end > len(demBytes) {
			end = len(demBytes)
		}
		return string(demBytes[start:end])
	})
	_ = vm.Set("read_stdin", func() string {
		return string(demBytes)
	})

	writeOut := func(call goja.FunctionCall) goja.Value {
		for i, arg := range call.Arguments {
			if i > 0 {
				stdoutBuf.WriteByte(' ')
			}
			stdoutBuf.WriteString(arg.String())
		}
		stdoutBuf.WriteByte('\n')
		return goja.Undefined()
	}
	_ = vm.Set("print", writeOut)
	_ = vm.Set("console", map[string]any{"log": writeOut})

	val, err := vm.RunProgram(p.program)
	if err != nil {
		return nil, fmt.Errorf("JS sandbox runtime error: %w", err)
	}

	rawOut := bytes.TrimSpace(stdoutBuf.Bytes())
	if len(rawOut) == 0 && val != nil && !goja.IsUndefined(val) && !goja.IsNull(val) {
		switch exp := val.Export().(type) {
		case string:
			rawOut = []byte(strings.TrimSpace(exp))
		default:
			if encoded, mErr := json.Marshal(exp); mErr == nil {
				rawOut = encoded
			}
		}
	}

	if ctrl != nil {
		ctrl.ReportProgress(100.0)
	}

	return finalizeCustomParseOutput(p.Name(), p.Version(), p.sha256, input.MatchID, rawOut), nil
}

func finalizeCustomParseOutput(name, version, shaHex string, matchID uint64, rawOut []byte) *ParseOutput {
	if len(rawOut) == 0 {
		rawOut = []byte(`{"status":"ok"}`)
	}
	if !json.Valid(rawOut) {
		wrapped, _ := json.Marshal(map[string]any{
			"match_id":   matchID,
			"raw_output": string(rawOut),
		})
		rawOut = wrapped
	}
	return &ParseOutput{
		ParserName:    name,
		ParserVersion: version,
		ParserSHA256:  shaHex,
		Payload:       json.RawMessage(rawOut),
	}
}

// ExtractWasmCustomSection finds a named WebAssembly Custom Section (ID=0) inside wasmBytes.
func ExtractWasmCustomSection(wasmBytes []byte, targetName string) ([]byte, bool) {
	if len(wasmBytes) < 8 || !bytes.Equal(wasmBytes[:8], []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}) {
		return nil, false
	}
	pos := 8
	for pos < len(wasmBytes) {
		secID := wasmBytes[pos]
		pos++
		secLen, n := readULEB128(wasmBytes[pos:])
		if n <= 0 || pos+n+int(secLen) > len(wasmBytes) {
			return nil, false
		}
		pos += n
		secBody := wasmBytes[pos : pos+int(secLen)]
		pos += int(secLen)

		if secID == 0 {
			nameLen, nn := readULEB128(secBody)
			if nn > 0 && nn+int(nameLen) <= len(secBody) {
				secName := string(secBody[nn : nn+int(nameLen)])
				if secName == targetName {
					payload := append([]byte(nil), secBody[nn+int(nameLen):]...)
					return payload, true
				}
			}
		}
	}
	return nil, false
}

func readULEB128(b []byte) (uint32, int) {
	var result uint32
	var shift uint
	for i := 0; i < len(b) && i < 5; i++ {
		bt := b[i]
		result |= uint32(bt&0x7f) << shift
		if bt&0x80 == 0 {
			return result, i + 1
		}
		shift += 7
	}
	return 0, 0
}
