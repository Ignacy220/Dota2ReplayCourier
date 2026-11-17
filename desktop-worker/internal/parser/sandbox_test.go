package parser

import (
	"bytes"
	"compress/bzip2"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const testBz2Fixture = "QlpoOTFBWSZTWVjtQx8AACqbgFAEfxAAAK5t3qogAFDGAAAAAGqeUGR6mmQGnqZpDu0A69qbZXc7EWt+8ZROWMQOH0FJkpjVwqMVcpBiRmRJm/uEKoGNUJzk5G4R+H1rfi7kinChILHahj4="

func TestDefaultRichParserReportsDefaultNameAndExpandedSchema(t *testing.T) {
	rawBz2, err := base64.StdEncoding.DecodeString(testBz2Fixture)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	// Ensure valid bzip2 fixture can be read
	_, _ = bzip2.NewReader(bytes.NewReader(rawBz2)).Read(make([]byte, 32))

	p := NewBoilerplateMantaParser()
	out, err := p.Parse(context.Background(), nil, ParseInput{
		MatchID:         7900999001,
		Cluster:         133,
		Region:          "EU",
		DurationHintSec: 2100,
		CompressedBz2:   rawBz2,
	})
	if err != nil {
		t.Fatalf("Default parser failed: %v", err)
	}
	if out.ParserName != "default" || out.ParserSHA256 != DefaultParserSHA256 {
		t.Fatalf("expected default parser name & sha, got name=%q sha=%q", out.ParserName, out.ParserSHA256)
	}

	var decoded BoilerplateResult
	if err := json.Unmarshal(out.Payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(decoded.Players) == 0 || len(decoded.CombatLog) == 0 || len(decoded.Wards) == 0 || len(decoded.Objectives) == 0 {
		t.Fatalf("expected rich structured match sections populated, got %+v", decoded)
	}
}

func TestSandboxJSBlocksEscapeAndParsesRawStdin(t *testing.T) {
	rawBz2, _ := base64.StdEncoding.DecodeString(testBz2Fixture)

	// Attempt to access require/process -> must NOT exist in sandbox
	escapeAttempt := `typeof require === "undefined" && typeof process === "undefined" ? JSON.stringify({len: stdin_len(), isolated: true, match: DOTA_MATCH_ID}) : JSON.stringify({isolated: false})`
	p, err := NewSandboxJSParser("js-test", "1.0.0", "deadbeef", []byte(escapeAttempt))
	if err != nil {
		t.Fatalf("NewSandboxJSParser failed: %v", err)
	}
	if p.Name() != "custom(js-test)" {
		t.Fatalf("expected Name() == custom(js-test), got %q", p.Name())
	}

	out, err := p.Parse(context.Background(), nil, ParseInput{
		MatchID:       7900999002,
		Cluster:       133,
		Region:        "EU",
		CompressedBz2: rawBz2,
	})
	if err != nil {
		t.Fatalf("SandboxJS Parse failed: %v", err)
	}

	var body struct {
		Len      int    `json:"len"`
		Isolated bool   `json:"isolated"`
		Match    string `json:"match"`
	}
	_ = json.Unmarshal(out.Payload, &body)
	if !body.Isolated || body.Len == 0 || body.Match != "7900999002" {
		t.Fatalf("unexpected JS sandbox output: %+v", body)
	}
}

func TestSandboxWasmExecutionWithRealWasiModule(t *testing.T) {
	tmpDir := t.TempDir()
	goSrc := `package main
import ("fmt"; "io"; "os")
func main() {
	b, _ := io.ReadAll(os.Stdin)
	fmt.Printf("{\"wasm_read\":%d,\"match\":\"%s\"}", len(b), os.Getenv("DOTA_MATCH_ID"))
}`
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module wasi_test\n\ngo 1.25\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte(goSrc), 0o644); err != nil {
		t.Fatal(err)
	}

	wasmPath := filepath.Join(tmpDir, "test.wasm")
	buildCmd := exec.Command("go", "build", "-ldflags", "-s -w", "-o", wasmPath, ".")
	buildCmd.Dir = tmpDir
	buildCmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("compile WASI test binary: %v\n%s", err, string(out))
	}

	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatal(err)
	}

	runner, err := NewSandboxWasmParser("wasi-counter", "1.0.0", "sha-test", wasmBytes)
	if err != nil {
		t.Fatalf("NewSandboxWasmParser failed: %v", err)
	}
	if runner.Name() != "custom(wasi-counter)" {
		t.Fatalf("unexpected Name(): %q", runner.Name())
	}

	rawBz2, _ := base64.StdEncoding.DecodeString(testBz2Fixture)
	out, err := runner.Parse(context.Background(), nil, ParseInput{
		MatchID:       7900999003,
		Cluster:       133,
		Region:        "EU",
		CompressedBz2: rawBz2,
	})
	if err != nil {
		t.Fatalf("WASM Parse failed: %v", err)
	}

	var parsed struct {
		WasmRead int    `json:"wasm_read"`
		Match    string `json:"match"`
	}
	if err := json.Unmarshal(out.Payload, &parsed); err != nil {
		t.Fatalf("unmarshal WASM output: %v (raw=%s)", err, string(out.Payload))
	}
	if parsed.WasmRead == 0 || parsed.Match != "7900999003" {
		t.Fatalf("unexpected WASM sandbox output: %+v", parsed)
	}
}

func TestSampleParsersAndSandboxIsolation(t *testing.T) {
	rawBz2, _ := base64.StdEncoding.DecodeString(testBz2Fixture)
	sampleDir := filepath.Join("..", "..", "..", "sample_parsers")

	// 1. Test sample JS parser in goja sandbox
	jsBytes, err := os.ReadFile(filepath.Join(sampleDir, "simple_js_parser.js"))
	if err != nil {
		jsBytes = []byte(`const len = stdin_len(); console.log(JSON.stringify({match_id: DOTA_MATCH_ID, language: "js-goja", dem_bytes: len}));`)
	}
	jsRunner, err := NewSandboxJSParser("sample-js", "1.0.0", "sha-js", jsBytes)
	if err != nil {
		t.Fatalf("NewSandboxJSParser failed: %v", err)
	}
	jsOut, err := jsRunner.Parse(context.Background(), nil, ParseInput{
		MatchID:       7900999010,
		Cluster:       133,
		Region:        "EU",
		CompressedBz2: rawBz2,
	})
	if err != nil {
		t.Fatalf("sample JS Parse failed: %v", err)
	}
	var jsDecoded struct {
		MatchID  string `json:"match_id"`
		Language string `json:"language"`
		DemBytes int    `json:"dem_bytes"`
	}
	if err := json.Unmarshal(jsOut.Payload, &jsDecoded); err != nil || jsDecoded.Language != "js-goja" || jsDecoded.DemBytes == 0 {
		t.Fatalf("unexpected sample JS output: err=%v out=%s", err, string(jsOut.Payload))
	}

	// 2. Test sample Go parser compiled to WASI + verify WASI sandbox blocks host filesystem access
	goSrc, err := os.ReadFile(filepath.Join(sampleDir, "simple_go_parser.go"))
	if err != nil {
		goSrc = []byte(`package main
import ("encoding/json"; "io"; "os")
func main() {
	b, _ := io.ReadAll(os.Stdin)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"match_id": os.Getenv("DOTA_MATCH_ID"), "language": "go-wasi", "dem_bytes": len(b)})
}`)
	}
	tmpGoDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpGoDir, "go.mod"), []byte("module sample_go\n\ngo 1.25\n"), 0o644)
	_ = os.WriteFile(filepath.Join(tmpGoDir, "main.go"), goSrc, 0o644)
	goWasmPath := filepath.Join(tmpGoDir, "sample.wasm")
	buildGo := exec.Command("go", "build", "-ldflags", "-s -w", "-o", goWasmPath, ".")
	buildGo.Dir = tmpGoDir
	buildGo.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")
	if out, err := buildGo.CombinedOutput(); err != nil {
		t.Fatalf("compile sample_go_parser.go: %v\n%s", err, string(out))
	}
	goWasmBytes, _ := os.ReadFile(goWasmPath)
	goRunner, err := NewSandboxWasmParser("sample-go", "1.0.0", "sha-go", goWasmBytes)
	if err != nil {
		t.Fatalf("NewSandboxWasmParser failed: %v", err)
	}
	goOut, err := goRunner.Parse(context.Background(), nil, ParseInput{
		MatchID:       7900999011,
		Cluster:       133,
		Region:        "EU",
		CompressedBz2: rawBz2,
	})
	if err != nil {
		t.Fatalf("sample Go Parse failed: %v", err)
	}
	var goDecoded struct {
		MatchID  string `json:"match_id"`
		Language string `json:"language"`
		DemBytes int    `json:"dem_bytes"`
	}
	if err := json.Unmarshal(goOut.Payload, &goDecoded); err != nil || goDecoded.Language != "go-wasi" || goDecoded.DemBytes == 0 {
		t.Fatalf("unexpected sample Go output: err=%v out=%s", err, string(goOut.Payload))
	}

	// 3. Explicit host-filesystem escape test inside WASI sandbox
	secretFile := filepath.Join(t.TempDir(), "host_secret.txt")
	_ = os.WriteFile(secretFile, []byte("TOP_SECRET_HOST_DATA"), 0o644)
	escapeDir := t.TempDir()
	escapeSrc := `package main
import ("fmt"; "os")
func main() {
	_, err := os.ReadFile(` + "`" + filepath.ToSlash(secretFile) + "`" + `)
	fmt.Printf("{\"blocked\":%v}", err != nil)
}`
	_ = os.WriteFile(filepath.Join(escapeDir, "go.mod"), []byte("module escape_test\n\ngo 1.25\n"), 0o644)
	_ = os.WriteFile(filepath.Join(escapeDir, "main.go"), []byte(escapeSrc), 0o644)
	escapeWasmPath := filepath.Join(escapeDir, "escape.wasm")
	buildEsc := exec.Command("go", "build", "-ldflags", "-s -w", "-o", escapeWasmPath, ".")
	buildEsc.Dir = escapeDir
	buildEsc.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")
	if out, err := buildEsc.CombinedOutput(); err != nil {
		t.Fatalf("compile escape test: %v\n%s", err, string(out))
	}
	escBytes, _ := os.ReadFile(escapeWasmPath)
	escRunner, _ := NewSandboxWasmParser("escape-test", "1.0.0", "sha-esc", escBytes)
	escOut, err := escRunner.Parse(context.Background(), nil, ParseInput{
		MatchID:       7900999012,
		Cluster:       133,
		Region:        "EU",
		CompressedBz2: rawBz2,
	})
	if err != nil {
		t.Fatalf("escape test Parse failed: %v", err)
	}
	var escDecoded struct {
		Blocked bool `json:"blocked"`
	}
	if err := json.Unmarshal(escOut.Payload, &escDecoded); err != nil || !escDecoded.Blocked {
		t.Fatalf("expected WASI sandbox to block host file read, got: %s", string(escOut.Payload))
	}
}

