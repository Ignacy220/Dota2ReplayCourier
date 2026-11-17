package parsers

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestUploadPackageAndMatchValidation(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Provide a stub valid WASM runtime for offline .py bundling tests.
	stubPyWasm := filepath.Join(tmpDir, "stub_python.wasm")
	if err := os.WriteFile(stubPyWasm, append([]byte{}, wasmMagic...), 0o644); err != nil {
		t.Fatalf("write stub py wasm: %v", err)
	}
	t.Setenv("PYTHON_WASM_PATH", stubPyWasm)

	mgr, err := NewManager(filepath.Join(tmpDir, "parsers"))
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	// Default starts active
	if act := mgr.GetActive(); act.Mode != "default" || act.ReportedName != "default" {
		t.Fatalf("expected initial active default parser, got %+v", act)
	}
	if !mgr.MatchesActive("default", DefaultParserSHA256) {
		t.Fatalf("expected default parser to match")
	}
	if mgr.MatchesActive("custom(foo)", "abc") {
		t.Fatalf("expected stale custom parser to be rejected when default is active")
	}

	// 1. Go source -> compiled to WASI .wasm
	goSrc := `package main
import ("fmt"; "io"; "os")
func main() {
	b, _ := io.ReadAll(os.Stdin)
	fmt.Printf("{\"bytes\":%d}", len(b))
}`
	goDesc, err := mgr.UploadCustom(ctx, "go-bytes", "bytes.go", []byte(goSrc), true)
	if err != nil {
		t.Fatalf("UploadCustom(.go) failed: %v", err)
	}
	if goDesc.Kind != "go" || goDesc.ReportedName != "custom(go-bytes)" {
		t.Fatalf("unexpected go descriptor: %+v", goDesc)
	}
	goArt, _, err := mgr.GetArtifact(goDesc.SHA256)
	if err != nil || len(goArt) < 8 || !bytes.Equal(goArt[:8], wasmMagic) {
		t.Fatalf("expected valid compiled .wasm artifact for .go upload, err=%v", err)
	}
	if mgr.MatchesActive("default", DefaultParserSHA256) {
		t.Fatalf("expected default submission rejected once custom(go-bytes) is active")
	}
	if !mgr.MatchesActive("custom(go-bytes)", goDesc.SHA256) {
		t.Fatalf("expected custom(go-bytes) matching SHA to pass")
	}

	// 2. Lua script -> compiled into standalone sandboxed gopher-lua .wasm
	luaSrc := `local n = stdin_len()
print(string.format('{"lua_len":%d}', n))`
	luaDesc, err := mgr.UploadCustom(ctx, "lua-len", "check.lua", []byte(luaSrc), true)
	if err != nil {
		t.Fatalf("UploadCustom(.lua) failed: %v", err)
	}
	luaArt, _, err := mgr.GetArtifact(luaDesc.SHA256)
	if err != nil || len(luaArt) < 8 || !bytes.Equal(luaArt[:8], wasmMagic) {
		t.Fatalf("expected valid compiled .wasm artifact for .lua upload, err=%v", err)
	}

	// 3. Re-uploading with the SAME name changes SHA256 and rejects the old SHA256!
	luaDesc2, err := mgr.UploadCustom(ctx, "lua-len", "check.lua", []byte(`print('{"v":2}')`), true)
	if err != nil {
		t.Fatalf("re-upload lua failed: %v", err)
	}
	if luaDesc2.SHA256 == luaDesc.SHA256 {
		t.Fatalf("expected updated source under same parser name to get a new SHA256")
	}
	if mgr.MatchesActive("custom(lua-len)", luaDesc.SHA256) {
		t.Fatalf("expected old revision under same name to be dismissed!")
	}
	if !mgr.MatchesActive("custom(lua-len)", luaDesc2.SHA256) {
		t.Fatalf("expected new revision under same name to be accepted")
	}

	// 4. JS validated & Python packaged with custom WASM section
	jsDesc, err := mgr.UploadCustom(ctx, "js-check", "check.js", []byte(`JSON.stringify({ok: true})`), false)
	if err != nil || jsDesc.Kind != "js" {
		t.Fatalf("UploadCustom(.js) failed: %v %+v", err, jsDesc)
	}
	if _, err := mgr.UploadCustom(ctx, "bad-js", "bad.js", []byte(`function ( {`), false); err == nil {
		t.Fatalf("expected syntax error for invalid JS")
	}

	pyDesc, err := mgr.UploadCustom(ctx, "py-check", "check.py", []byte(`import sys, json; print(json.dumps({"n": len(sys.stdin.buffer.read())}))`), true)
	if err != nil || pyDesc.Kind != "py" {
		t.Fatalf("UploadCustom(.py) failed: %v %+v", err, pyDesc)
	}
	pyArt, _, _ := mgr.GetArtifact(pyDesc.SHA256)
	if !bytes.Contains(pyArt, []byte(pyCustomSecName)) {
		t.Fatalf("expected custom section %q embedded in .py .wasm artifact", pyCustomSecName)
	}
}
