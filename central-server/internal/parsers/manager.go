package parsers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/dota2-analytics/central-server/internal/models"
	lua "github.com/yuin/gopher-lua"
)

const (
	DefaultParserSHA256 = "builtin-default-v2"
	RawParserSHA256     = "builtin-raw-replay-v1"

	pythonWasmURL    = "https://github.com/vmware-labs/webassembly-language-runtimes/releases/download/python/3.12.0%2B20231211-040d5a6/python-3.12.0.wasm"
	pythonWasmSHA256 = "e5dc5a398b07b54ea8fdb503bf68fb583d533f10ec3f930963e02b9505f7a763"
	pyCustomSecName  = "dota_py_entrypoint"
)

var (
	validNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{2,40}$`)
	wasmMagic   = []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}
)

type persistedState struct {
	GlobalRevision int                                `json:"global_revision"`
	ActiveID       string                             `json:"active_id"`
	Parsers        map[string]models.ParserDescriptor `json:"parsers"`
}

// Manager stores uploaded custom parsers, compiles/packages scripts into sandboxed WASI .wasm
// or validated .js artifacts, tracks active parser revision/hashes, and checks worker reports.
type Manager struct {
	baseDir     string
	artifactDir string
	sourceDir   string
	runtimeDir  string
	statePath   string

	mu    sync.RWMutex
	state persistedState
}

// NewManager opens or initializes parser storage at baseDir (default: data/parsers).
func NewManager(baseDir string) (*Manager, error) {
	if strings.TrimSpace(baseDir) == "" {
		baseDir = filepath.Join("data", "parsers")
	}
	artDir := filepath.Join(baseDir, "artifacts")
	srcDir := filepath.Join(baseDir, "sources")
	rtDir := filepath.Join(filepath.Dir(baseDir), "runtimes")

	for _, dir := range []string{baseDir, artDir, srcDir, rtDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create parser dir %s: %w", dir, err)
		}
	}

	m := &Manager{
		baseDir:     baseDir,
		artifactDir: artDir,
		sourceDir:   srcDir,
		runtimeDir:  rtDir,
		statePath:   filepath.Join(baseDir, "state.json"),
		state: persistedState{
			GlobalRevision: 1,
			ActiveID:       "default",
			Parsers:        make(map[string]models.ParserDescriptor),
		},
	}
	m.load()
	return m, nil
}

func (m *Manager) load() {
	raw, err := os.ReadFile(m.statePath)
	if err != nil {
		_ = m.saveLocked()
		return
	}
	var loaded persistedState
	if err := json.Unmarshal(raw, &loaded); err != nil {
		return
	}
	if loaded.Parsers == nil {
		loaded.Parsers = make(map[string]models.ParserDescriptor)
	}
	if loaded.GlobalRevision <= 0 {
		loaded.GlobalRevision = 1
	}
	if loaded.ActiveID == "" {
		loaded.ActiveID = "default"
	}
	if loaded.ActiveID != "default" && loaded.ActiveID != "raw" {
		if _, ok := loaded.Parsers[loaded.ActiveID]; !ok {
			loaded.ActiveID = "default"
		}
	}
	m.state = loaded
}

func (m *Manager) saveLocked() error {
	raw, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(m.statePath, raw, 0o644)
}

// SaveState persists the current active parser selection to disk.
func (m *Manager) SaveState() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saveLocked()
}

func (m *Manager) defaultDescriptorLocked() models.ParserDescriptor {
	return models.ParserDescriptor{
		Mode:         "default",
		ID:           "default",
		Name:         "default",
		ReportedName: "default",
		Kind:         "default",
		Revision:     m.state.GlobalRevision,
		SHA256:       DefaultParserSHA256,
		UpdatedAt:    time.Now().UTC(),
	}
}

func (m *Manager) rawDescriptorLocked() models.ParserDescriptor {
	return models.ParserDescriptor{
		Mode:         "raw",
		ID:           "raw",
		Name:         "raw",
		ReportedName: "raw",
		Kind:         "raw",
		Revision:     m.state.GlobalRevision,
		SHA256:       RawParserSHA256,
		UpdatedAt:    time.Now().UTC(),
	}
}

// GetActive returns a copy of the currently configured parser descriptor.
func (m *Manager) GetActive() models.ParserDescriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.state.ActiveID == "default" {
		return m.defaultDescriptorLocked()
	}
	if m.state.ActiveID == "raw" {
		return m.rawDescriptorLocked()
	}
	desc, ok := m.state.Parsers[m.state.ActiveID]
	if !ok {
		return m.defaultDescriptorLocked()
	}
	return desc
}

// ListAll returns all saved custom parsers alongside the currently active parser descriptor.
func (m *Manager) ListAll() (active models.ParserDescriptor, custom []models.ParserDescriptor) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.state.ActiveID == "default" {
		active = m.defaultDescriptorLocked()
	} else if m.state.ActiveID == "raw" {
		active = m.rawDescriptorLocked()
	} else if d, ok := m.state.Parsers[m.state.ActiveID]; ok {
		active = d
	} else {
		active = m.defaultDescriptorLocked()
	}

	custom = make([]models.ParserDescriptor, 0, len(m.state.Parsers))
	for _, d := range m.state.Parsers {
		custom = append(custom, d)
	}
	return active, custom
}

// ActivateDefault switches the active parser back to the built-in rich Default parser.
func (m *Manager) ActivateDefault() (models.ParserDescriptor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.GlobalRevision++
	m.state.ActiveID = "default"
	if err := m.saveLocked(); err != nil {
		return models.ParserDescriptor{}, err
	}
	return m.defaultDescriptorLocked(), nil
}

// ActivateRaw switches the pipeline to omit parsing and have workers return raw .dem.bz2 replays.
func (m *Manager) ActivateRaw() (models.ParserDescriptor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.GlobalRevision++
	m.state.ActiveID = "raw"
	if err := m.saveLocked(); err != nil {
		return models.ParserDescriptor{}, err
	}
	return m.rawDescriptorLocked(), nil
}

// ActivateCustom activates a previously uploaded custom parser by ID.
func (m *Manager) ActivateCustom(id string) (models.ParserDescriptor, error) {
	cleanID := strings.ToLower(strings.TrimSpace(id))
	m.mu.Lock()
	defer m.mu.Unlock()

	desc, ok := m.state.Parsers[cleanID]
	if !ok {
		return models.ParserDescriptor{}, fmt.Errorf("custom parser %q not found", id)
	}
	m.state.GlobalRevision++
	desc.Revision = m.state.GlobalRevision
	desc.UpdatedAt = time.Now().UTC()
	m.state.Parsers[cleanID] = desc
	m.state.ActiveID = cleanID
	if err := m.saveLocked(); err != nil {
		return models.ParserDescriptor{}, err
	}
	return desc, nil
}

// ActivateEphemeral activates either "default" or a custom parser ID in memory without writing state.json.
func (m *Manager) ActivateEphemeral(id string) (models.ParserDescriptor, error) {
	cleanID := strings.ToLower(strings.TrimSpace(id))
	m.mu.Lock()
	defer m.mu.Unlock()

	if cleanID == "" || cleanID == "default" {
		m.state.GlobalRevision++
		m.state.ActiveID = "default"
		return m.defaultDescriptorLocked(), nil
	}
	if cleanID == "raw" || cleanID == "none" {
		m.state.GlobalRevision++
		m.state.ActiveID = "raw"
		return m.rawDescriptorLocked(), nil
	}

	desc, ok := m.state.Parsers[cleanID]
	if !ok {
		return models.ParserDescriptor{}, fmt.Errorf("custom parser %q not found", id)
	}
	m.state.GlobalRevision++
	desc.Revision = m.state.GlobalRevision
	desc.UpdatedAt = time.Now().UTC()
	m.state.Parsers[cleanID] = desc
	m.state.ActiveID = cleanID
	return desc, nil
}

// DeleteCustom removes a saved custom parser (switching back to default if it was active).
func (m *Manager) DeleteCustom(id string) (models.ParserDescriptor, error) {
	cleanID := strings.ToLower(strings.TrimSpace(id))
	m.mu.Lock()
	defer m.mu.Unlock()

	desc, ok := m.state.Parsers[cleanID]
	if !ok {
		return models.ParserDescriptor{}, fmt.Errorf("custom parser %q not found", id)
	}
	delete(m.state.Parsers, cleanID)
	if desc.SHA256 != "" {
		_ = os.Remove(filepath.Join(m.artifactDir, desc.SHA256+".bin"))
	}
	if m.state.ActiveID == cleanID {
		m.state.GlobalRevision++
		m.state.ActiveID = "default"
	}
	if err := m.saveLocked(); err != nil {
		return models.ParserDescriptor{}, err
	}
	if m.state.ActiveID == "default" {
		return m.defaultDescriptorLocked(), nil
	}
	return m.state.Parsers[m.state.ActiveID], nil
}

// GetArtifact reads the packaged binary artifact for a given SHA-256 digest.
func (m *Manager) GetArtifact(shaHex string) ([]byte, *models.ParserDescriptor, error) {
	cleanSHA := strings.ToLower(strings.TrimSpace(shaHex))
	if len(cleanSHA) != 64 {
		return nil, nil, errors.New("invalid sha256")
	}
	raw, err := os.ReadFile(filepath.Join(m.artifactDir, cleanSHA+".bin"))
	if err != nil {
		return nil, nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, d := range m.state.Parsers {
		if d.SHA256 == cleanSHA {
			copyD := d
			return raw, &copyD, nil
		}
	}
	return raw, nil, nil
}

// MatchesActive validates that a worker's reported parser_name and optional parser_sha256
// match the currently configured active parser on the server.
func (m *Manager) MatchesActive(reportedName, reportedSHA256 string) bool {
	active := m.GetActive()
	name := strings.TrimSpace(reportedName)
	sha := strings.ToLower(strings.TrimSpace(reportedSHA256))

	if active.Mode == "default" {
		if name != "default" {
			return false
		}
		return sha == "" || sha == active.SHA256
	}
	if active.Mode == "raw" {
		if name != "raw" {
			return false
		}
		return sha == "" || sha == active.SHA256
	}

	// Custom parser requires exact "custom(<name>)" AND matching SHA-256 digest so edits under
	// the same name invalidate stale workers cleanly.
	if name != active.ReportedName {
		return false
	}
	return sha != "" && sha == active.SHA256
}

// UploadCustom packages a custom parser (.wasm, .go, .lua, .js, .py) into a sandboxed artifact,
// saves it, and optionally sets it as the active parser.
func (m *Manager) UploadCustom(ctx context.Context, rawName, filename string, sourceBytes []byte, activate bool) (models.ParserDescriptor, error) {
	name := strings.TrimSpace(rawName)
	if name == "" {
		base := filepath.Base(filename)
		name = strings.TrimSuffix(base, filepath.Ext(base))
	}
	if !validNameRe.MatchString(name) || strings.EqualFold(name, "default") || strings.EqualFold(name, "raw") || strings.EqualFold(name, "none") {
		return models.ParserDescriptor{}, errors.New("parser name must be 2-40 characters (letters, digits, _, -), and cannot be 'default', 'raw', or 'none'")
	}
	if len(sourceBytes) == 0 {
		return models.ParserDescriptor{}, errors.New("uploaded parser file is empty")
	}

	ext := strings.ToLower(filepath.Ext(filename))
	var kind string
	var artifact []byte
	var err error

	switch ext {
	case ".wasm":
		kind = "wasm"
		if len(sourceBytes) < 8 || !bytes.Equal(sourceBytes[:8], wasmMagic) {
			return models.ParserDescriptor{}, errors.New("invalid .wasm binary header")
		}
		artifact = sourceBytes

	case ".go":
		kind = "go"
		artifact, err = compileGoSourceToWasm(ctx, sourceBytes)
		if err != nil {
			return models.ParserDescriptor{}, err
		}

	case ".lua":
		kind = "lua"
		artifact, err = packageLuaSourceToWasm(ctx, sourceBytes)
		if err != nil {
			return models.ParserDescriptor{}, err
		}

	case ".js":
		kind = "js"
		if _, err := goja.Compile(filepath.Base(filename), string(sourceBytes), true); err != nil {
			return models.ParserDescriptor{}, fmt.Errorf("JavaScript syntax error: %w", err)
		}
		artifact = sourceBytes

	case ".py":
		kind = "py"
		artifact, err = m.packagePythonScriptToWasm(ctx, sourceBytes)
		if err != nil {
			return models.ParserDescriptor{}, err
		}

	default:
		return models.ParserDescriptor{}, fmt.Errorf("unsupported file extension %q (supported: .wasm, .go, .lua, .py, .js)", ext)
	}

	sum := sha256.Sum256(artifact)
	shaHex := hex.EncodeToString(sum[:])

	artPath := filepath.Join(m.artifactDir, shaHex+".bin")
	if err := os.WriteFile(artPath, artifact, 0o644); err != nil {
		return models.ParserDescriptor{}, fmt.Errorf("store artifact: %w", err)
	}

	cleanID := strings.ToLower(name)
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.GlobalRevision++
	srcPath := filepath.Join(m.sourceDir, fmt.Sprintf("%s_rev%d%s", cleanID, m.state.GlobalRevision, ext))
	_ = os.WriteFile(srcPath, sourceBytes, 0o644)

	desc := models.ParserDescriptor{
		Mode:         "custom",
		ID:           cleanID,
		Name:         name,
		ReportedName: fmt.Sprintf("custom(%s)", name),
		Kind:         kind,
		Filename:     filepath.Base(filename),
		Revision:     m.state.GlobalRevision,
		SHA256:       shaHex,
		ArtifactSize: int64(len(artifact)),
		DownloadURL:  fmt.Sprintf("/api/v1/parser/artifact/%s", shaHex),
		UpdatedAt:    time.Now().UTC(),
	}
	m.state.Parsers[cleanID] = desc
	if activate {
		m.state.ActiveID = cleanID
	}
	if err := m.saveLocked(); err != nil {
		return models.ParserDescriptor{}, err
	}
	return desc, nil
}

func (m *Manager) packagePythonScriptToWasm(ctx context.Context, script []byte) ([]byte, error) {
	if len(bytes.TrimSpace(script)) == 0 {
		return nil, errors.New("python script cannot be empty")
	}
	pyWasmPath := strings.TrimSpace(os.Getenv("PYTHON_WASM_PATH"))
	if pyWasmPath == "" {
		pyWasmPath = filepath.Join(m.runtimeDir, "python-3.12.0.wasm")
		if err := ensurePythonWasmCached(ctx, pyWasmPath); err != nil {
			return nil, fmt.Errorf("prepare python WASI runtime: %w", err)
		}
	}
	baseWasm, err := os.ReadFile(pyWasmPath)
	if err != nil {
		return nil, fmt.Errorf("read python wasm runtime %s: %w", pyWasmPath, err)
	}
	if len(baseWasm) < 8 || !bytes.Equal(baseWasm[:8], wasmMagic) {
		return nil, fmt.Errorf("invalid python wasm runtime at %s", pyWasmPath)
	}
	return AppendWasmCustomSection(baseWasm, pyCustomSecName, script), nil
}

func ensurePythonWasmCached(ctx context.Context, destPath string) error {
	if info, err := os.Stat(destPath); err == nil && info.Size() > 0 {
		if ok, _ := verifyFileSHA256(destPath, pythonWasmSHA256); ok {
			return nil
		}
	}

	dlCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(dlCtx, http.MethodGet, pythonWasmURL, nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d fetching CPython WASI runtime", resp.StatusCode)
	}

	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, hasher), resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	_ = f.Close()

	gotSHA := hex.EncodeToString(hasher.Sum(nil))
	if gotSHA != pythonWasmSHA256 {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("python.wasm sha256 mismatch: got %s want %s", gotSHA, pythonWasmSHA256)
	}
	return os.Rename(tmpPath, destPath)
}

func verifyFileSHA256(path, wantHex string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == wantHex, nil
}

// AppendWasmCustomSection appends a legal WebAssembly custom section (ID=0) to a valid .wasm module.
func AppendWasmCustomSection(wasmBytes []byte, secName string, payload []byte) []byte {
	var body bytes.Buffer
	writeULEB128(&body, uint32(len(secName)))
	body.WriteString(secName)
	body.Write(payload)

	var out bytes.Buffer
	out.Grow(len(wasmBytes) + 1 + 5 + body.Len())
	out.Write(wasmBytes)
	out.WriteByte(0x00) // Custom section ID = 0
	writeULEB128(&out, uint32(body.Len()))
	out.Write(body.Bytes())
	return out.Bytes()
}

func writeULEB128(buf *bytes.Buffer, v uint32) {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		buf.WriteByte(b)
		if v == 0 {
			break
		}
	}
}

func compileGoSourceToWasm(ctx context.Context, src []byte) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "dota-go-parser-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	if err := os.WriteFile(filepath.Join(tmpDir, "main.go"), src, 0o644); err != nil {
		return nil, err
	}
	usesManta := strings.Contains(string(src), "github.com/dotabuff/manta")
	seedGoModuleFiles(tmpDir, false, usesManta)
	return buildDirToWasiWasm(ctx, tmpDir, usesManta)
}

func packageLuaSourceToWasm(ctx context.Context, luaSrc []byte) ([]byte, error) {
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()
	if _, err := L.LoadString(string(luaSrc)); err != nil {
		return nil, fmt.Errorf("Lua syntax error: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "dota-lua-parser-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	if err := os.WriteFile(filepath.Join(tmpDir, "script.lua"), luaSrc, 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte(luaWasmWrapperMainGo), 0o644); err != nil {
		return nil, err
	}
	seedGoModuleFiles(tmpDir, true, false)
	return buildDirToWasiWasm(ctx, tmpDir, true)
}

func seedGoModuleFiles(tmpDir string, needLua, needManta bool) {
	var modBuf strings.Builder
	modBuf.WriteString("module dota_custom_parser\n\ngo 1.25\n\n")

	if needLua {
		modBuf.WriteString("require github.com/yuin/gopher-lua v1.1.2\n")
		copyNearestGoSum(tmpDir, "central-server")
	} else if needManta {
		modBuf.WriteString("require github.com/dotabuff/manta v1.5.2\n")
		copyNearestGoSum(tmpDir, "desktop-worker")
	}
	_ = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modBuf.String()), 0o644)
}

func copyNearestGoSum(tmpDir, sub string) {
	candidates := []string{
		"go.sum",
		filepath.Join(sub, "go.sum"),
		filepath.Join("..", sub, "go.sum"),
		filepath.Join("..", "..", sub, "go.sum"),
		filepath.Join("..", "..", "..", sub, "go.sum"),
	}
	for _, p := range candidates {
		if raw, err := os.ReadFile(p); err == nil && len(raw) > 0 {
			_ = os.WriteFile(filepath.Join(tmpDir, "go.sum"), raw, 0o644)
			return
		}
	}
}

func buildDirToWasiWasm(ctx context.Context, dir string, withModTidy bool) ([]byte, error) {
	buildCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	env := append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")

	if withModTidy {
		tidyCmd := exec.CommandContext(buildCtx, "go", "mod", "tidy")
		tidyCmd.Dir = dir
		tidyCmd.Env = env
		if out, err := tidyCmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("go mod tidy failed: %v\n%s", err, strings.TrimSpace(string(out)))
		}
	}

	outPath := filepath.Join(dir, "parser.wasm")
	buildCmd := exec.CommandContext(buildCtx, "go", "build", "-ldflags", "-s -w", "-o", outPath, ".")
	buildCmd.Dir = dir
	buildCmd.Env = env
	if out, err := buildCmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("WASM compilation failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}

	return os.ReadFile(outPath)
}

// Keep binary import happy if unused directly in non-test helper
var _ = binary.MaxVarintLen32

const luaWasmWrapperMainGo = `package main

import (
	_ "embed"
	"fmt"
	"io"
	"os"

	lua "github.com/yuin/gopher-lua"
)

//go:embed script.lua
var userLuaScript string

func main() {
	demBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read stdin: %v\n", err)
		os.Exit(1)
	}

	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()

	// Strict sandbox: expose ONLY pure in-memory standard libraries (no io, os, debug, or package loaders).
	for _, openFn := range []lua.LGFunction{
		lua.OpenBase,
		lua.OpenTable,
		lua.OpenString,
		lua.OpenMath,
	} {
		L.Push(L.NewFunction(openFn))
		L.Push(lua.LString(""))
		L.Call(1, 0)
	}

	// Remove filesystem helpers from base if present
	L.SetGlobal("dofile", lua.LNil)
	L.SetGlobal("loadfile", lua.LNil)

	L.SetGlobal("read_stdin", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString(string(demBytes)))
		return 1
	}))
	L.SetGlobal("stdin_len", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LNumber(len(demBytes)))
		return 1
	}))
	L.SetGlobal("stdin_slice", L.NewFunction(func(L *lua.LState) int {
		start := L.CheckInt(1)
		length := L.CheckInt(2)
		if start < 0 {
			start = 0
		}
		if start > len(demBytes) {
			L.Push(lua.LString(""))
			return 1
		}
		end := start + length
		if length < 0 || end > len(demBytes) {
			end = len(demBytes)
		}
		L.Push(lua.LString(string(demBytes[start:end])))
		return 1
	}))
	L.SetGlobal("DOTA_MATCH_ID", lua.LString(os.Getenv("DOTA_MATCH_ID")))
	L.SetGlobal("DOTA_CLUSTER", lua.LString(os.Getenv("DOTA_CLUSTER")))
	L.SetGlobal("DOTA_REGION", lua.LString(os.Getenv("DOTA_REGION")))

	if err := L.DoString(userLuaScript); err != nil {
		fmt.Fprintf(os.Stderr, "lua runtime error: %v\n", err)
		os.Exit(1)
	}
	if L.GetTop() >= 1 {
		top := L.Get(-1)
		if s, ok := top.(lua.LString); ok && len(s) > 0 {
			os.Stdout.WriteString(string(s))
		}
	}
}
`
