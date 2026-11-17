package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/dota2-analytics/desktop-worker/internal/client"
	"github.com/dota2-analytics/desktop-worker/internal/config"
	"github.com/dota2-analytics/desktop-worker/internal/downloader"
	"github.com/dota2-analytics/desktop-worker/internal/orchestrator"
	"github.com/dota2-analytics/desktop-worker/internal/sentinel"
	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:frontend/dist
var embeddedAssets embed.FS

func main() {
	cfgMgr := config.NewManager("")
	if envURL := os.Getenv("CENTRAL_SERVER_URL"); envURL != "" {
		cfgMgr.Update(func(c *config.WorkerConfig) {
			c.ServerURL = envURL
		})
	}

	sent := sentinel.NewSentinel(cfgMgr)
	apiClient := client.NewAPIClient()
	dl := downloader.NewDownloader()
	orch := orchestrator.NewOrchestrator(cfgMgr, sent, apiClient, dl)

	app := NewApp(cfgMgr, sent, orch)

	distFS, err := fs.Sub(embeddedAssets, "frontend/dist")
	if err != nil {
		log.Fatalf("Failed to load embedded frontend assets: %v", err)
	}

	startLocalAPIBridge(app, distFS)

	err = wails.Run(&options.App{
		Title:     "Dota2ReplayCourier",
		Width:     1120,
		Height:    720,
		MinWidth:  920,
		MinHeight: 600,
		AssetServer: &assetserver.Options{
			Assets: distFS,
		},
		BackgroundColour: &options.RGBA{R: 10, G: 13, B: 20, A: 1},
		OnStartup:        app.Startup,
		Bind: []interface{}{
			app,
		},
	})

	if err != nil {
		runFallbackWindowLoop(app)
	}
}

func startLocalAPIBridge(app *App, distFS fs.FS) string {
	uiPort := os.Getenv("WORKER_UI_PORT")
	if uiPort == "" {
		uiPort = "34115"
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(distFS)))
	mux.HandleFunc("/api/local/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(app.GetStatus())
	})
	mux.HandleFunc("/api/local/toggle-pause", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(app.TogglePause())
	})
	mux.HandleFunc("/api/local/benchmark", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(app.RunBenchmark())
	})
	mux.HandleFunc("/api/local/preferences", func(w http.ResponseWriter, r *http.Request) {
		var upd PreferencesUpdate
		_ = json.NewDecoder(r.Body).Decode(&upd)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(app.SavePreferences(upd))
	})
	mux.HandleFunc("/api/local/test-connection", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ServerURL string `json:"server_url"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(app.TestServerConnection(body.ServerURL))
	})
	mux.HandleFunc("/api/local/auth/register", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Nickname string `json:"nickname"`
			Password string `json:"password"`
			Faction  string `json:"faction"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		snap, err := app.AuthRegister(body.Nickname, body.Password, body.Faction)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(snap)
	})
	mux.HandleFunc("/api/local/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Nickname string `json:"nickname"`
			Password string `json:"password"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		snap, err := app.AuthLogin(body.Nickname, body.Password)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(snap)
	})
	mux.HandleFunc("/api/local/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(app.AuthLogout())
	})
	mux.HandleFunc("/api/local/auth/dismiss", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(app.DismissAuthPrompt())
	})
	mux.HandleFunc("/api/local/reprobe-regions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(app.ReprobeRegions())
	})

	ln, err := net.Listen("tcp", "127.0.0.1:"+uiPort)
	if err != nil {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return ""
		}
	}

	srv := &http.Server{Handler: mux}
	go func() {
		_ = srv.Serve(ln)
	}()

	return fmt.Sprintf("http://%s", ln.Addr().String())
}

func runFallbackWindowLoop(app *App) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app.Startup(ctx)
	url := "http://127.0.0.1:34115"

	switch runtime.GOOS {
	case "windows":
		edgePaths := []string{
			`C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe`,
			`C:\Program Files\Microsoft\Edge\Application\msedge.exe`,
		}
		launched := false
		for _, p := range edgePaths {
			if _, err := os.Stat(p); err == nil {
				if exec.Command(p, "--app="+url, "--window-size=1120,720").Start() == nil {
					launched = true
					break
				}
			}
		}
		if !launched {
			_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
		}
	case "darwin":
		_ = exec.Command("open", url).Start()
	default:
		_ = exec.Command("xdg-open", url).Start()
	}

	<-ctx.Done()
}
