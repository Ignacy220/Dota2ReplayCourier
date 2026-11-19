package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dota2-analytics/central-server/internal/accounts"
	"github.com/dota2-analytics/central-server/internal/api"
	"github.com/dota2-analytics/central-server/internal/cli"
	"github.com/dota2-analytics/central-server/internal/harvester"
	"github.com/dota2-analytics/central-server/internal/parsers"
	"github.com/dota2-analytics/central-server/internal/queue"
	"github.com/dota2-analytics/central-server/internal/resolver"
	"github.com/dota2-analytics/central-server/internal/storage"
	"github.com/redis/go-redis/v9"
)

func main() {
	cliFlags, err := cli.ParseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		log.Fatalf("Invalid CLI flags: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fatalf := func(format string, args ...any) {
		log.SetOutput(os.Stderr)
		log.Fatalf(format, args...)
	}
	if cliFlags.Headless {
		log.SetOutput(io.Discard)
	}

	port := getEnv("PORT", "8080")
	if cliFlags.Set["port"] && cliFlags.Port != "" {
		port = cliFlags.Port
	}
	redisAddr := getEnv("REDIS_ADDR", "")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	storageDir := getEnv("STORAGE_DIR", filepath.Join("data", "matches"))
	accountsDBPath := getEnv("ACCOUNTS_DB_PATH", filepath.Join("data", "accounts.db"))
	parsersDir := getEnv("PARSERS_DIR", filepath.Join("data", "parsers"))
	webhookURL := getEnv("EXTERNAL_WEBHOOK_URL", "")
	steamAPIKey := getEnv("STEAM_API_KEY", "")
	stratzToken := getEnv("STRATZ_API_TOKEN", "")
	if stratzToken == "" {
		stratzToken = getEnv("STRATZ_API_KEY", "")
	}
	openDotaKey := getEnv("OPENDOTA_API_KEY", "")
	apiKeysPath := filepath.Join(filepath.Dir(accountsDBPath), "api_keys.json")
	if loadedSteam, loadedStratz, loadedOpenDota, ok := api.LoadPersistedAPIKeys(apiKeysPath); ok {
		if loadedSteam != "" {
			steamAPIKey = loadedSteam
		}
		if loadedStratz != "" {
			stratzToken = loadedStratz
		}
		if loadedOpenDota != "" {
			openDotaKey = loadedOpenDota
		}
	}

	if steamAPIKey != "" {
		log.Printf("[Bootstrap] Loaded Steam Web API key (length=%d)", len(steamAPIKey))
	} else {
		log.Printf("[Bootstrap] No Steam Web API key configured — using OpenDota publicMatches fallback (configure in /admin)")
	}

	var rdb *redis.Client
	if redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: redisPassword,
		})
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := rdb.Ping(pingCtx).Err()
		cancel()
		if err != nil {
			log.Printf("[Bootstrap] External Redis at %s not reachable (%v), using embedded Redis.", redisAddr, err)
			rdb = nil
		}
	}

	if rdb == nil {
		mr, err := miniredis.Run()
		if err != nil {
			fatalf("Failed to start embedded Redis server: %v", err)
		}
		defer mr.Close()
		rdb = redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})
	}

	leaseMgr := queue.NewLeaseManager(rdb, 30*time.Minute)

	dataDir := filepath.Dir(accountsDBPath)
	initialFilterCfg, ok := api.LoadPersistedFilterConfig(filepath.Join(dataDir, "filter_config.json"))
	if !ok {
		initialFilterCfg, _ = leaseMgr.LoadFilterConfig(ctx)
	}
	initialFilterCfg.Enabled = false
	filterEngine := harvester.NewFilterEngine(initialFilterCfg)

	saltResolver := resolver.NewMultiResolver(stratzToken, openDotaKey)
	harv := harvester.NewHarvester(steamAPIKey, filterEngine, leaseMgr, saltResolver)
	harv.Start(ctx)

	store := storage.NewAnalyticsStore(storageDir, webhookURL)
	if loadedWebhookURL, loadedWebhookEnabled, loadedFileStorageEnabled, ok := api.LoadPersistedSinkConfig(filepath.Join(dataDir, "sink_config.json")); ok {
		store.UpdateSinkConfig(loadedWebhookURL, loadedWebhookEnabled)
		store.SetFileStorageEnabled(loadedFileStorageEnabled)
	}
	store.Start(ctx)

	accStore, err := accounts.NewStore(accountsDBPath)
	if err != nil {
		fatalf("Failed to initialize SQLite accounts database (%s): %v", accountsDBPath, err)
	}
	defer accStore.Close()
	log.Printf("[Storage] SQLite accounts database active at %s", accountsDBPath)

	parserMgr, err := parsers.NewManager(parsersDir)
	if err != nil {
		fatalf("Failed to initialize parser store (%s): %v", parsersDir, err)
	}
	log.Printf("[Parser] Active parser configured as %s (rev=%d)", parserMgr.GetActive().ReportedName, parserMgr.GetActive().Revision)

	srv := api.NewServer(leaseMgr, filterEngine, harv, store, accStore, parserMgr, "")
	srv.SetAPIKeysPath(apiKeysPath)
	srv.EnableAdminPasswordAuth(filepath.Join(dataDir, "admin_auth.json"))

	if err := cli.ApplyStartupFlags(cliFlags, filterEngine, harv, store, parserMgr, srv); err != nil {
		fatalf("Failed to apply startup CLI flags: %v", err)
	}
	if !cliFlags.Headless && !srv.HasAdminPassword() {
		log.Printf("[Security] Admin Console locked — run 'password' in this terminal to set the admin password")
	}

	if cliFlags.Headless {
		log.SetOutput(os.Stderr)
	}

	baseURL := "http://localhost:" + port
	go cli.RunInteractiveLoop(ctx, os.Stdin, os.Stdout, stop, cli.Dependencies{
		FilterEngine: filterEngine,
		Harvester:    harv,
		Store:        store,
		LeaseMgr:     leaseMgr,
		ParserMgr:    parserMgr,
		Server:       srv,
		DataDir:      dataDir,
		BaseURL:      baseURL,
		Headless:     cliFlags.Headless,
	})

	go func() {
		if !cliFlags.Headless {
			log.Printf("Dota2ReplayCourier Coordinator listening on %s", baseURL)
			log.Printf("Admin Console:       %s/admin (type 'help' for terminal CLI commands)", baseURL)
			log.Printf("Match File Storage:  %s/<match_id>.json", storageDir)
			log.Printf("Live Match Stream:   %s/api/v1/stream/matches", baseURL)
		}
		if err := srv.Listen(":" + port); err != nil {
			log.Printf("Server stopped: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down Coordinator...")
	_ = srv.App().Shutdown()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
