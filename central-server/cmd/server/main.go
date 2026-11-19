package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dota2-analytics/central-server/internal/accounts"
	"github.com/dota2-analytics/central-server/internal/api"
	"github.com/dota2-analytics/central-server/internal/harvester"
	"github.com/dota2-analytics/central-server/internal/parsers"
	"github.com/dota2-analytics/central-server/internal/queue"
	"github.com/dota2-analytics/central-server/internal/resolver"
	"github.com/dota2-analytics/central-server/internal/storage"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	port := getEnv("PORT", "8080")
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
			log.Fatalf("Failed to start embedded Redis server: %v", err)
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
		log.Fatalf("Failed to initialize SQLite accounts database (%s): %v", accountsDBPath, err)
	}
	defer accStore.Close()
	log.Printf("[Storage] SQLite accounts database active at %s", accountsDBPath)

	parserMgr, err := parsers.NewManager(parsersDir)
	if err != nil {
		log.Fatalf("Failed to initialize parser store (%s): %v", parsersDir, err)
	}
	log.Printf("[Parser] Active parser configured as %s (rev=%d)", parserMgr.GetActive().ReportedName, parserMgr.GetActive().Revision)

	srv := api.NewServer(leaseMgr, filterEngine, harv, store, accStore, parserMgr, "")
	srv.SetAPIKeysPath(apiKeysPath)

	go func() {
		log.Printf("Dota2ReplayCourier Coordinator listening on http://localhost:%s", port)
		log.Printf("Admin Console:       http://localhost:%s/admin", port)
		log.Printf("Match File Storage:  %s/<match_id>.json", storageDir)
		log.Printf("Live Match Stream:   http://localhost:%s/api/v1/stream/matches", port)
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
