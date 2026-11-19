package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dota2-analytics/desktop-worker/internal/client"
	"github.com/dota2-analytics/desktop-worker/internal/config"
	"github.com/dota2-analytics/desktop-worker/internal/downloader"
	"github.com/dota2-analytics/desktop-worker/internal/headless"
	"github.com/dota2-analytics/desktop-worker/internal/orchestrator"
	"github.com/dota2-analytics/desktop-worker/internal/sentinel"
)

func main() {
	flags, err := headless.ParseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		log.Fatalf("Invalid CLI flags: %v", err)
	}

	cfgMgr := config.NewManager("")
	if envURL := os.Getenv("CENTRAL_SERVER_URL"); envURL != "" && !flags.Set["server"] {
		cfgMgr.UpdateEphemeral(func(c *config.WorkerConfig) {
			c.ServerURL = envURL
		})
	}

	sent := sentinel.NewSentinel(cfgMgr)
	apiClient := client.NewAPIClient()
	dl := downloader.NewDownloader()
	orch := orchestrator.NewOrchestrator(cfgMgr, sent, apiClient, dl)

	if err := headless.ApplyStartupFlags(flags, cfgMgr, orch); err != nil {
		log.Fatalf("Failed to apply startup CLI flags: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	headless.RunWithFlags(ctx, os.Stdin, os.Stdout, stop, cfgMgr, sent, orch, flags)
}
