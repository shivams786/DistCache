package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/codex/distcache/internal/app"
	"github.com/codex/distcache/internal/config"
	"github.com/codex/distcache/internal/logging"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.NodeID)
	service, err := app.New(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize node", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("node stopped with error", "error", err)
		os.Exit(1)
	}
}
