package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rstmyldrm7/go-notify/internal/config"
	"github.com/rstmyldrm7/go-notify/internal/metrics"
	"github.com/rstmyldrm7/go-notify/internal/queue"
	"github.com/rstmyldrm7/go-notify/internal/scheduler"
	"github.com/rstmyldrm7/go-notify/internal/storage"
)

func main() {
	cfg := config.Load()

	logLevel := slog.LevelDebug
	if cfg.Env == "production" {
		logLevel = slog.LevelInfo
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(log)

	if err := run(cfg, log); err != nil {
		log.Error("scheduler service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := storage.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	repo := storage.NewRepository(pool)

	publisher := queue.NewKafkaPublisher(cfg.KafkaBrokers, log)
	defer publisher.Close()

	// Observability endpoint: /metrics for Prometheus, /healthz for liveness.
	obsSrv := metrics.StartObservabilityServer(cfg.SchedulerMetricsAddr, repo.Ping, log)

	log.Info("scheduler service started", "metrics_addr", cfg.SchedulerMetricsAddr)
	scheduler.New(repo, publisher, cfg.SchedulerInterval, cfg.SchedulerBatchSize, log).Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = obsSrv.Shutdown(shutdownCtx)
	log.Info("scheduler service stopped")
	return nil
}
