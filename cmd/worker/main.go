package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rstmyldrm7/go-notify/internal/config"
	"github.com/rstmyldrm7/go-notify/internal/domain"
	"github.com/rstmyldrm7/go-notify/internal/metrics"
	"github.com/rstmyldrm7/go-notify/internal/provider"
	"github.com/rstmyldrm7/go-notify/internal/queue"
	"github.com/rstmyldrm7/go-notify/internal/storage"
	"github.com/rstmyldrm7/go-notify/internal/worker"
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
		log.Error("worker service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.ProviderURL == "" {
		return errors.New("PROVIDER_URL is required (set it to your webhook.site URL)")
	}

	pool, err := storage.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	repo := storage.NewRepository(pool)

	prov := provider.New(cfg.ProviderURL, cfg.ProviderTimeout)

	dlq := queue.NewDLQProducer(cfg.KafkaBrokers)
	defer dlq.Close()

	deps := worker.Deps{
		Brokers:     cfg.KafkaBrokers,
		GroupPrefix: cfg.ConsumerGroupPrefix,
		Provider:    prov,
		Repo:        repo,
		DLQ:         dlq,
		Log:         log,
		Senders:     cfg.SenderConcurrency,
		BufferSize:  cfg.QueueBufferSize,
		RatePerSec:  cfg.RateLimitPerSec,
		RateBurst:   cfg.RateLimitBurst,
		MaxAttempts: cfg.RetryMaxAttempts,
		Backoff:     cfg.RetryBackoff,
	}

	// Observability endpoint: /metrics for Prometheus, /healthz for liveness.
	metricsSrv := metrics.StartObservabilityServer(cfg.MetricsAddr, repo.Ping, log)

	// One isolated pool per channel, all running until shutdown.
	var wg sync.WaitGroup
	for _, ch := range domain.AllChannels {
		p := worker.NewPool(ch, deps)
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Run(ctx)
		}()
	}
	log.Info("worker service started",
		"channels", domain.AllChannels, "metrics_addr", cfg.MetricsAddr)

	wg.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	log.Info("worker service stopped")
	return nil
}
