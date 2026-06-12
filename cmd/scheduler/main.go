package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rstmyldrm7/go-notify/internal/config"
	"github.com/rstmyldrm7/go-notify/internal/metrics"
	"github.com/rstmyldrm7/go-notify/internal/observ"
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

	shutdownTracer, err := observ.InitTracer(ctx, "notify-scheduler", cfg.OTLPEndpoint, log)
	if err != nil {
		return err
	}
	defer observ.FlushOnShutdown(shutdownTracer)

	pool, err := storage.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	repo := storage.NewRepository(pool)

	publisher := queue.NewKafkaPublisher(cfg.KafkaBrokers, log)
	defer publisher.Close()

	// Safety invariant: the reaper must never reclaim a row that could still be
	// in flight, or it would double-send. Floor inflightAfter at the worker's
	// worst-case delivery time (timeout × attempts, plus margin) regardless of
	// how REAPER_INFLIGHT_AFTER was set.
	worstCase := cfg.ProviderTimeout*time.Duration(cfg.RetryMaxAttempts) + 30*time.Second
	inflightAfter := cfg.ReaperInflightAfter
	if inflightAfter < worstCase {
		log.Warn("reaper inflight_after below worst-case delivery time, raising it",
			"configured", inflightAfter.String(), "worst_case", worstCase.String())
		inflightAfter = worstCase
	}

	// Observability endpoint: /metrics for Prometheus, /healthz for liveness.
	metricsSrv := startMetricsServer(cfg.SchedulerMetricsAddr, repo, log)

	sched := scheduler.New(repo, publisher, cfg.SchedulerInterval, cfg.SchedulerBatchSize, log)
	reaper := scheduler.NewReaper(repo, publisher,
		cfg.ReaperInterval, cfg.ReaperPendingAfter, inflightAfter, cfg.ReaperBatchSize, log)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); sched.Run(ctx) }()
	go func() { defer wg.Done(); reaper.Run(ctx) }()

	log.Info("scheduler service started", "metrics_addr", cfg.SchedulerMetricsAddr)
	wg.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	log.Info("scheduler service stopped")
	return nil
}

func startMetricsServer(addr string, repo *storage.Repository, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := repo.Ping(r.Context()); err != nil {
			http.Error(w, `{"status":"degraded","database":"unreachable"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "error", err)
		}
	}()
	return srv
}
