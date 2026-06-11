package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/rstmyldrm7/go-notify/internal/api"
	"github.com/rstmyldrm7/go-notify/internal/config"
	"github.com/rstmyldrm7/go-notify/internal/queue"
	"github.com/rstmyldrm7/go-notify/internal/storage"
)

func main() {
	cfg := config.Load()

	logLevel := slog.LevelDebug
	if cfg.Env == "production" {
		logLevel = slog.LevelInfo
		gin.SetMode(gin.ReleaseMode)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(log)

	if err := run(cfg, log); err != nil {
		log.Error("api service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.MigrateOnStart {
		if err := storage.RunMigrations(cfg.DatabaseURL); err != nil {
			return err
		}
		log.Info("migrations applied")
	}

	pool, err := storage.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	publisher := queue.NewKafkaPublisher(cfg.KafkaBrokers, log)
	defer publisher.Close()

	handler := &api.Handler{
		Repo:      storage.NewRepository(pool),
		Publisher: publisher,
		Log:       log,
	}

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: api.NewRouter(handler, log),
		// Guard against slow/stalled clients (e.g. slowloris) holding
		// connections open and exhausting the server.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("api service listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
