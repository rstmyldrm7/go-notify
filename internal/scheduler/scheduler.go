package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/rstmyldrm7/go-notify/internal/queue"
	"github.com/rstmyldrm7/go-notify/internal/storage"
)

// Scheduler is the background poller that turns future-dated notifications into
// work. On a fixed interval it claims notifications whose scheduled_at has
// passed, publishes them to Kafka and flips their status to 'queued' — the same
// transition the API performs for immediate notifications.
type Scheduler struct {
	repo      *storage.Repository
	publisher queue.Publisher
	interval  time.Duration
	batchSize int
	log       *slog.Logger
}

func New(repo *storage.Repository, publisher queue.Publisher, interval time.Duration, batchSize int, log *slog.Logger) *Scheduler {
	return &Scheduler{
		repo:      repo,
		publisher: publisher,
		interval:  interval,
		batchSize: batchSize,
		log:       log,
	}
}

// Run polls until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()

	s.log.Info("scheduler started",
		"interval", s.interval.String(), "batch_size", s.batchSize)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopped")
			return
		case <-t.C:
			s.tick(ctx)
		}
	}
}

// tick dispatches all currently-due notifications, draining in batches so a
// backlog (e.g. many notifications scheduled for the same minute, or a restart
// after downtime) is cleared without holding one oversized transaction.
func (s *Scheduler) tick(ctx context.Context) {
	for {
		n, err := s.repo.DispatchDue(ctx, time.Now(), s.batchSize, s.publisher.PublishNotifications)
		if err != nil {
			s.log.ErrorContext(ctx, "dispatch due failed", "error", err)
			return
		}
		if n > 0 {
			s.log.InfoContext(ctx, "dispatched scheduled notifications", "count", n)
		}
		if n < s.batchSize {
			return // fewer than a full batch means we have caught up
		}
	}
}
