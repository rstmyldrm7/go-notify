package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/rstmyldrm7/go-notify/internal/ctxutil"
	"github.com/rstmyldrm7/go-notify/internal/metrics"
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
		// One correlation id per dispatch run, propagated into the published
		// Kafka headers so a scheduled delivery is traceable end to end.
		runCtx := ctxutil.WithCorrelationID(ctx, uuid.NewString())

		dispatched, err := s.repo.DispatchDue(runCtx, time.Now(), s.batchSize, s.publisher.PublishNotifications)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down: the cancelled context, not a real failure
			}
			s.log.ErrorContext(ctx, "dispatch due failed", "error", err)
			metrics.SchedulerDispatchErrors.Inc()
			return
		}

		n := len(dispatched)
		if n > 0 {
			now := time.Now()
			for _, notif := range dispatched {
				if notif.ScheduledAt != nil {
					metrics.SchedulingLag.Observe(now.Sub(*notif.ScheduledAt).Seconds())
				}
			}
			metrics.SchedulerDispatched.Add(float64(n))
			s.log.InfoContext(runCtx, "dispatched scheduled notifications",
				"count", n, "correlation_id", ctxutil.CorrelationID(runCtx))
		}
		if n < s.batchSize {
			return // fewer than a full batch means we have caught up
		}
	}
}
