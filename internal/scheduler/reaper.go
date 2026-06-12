package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/rstmyldrm7/go-notify/internal/domain"
	"github.com/rstmyldrm7/go-notify/internal/metrics"
	"github.com/rstmyldrm7/go-notify/internal/queue"
	"github.com/rstmyldrm7/go-notify/internal/storage"
)

// Reaper is the reconciliation poller and the system's safety net. The database
// is the source of truth for delivery; the Reaper periodically re-dispatches
// notifications that an edge case left stranded in a non-terminal state — an API
// publish failure, a message lost to an out-of-order Kafka commit, or a worker
// that crashed mid-delivery.
//
// It is meant to stay near-idle. A sustained reap rate (the
// notify_reaper_reaped_total metric) is a signal to fix the upstream cause, not
// to let the reaper carry load.
type Reaper struct {
	repo          *storage.Repository
	publisher     queue.Publisher
	interval      time.Duration
	pendingAfter  time.Duration
	inflightAfter time.Duration
	batchSize     int
	log           *slog.Logger
}

func NewReaper(
	repo *storage.Repository,
	publisher queue.Publisher,
	interval, pendingAfter, inflightAfter time.Duration,
	batchSize int,
	log *slog.Logger,
) *Reaper {
	return &Reaper{
		repo:          repo,
		publisher:     publisher,
		interval:      interval,
		pendingAfter:  pendingAfter,
		inflightAfter: inflightAfter,
		batchSize:     batchSize,
		log:           log,
	}
}

// Run polls until ctx is cancelled.
func (rp *Reaper) Run(ctx context.Context) {
	t := time.NewTicker(rp.interval)
	defer t.Stop()

	rp.log.Info("reaper started",
		"interval", rp.interval.String(),
		"pending_after", rp.pendingAfter.String(),
		"inflight_after", rp.inflightAfter.String())

	for {
		select {
		case <-ctx.Done():
			rp.log.Info("reaper stopped")
			return
		case <-t.C:
			rp.tick(ctx)
		}
	}
}

// tick reclaims all currently-stuck notifications, draining in batches so a
// large backlog (e.g. a long outage) is cleared without one oversized tx.
func (rp *Reaper) tick(ctx context.Context) {
	for {
		stuck, err := rp.repo.ReapStuck(ctx, time.Now(),
			rp.pendingAfter, rp.inflightAfter, rp.batchSize, rp.publisher.PublishNotifications)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down: the cancelled context, not a real failure
			}
			rp.log.ErrorContext(ctx, "reap failed", "error", err)
			return
		}

		n := len(stuck)
		if n > 0 {
			byStatus := make(map[domain.Status]int, 3)
			for _, s := range stuck {
				byStatus[s.Status]++
				metrics.Reaped.WithLabelValues(string(s.Status)).Inc()
			}
			// WARN: every reap means something upstream stranded these rows.
			rp.log.WarnContext(ctx, "reaped stuck notifications",
				"count", n,
				"pending", byStatus[domain.StatusPending],
				"queued", byStatus[domain.StatusQueued],
				"processing", byStatus[domain.StatusProcessing])
		}
		if n < rp.batchSize {
			return // fewer than a full batch means we have caught up
		}
	}
}
