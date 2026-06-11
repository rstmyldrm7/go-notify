package queue

import (
	"context"
	"log/slog"

	"github.com/rstmyldrm7/go-notify/internal/domain"
)

// Publisher abstracts the message queue from the API service. Accepting a
// slice lets the batch endpoint publish 1000 notifications in one round trip.
type Publisher interface {
	PublishNotifications(ctx context.Context, ns []*domain.Notification) error
}

// NoopPublisher accepts everything and delivers nothing. Used in tests and
// when running the API without a broker.
type NoopPublisher struct {
	Log *slog.Logger
}

func (p *NoopPublisher) PublishNotifications(ctx context.Context, ns []*domain.Notification) error {
	p.Log.DebugContext(ctx, "noop publisher: pretending to publish", "count", len(ns))
	return nil
}
