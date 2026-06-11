// Package ctxutil holds context helpers shared across service boundaries,
// so the API middleware and the queue layer agree on the same context keys.
package ctxutil

import "context"

type ctxKey string

const correlationIDKey ctxKey = "correlation_id"

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey).(string)
	return id
}
