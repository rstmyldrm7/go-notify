package queue

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// headerCarrier adapts a Kafka message's headers to the OpenTelemetry
// TextMapCarrier interface so the W3C trace context can ride across the broker
// as ordinary message headers. Kafka allows duplicate header keys, so Set
// overwrites any existing entry to keep a single canonical value.
type headerCarrier struct {
	headers *[]kafka.Header
}

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// injectTrace writes the active trace context from ctx into headers so the
// consumer can resume the same trace. It is a no-op when no span is recording.
func injectTrace(ctx context.Context, headers *[]kafka.Header) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: headers})
}

// ExtractTrace returns a context carrying the trace context propagated in msg's
// headers, so the worker can start its processing span as a child of the
// API/scheduler span that produced the message.
func ExtractTrace(ctx context.Context, msg kafka.Message) context.Context {
	h := msg.Headers
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &h})
}

var _ propagation.TextMapCarrier = headerCarrier{}
