package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/rstmyldrm7/go-notify/internal/domain"
	"github.com/rstmyldrm7/go-notify/internal/observ"
)

// DLQEnvelope wraps a notification that could not be delivered together with
// the failure context, so an operator can inspect it or a replay job can
// re-publish the original message later.
type DLQEnvelope struct {
	Original      NotificationMessage `json:"original"`
	Error         string              `json:"error"`
	Attempts      int                 `json:"attempts"`
	FailedAt      time.Time           `json:"failed_at"`
	CorrelationID string              `json:"correlation_id,omitempty"`
}

// DLQProducer publishes failed messages to the per-channel dead-letter topics.
// It is separate from KafkaPublisher because it writes envelopes, not raw
// notifications, and is owned by the worker rather than the API.
type DLQProducer struct {
	writer *kafka.Writer
}

func NewDLQProducer(brokers []string) *DLQProducer {
	return &DLQProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			BatchTimeout:           10 * time.Millisecond,
			AllowAutoTopicCreation: false,
		},
	}
}

// Publish writes env to the dead-letter topic of channel, e.g. "sms.dlq".
func (p *DLQProducer) Publish(ctx context.Context, channel domain.Channel, env DLQEnvelope) error {
	ctx, span := otel.Tracer(observ.Tracer).Start(ctx, "kafka.dlq.publish",
		trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	value, err := json.Marshal(env)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshal dlq envelope: %w", err)
	}
	headers := []kafka.Header{}
	injectTrace(ctx, &headers)
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   DLQTopic(channel),
		Key:     []byte(env.Original.Recipient),
		Value:   value,
		Headers: headers,
	}); err != nil {
		span.RecordError(err)
		return fmt.Errorf("write to dlq: %w", err)
	}
	return nil
}

func (p *DLQProducer) Close() error {
	return p.writer.Close()
}
