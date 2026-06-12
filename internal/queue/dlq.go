package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/rstmyldrm7/go-notify/internal/domain"
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
	value, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal dlq envelope: %w", err)
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic: DLQTopic(channel),
		Key:   []byte(env.Original.Recipient),
		Value: value,
	}); err != nil {
		return fmt.Errorf("write to dlq: %w", err)
	}
	return nil
}

func (p *DLQProducer) Close() error {
	return p.writer.Close()
}
