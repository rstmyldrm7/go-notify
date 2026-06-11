package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/rstmyldrm7/go-notify/internal/ctxutil"
	"github.com/rstmyldrm7/go-notify/internal/domain"
)

// Kafka has no native priority support, so each priority level gets its own
// topic and workers drain high before normal before low.
const (
	TopicHigh   = "notifications.high"
	TopicNormal = "notifications.normal"
	TopicLow    = "notifications.low"
)

func TopicForPriority(p domain.Priority) string {
	switch p {
	case domain.PriorityHigh:
		return TopicHigh
	case domain.PriorityLow:
		return TopicLow
	default:
		return TopicNormal
	}
}

const correlationIDHeader = "correlation_id"

// NotificationMessage is the wire format. It is self-contained: the worker
// delivers straight from the payload and never has to SELECT the row back.
type NotificationMessage struct {
	ID        uuid.UUID       `json:"id"`
	Recipient string          `json:"recipient"`
	Channel   domain.Channel  `json:"channel"`
	Content   string          `json:"content"`
	Priority  domain.Priority `json:"priority"`
	CreatedAt time.Time       `json:"created_at"`
}

// KafkaPublisher writes notifications to the priority topics.
type KafkaPublisher struct {
	writer *kafka.Writer
	log    *slog.Logger
}

func NewKafkaPublisher(brokers []string, log *slog.Logger) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr: kafka.TCP(brokers...),
			// Hash by message key (recipient) so one recipient's messages
			// stay ordered within a partition.
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			// Sync writes flush after BatchTimeout; the 1s default would be
			// added to every API request latency.
			BatchTimeout:           10 * time.Millisecond,
			AllowAutoTopicCreation: false,
		},
		log: log,
	}
}

func (p *KafkaPublisher) PublishNotifications(ctx context.Context, ns []*domain.Notification) error {
	if len(ns) == 0 {
		return nil
	}

	headers := []kafka.Header{}
	if cid := ctxutil.CorrelationID(ctx); cid != "" {
		headers = append(headers, kafka.Header{Key: correlationIDHeader, Value: []byte(cid)})
	}

	msgs := make([]kafka.Message, len(ns))
	for i, n := range ns {
		value, err := json.Marshal(NotificationMessage{
			ID:        n.ID,
			Recipient: n.Recipient,
			Channel:   n.Channel,
			Content:   n.Content,
			Priority:  n.Priority,
			CreatedAt: n.CreatedAt,
		})
		if err != nil {
			return fmt.Errorf("marshal notification %s: %w", n.ID, err)
		}
		msgs[i] = kafka.Message{
			Topic:   TopicForPriority(n.Priority),
			Key:     []byte(n.Recipient),
			Value:   value,
			Headers: headers,
		}
	}

	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("write to kafka: %w", err)
	}
	p.log.DebugContext(ctx, "published to kafka", "count", len(msgs))
	return nil
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
