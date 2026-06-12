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

// Kafka has no native priority support and each channel must be isolated, so
// the topic name encodes both dimensions: "<channel>.<priority>", e.g.
// "sms.high". This gives the 9-topic channel × priority matrix. Each channel
// also owns a dead-letter topic, "<channel>.dlq".
//
// Workers run one pool per channel and, within a pool, drain high before
// normal before low.
func TopicFor(channel domain.Channel, priority domain.Priority) string {
	return string(channel) + "." + priorityName(priority)
}

// DLQTopic is the dead-letter topic for a channel, e.g. "sms.dlq".
func DLQTopic(channel domain.Channel) string {
	return string(channel) + ".dlq"
}

func priorityName(p domain.Priority) string {
	switch p {
	case domain.PriorityHigh:
		return "high"
	case domain.PriorityLow:
		return "low"
	default:
		return "normal"
	}
}

// AllTopics enumerates every channel × priority work topic plus the per-channel
// DLQ topics. Used to provision Kafka and by operational tooling.
func AllTopics() []string {
	topics := make([]string, 0, len(domain.AllChannels)*(len(domain.AllPriorities)+1))
	for _, c := range domain.AllChannels {
		for _, p := range domain.AllPriorities {
			topics = append(topics, TopicFor(c, p))
		}
		topics = append(topics, DLQTopic(c))
	}
	return topics
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
			Topic:   TopicFor(n.Channel, n.Priority),
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
