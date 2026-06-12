package queue

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer reads from a single topic as part of a consumer group with
// auto-commit disabled: offsets advance only when the caller explicitly
// commits a message, after it has been fully processed or offloaded to the
// DLQ. This is what lets the worker guarantee at-least-once delivery and avoid
// losing messages on crash.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer joins groupID and starts reading topic. Multiple consumers in the
// same group may read different topics concurrently (the worker uses one per
// priority topic of a channel).
func NewConsumer(brokers []string, groupID, topic string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 1,
			MaxBytes: 10 << 20, // 10 MiB
			MaxWait:  500 * time.Millisecond,
			// CommitInterval 0 keeps commits explicit and synchronous: nothing
			// is committed until Commit is called.
			CommitInterval: 0,
		}),
	}
}

// Fetch returns the next message without committing its offset. It blocks until
// a message is available or ctx is cancelled.
func (c *Consumer) Fetch(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// Commit advances the committed offset past msg. Called only after the message
// has been delivered or routed to the DLQ, so a crash mid-processing replays
// the message rather than dropping it.
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

// Close stops the reader and unblocks any in-flight Fetch.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// HeaderValue returns the value of the first header with the given key, or "".
func HeaderValue(msg kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// CorrelationID extracts the correlation id propagated by the API, if present.
func CorrelationID(msg kafka.Message) string {
	return HeaderValue(msg, correlationIDHeader)
}
