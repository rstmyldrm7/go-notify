package domain

import (
	"time"

	"github.com/google/uuid"
)

// Channel is the delivery medium of a notification.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Priority determines which queue a notification is published to and how
// eagerly workers consume it.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Status is the lifecycle state of a notification. The database row is the
// single source of truth; Kafka messages only carry work, never state.
//
//	pending ──▶ queued ──▶ processing ──▶ sent
//	   │                        │
//	   │                        └──▶ failed ──▶ (queued again)  or  ──▶ dead
//	   └──▶ cancelled
//	scheduled ──▶ queued (dispatched by the scheduler when due)
type Status string

const (
	StatusPending    Status = "pending"    // accepted by the API, not yet published
	StatusQueued     Status = "queued"     // published to Kafka, waiting for a worker
	StatusProcessing Status = "processing" // claimed by a worker, delivery in flight
	StatusSent       Status = "sent"       // provider accepted the message
	StatusFailed     Status = "failed"     // transient failure, waiting for retry
	StatusDead       Status = "dead"       // retries exhausted or permanent failure (DLQ)
	StatusCancelled  Status = "cancelled"  // cancelled by the client before delivery
	StatusScheduled  Status = "scheduled"  // future-dated, waiting for the scheduler
)

// Notification is the core entity, mapping 1:1 to the notifications table.
type Notification struct {
	ID             uuid.UUID  `json:"id"`
	BatchID        *uuid.UUID `json:"batch_id,omitempty"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`

	Recipient string   `json:"recipient"`
	Channel   Channel  `json:"channel"`
	Content   string   `json:"content"`
	Priority  Priority `json:"priority"`
	Status    Status   `json:"status"`

	AttemptCount int        `json:"attempt_count"`
	NextRetryAt  *time.Time `json:"next_retry_at,omitempty"`
	ScheduledAt  *time.Time `json:"scheduled_at,omitempty"`
	LastError    *string    `json:"last_error,omitempty"`

	ProviderMessageID *string `json:"provider_message_id,omitempty"`

	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	SentAt    *time.Time `json:"sent_at,omitempty"`
}

// CanBeCancelled reports whether the notification is still in a state where
// cancellation is allowed. Once a worker may have picked it up, it is too
// late: the actual guard is the conditional UPDATE in the storage layer; this
// is only used for friendly error messages.
func (n *Notification) CanBeCancelled() bool {
	return n.Status == StatusPending || n.Status == StatusQueued || n.Status == StatusScheduled
}
