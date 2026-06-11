package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Per-channel content limits (characters). SMS follows the single-segment
// GSM-7 limit; email and push are pragmatic caps to protect the pipeline.
var contentLimits = map[Channel]int{
	ChannelSMS:   160,
	ChannelPush:  512,
	ChannelEmail: 10000,
}

const maxRecipientLength = 320 // longest valid email address

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationErrors carries every failed rule so API consumers can fix a
// request in one round trip instead of discovering errors one by one.
type ValidationErrors []FieldError

func (v ValidationErrors) Error() string {
	parts := make([]string, len(v))
	for i, fe := range v {
		parts[i] = fe.Field + ": " + fe.Message
	}
	return strings.Join(parts, "; ")
}

// NewNotificationParams is the validated input for creating a notification.
type NewNotificationParams struct {
	Recipient      string
	Channel        Channel
	Content        string
	Priority       Priority   // empty defaults to normal
	ScheduledAt    *time.Time // future time switches initial status to scheduled
	IdempotencyKey *string
	BatchID        *uuid.UUID
}

// NewNotification validates params and builds a Notification ready to be
// persisted. It is the single place where creation rules live, shared by the
// single and batch endpoints.
func NewNotification(p NewNotificationParams, now time.Time) (*Notification, error) {
	var errs ValidationErrors

	if strings.TrimSpace(p.Recipient) == "" {
		errs = append(errs, FieldError{"recipient", "is required"})
	} else if len(p.Recipient) > maxRecipientLength {
		errs = append(errs, FieldError{"recipient", fmt.Sprintf("must be at most %d characters", maxRecipientLength)})
	}

	limit, validChannel := contentLimits[p.Channel]
	if !validChannel {
		errs = append(errs, FieldError{"channel", "must be one of: sms, email, push"})
	}

	if strings.TrimSpace(p.Content) == "" {
		errs = append(errs, FieldError{"content", "is required"})
	} else if validChannel && len([]rune(p.Content)) > limit {
		errs = append(errs, FieldError{"content", fmt.Sprintf("exceeds %d character limit for channel %s", limit, p.Channel)})
	}

	if p.Priority == "" {
		p.Priority = PriorityNormal
	}
	switch p.Priority {
	case PriorityHigh, PriorityNormal, PriorityLow:
	default:
		errs = append(errs, FieldError{"priority", "must be one of: high, normal, low"})
	}

	if p.IdempotencyKey != nil && strings.TrimSpace(*p.IdempotencyKey) == "" {
		errs = append(errs, FieldError{"idempotency_key", "must not be blank when provided"})
	}

	status := StatusPending
	if p.ScheduledAt != nil {
		if !p.ScheduledAt.After(now) {
			errs = append(errs, FieldError{"scheduled_at", "must be in the future"})
		}
		status = StatusScheduled
	}

	if len(errs) > 0 {
		return nil, errs
	}

	return &Notification{
		ID:             uuid.New(),
		BatchID:        p.BatchID,
		IdempotencyKey: p.IdempotencyKey,
		Recipient:      p.Recipient,
		Channel:        p.Channel,
		Content:        p.Content,
		Priority:       p.Priority,
		Status:         status,
		ScheduledAt:    p.ScheduledAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// SamePayload reports whether another request carries the same business
// content. Used to detect idempotency key reuse with a different payload.
func (n *Notification) SamePayload(recipient string, channel Channel, content string) bool {
	return n.Recipient == recipient && n.Channel == channel && n.Content == content
}
