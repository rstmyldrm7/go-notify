package api

import (
	"time"

	"github.com/google/uuid"

	"github.com/rstmyldrm7/go-notify/internal/domain"
)

type createNotificationRequest struct {
	Recipient   string     `json:"recipient"`
	Channel     string     `json:"channel"`
	Content     string     `json:"content"`
	Priority    string     `json:"priority"`
	ScheduledAt *time.Time `json:"scheduled_at"`
}

type batchItemRequest struct {
	createNotificationRequest
	IdempotencyKey *string `json:"idempotency_key"`
}

type createBatchRequest struct {
	Notifications []batchItemRequest `json:"notifications"`
}

const maxBatchSize = 1000

type batchItemResult struct {
	Index        int                  `json:"index"`
	Duplicate    bool                 `json:"duplicate"`
	Notification *domain.Notification `json:"notification"`
}

type createBatchResponse struct {
	BatchID    uuid.UUID         `json:"batch_id"`
	Total      int               `json:"total"`
	Created    int               `json:"created"`
	Duplicates int               `json:"duplicates"`
	Items      []batchItemResult `json:"items"`
}

type listResponse struct {
	Data    []*domain.Notification `json:"data"`
	Page    int                    `json:"page"`
	Limit   int                    `json:"limit"`
	HasMore bool                   `json:"has_more"`
}

type batchSummaryResponse struct {
	BatchID uuid.UUID             `json:"batch_id"`
	Total   int                   `json:"total"`
	Counts  map[domain.Status]int `json:"counts"`
}

type errorResponse struct {
	Error   string                  `json:"error"`
	Details domain.ValidationErrors `json:"details,omitempty"`
}
