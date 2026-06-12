package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/rstmyldrm7/go-notify/internal/ctxutil"
	"github.com/rstmyldrm7/go-notify/internal/domain"
	"github.com/rstmyldrm7/go-notify/internal/metrics"
	"github.com/rstmyldrm7/go-notify/internal/queue"
	"github.com/rstmyldrm7/go-notify/internal/storage"
)

const idempotencyKeyHeader = "Idempotency-Key"

// Repository is the persistence surface the handlers depend on. It is declared
// here, on the consumer side (the Go idiom), so the handlers can be unit-tested
// against a fake; *storage.Repository satisfies it structurally.
type Repository interface {
	Create(ctx context.Context, n *domain.Notification) (storage.CreateResult, error)
	CreateBatch(ctx context.Context, ns []*domain.Notification) ([]storage.CreateResult, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	List(ctx context.Context, f storage.ListFilter) ([]*domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (storage.CancelOutcome, error)
	BatchSummary(ctx context.Context, id uuid.UUID) (map[domain.Status]int, error)
	MarkQueued(ctx context.Context, ids []uuid.UUID) error
	Ping(ctx context.Context) error
}

type Handler struct {
	Repo      Repository
	Publisher queue.Publisher
	Log       *slog.Logger
}

// publishAndQueue pushes pending notifications to the queue and flips their
// status to queued. Publish failures are not fatal for the API response: the
// rows stay pending and can be rescued later (scheduler step).
func (h *Handler) publishAndQueue(c *gin.Context, ns []*domain.Notification) {
	ctx := c.Request.Context()

	var pending []*domain.Notification
	for _, n := range ns {
		if n.Status == domain.StatusPending { // scheduled rows wait for the scheduler
			pending = append(pending, n)
		}
	}
	if len(pending) == 0 {
		return
	}

	if err := h.Publisher.PublishNotifications(ctx, pending); err != nil {
		h.Log.ErrorContext(ctx, "publish failed, rows stay pending",
			"count", len(pending), "error", err)
		metrics.NotificationsPublished.WithLabelValues("failure").Add(float64(len(pending)))
		return
	}
	metrics.NotificationsPublished.WithLabelValues("success").Add(float64(len(pending)))

	ids := make([]uuid.UUID, len(pending))
	for i, n := range pending {
		ids[i] = n.ID
	}
	if err := h.Repo.MarkQueued(ctx, ids); err != nil {
		h.Log.ErrorContext(ctx, "mark queued failed", "error", err)
		return
	}
	for _, n := range pending {
		n.Status = domain.StatusQueued
	}
}

// CreateNotification handles POST /api/v1/notifications.
func (h *Handler) CreateNotification(c *gin.Context) {
	var req createNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	var key *string
	if k := c.GetHeader(idempotencyKeyHeader); k != "" {
		key = &k
	}

	n, err := domain.NewNotification(domain.NewNotificationParams{
		Recipient:      req.Recipient,
		Channel:        domain.Channel(req.Channel),
		Content:        req.Content,
		Priority:       domain.Priority(req.Priority),
		ScheduledAt:    req.ScheduledAt,
		IdempotencyKey: key,
	}, time.Now())
	if err != nil {
		var verrs domain.ValidationErrors
		errors.As(err, &verrs)
		c.JSON(http.StatusUnprocessableEntity, errorResponse{Error: "validation failed", Details: verrs})
		return
	}

	res, err := h.Repo.Create(c.Request.Context(), n)
	if err != nil {
		h.serverError(c, "create notification", err)
		return
	}

	if res.Duplicate {
		// Same key, different payload: client bug — surface it loudly.
		if !res.Notification.SamePayload(req.Recipient, domain.Channel(req.Channel), req.Content) {
			c.JSON(http.StatusConflict, errorResponse{
				Error: "idempotency key already used with a different payload"})
			return
		}
		c.Header("Idempotency-Replayed", "true")
		c.JSON(http.StatusOK, res.Notification)
		return
	}

	metrics.NotificationsCreated.WithLabelValues(string(res.Notification.Channel), string(res.Notification.Priority)).Inc()
	h.publishAndQueue(c, []*domain.Notification{res.Notification})
	c.JSON(http.StatusCreated, res.Notification)
}

// CreateBatch handles POST /api/v1/notifications/batch.
func (h *Handler) CreateBatch(c *gin.Context) {
	var req createBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if len(req.Notifications) == 0 {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "notifications must not be empty"})
		return
	}
	if len(req.Notifications) > maxBatchSize {
		c.JSON(http.StatusBadRequest, errorResponse{
			Error: "batch size exceeds the maximum of " + strconv.Itoa(maxBatchSize)})
		return
	}

	batchID := uuid.New()
	now := time.Now()
	ns := make([]*domain.Notification, len(req.Notifications))
	for i, item := range req.Notifications {
		n, err := domain.NewNotification(domain.NewNotificationParams{
			Recipient:      item.Recipient,
			Channel:        domain.Channel(item.Channel),
			Content:        item.Content,
			Priority:       domain.Priority(item.Priority),
			ScheduledAt:    item.ScheduledAt,
			IdempotencyKey: item.IdempotencyKey,
			BatchID:        &batchID,
		}, now)
		if err != nil {
			var verrs domain.ValidationErrors
			errors.As(err, &verrs)
			c.JSON(http.StatusUnprocessableEntity, errorResponse{
				Error:   "validation failed at index " + strconv.Itoa(i),
				Details: verrs,
			})
			return
		}
		ns[i] = n
	}

	results, err := h.Repo.CreateBatch(c.Request.Context(), ns)
	if err != nil {
		h.serverError(c, "create batch", err)
		return
	}

	resp := createBatchResponse{BatchID: batchID, Total: len(results)}
	var fresh []*domain.Notification
	for i, res := range results {
		if res.Duplicate {
			resp.Duplicates++
		} else {
			resp.Created++
			fresh = append(fresh, res.Notification)
			metrics.NotificationsCreated.WithLabelValues(string(res.Notification.Channel), string(res.Notification.Priority)).Inc()
		}
		resp.Items = append(resp.Items, batchItemResult{
			Index: i, Duplicate: res.Duplicate, Notification: res.Notification,
		})
	}
	h.publishAndQueue(c, fresh)
	c.JSON(http.StatusCreated, resp)
}

// GetNotification handles GET /api/v1/notifications/:id.
func (h *Handler) GetNotification(c *gin.Context) {
	id, ok := h.parseID(c)
	if !ok {
		return
	}
	n, err := h.Repo.GetByID(c.Request.Context(), id)
	if errors.Is(err, storage.ErrNotFound) {
		c.JSON(http.StatusNotFound, errorResponse{Error: "notification not found"})
		return
	}
	if err != nil {
		h.serverError(c, "get notification", err)
		return
	}
	c.JSON(http.StatusOK, n)
}

// ListNotifications handles GET /api/v1/notifications.
func (h *Handler) ListNotifications(c *gin.Context) {
	f := storage.ListFilter{Limit: 20}

	if s := c.Query("status"); s != "" {
		status := domain.Status(s)
		f.Status = &status
	}
	if s := c.Query("channel"); s != "" {
		channel := domain.Channel(s)
		f.Channel = &channel
	}
	if s := c.Query("from"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid 'from' timestamp, use RFC3339"})
			return
		}
		f.From = &t
	}
	if s := c.Query("to"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid 'to' timestamp, use RFC3339"})
			return
		}
		f.To = &t
	}
	if s := c.Query("limit"); s != "" {
		limit, err := strconv.Atoi(s)
		if err != nil || limit < 1 || limit > 100 {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "limit must be between 1 and 100"})
			return
		}
		f.Limit = limit
	}
	page := 1
	if s := c.Query("page"); s != "" {
		p, err := strconv.Atoi(s)
		if err != nil || p < 1 {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "page must be a positive integer"})
			return
		}
		page = p
	}
	limit := f.Limit
	f.Offset = (page - 1) * limit

	// Fetch one extra row to know whether a next page exists.
	f.Limit++
	items, err := h.Repo.List(c.Request.Context(), f)
	if err != nil {
		h.serverError(c, "list notifications", err)
		return
	}

	resp := listResponse{Data: items, Page: page, Limit: limit}
	if len(items) > limit {
		resp.Data = items[:limit]
		resp.HasMore = true
	}
	if resp.Data == nil {
		resp.Data = []*domain.Notification{}
	}
	c.JSON(http.StatusOK, resp)
}

// CancelNotification handles DELETE /api/v1/notifications/:id.
func (h *Handler) CancelNotification(c *gin.Context) {
	id, ok := h.parseID(c)
	if !ok {
		return
	}
	outcome, err := h.Repo.Cancel(c.Request.Context(), id)
	if errors.Is(err, storage.ErrNotFound) {
		c.JSON(http.StatusNotFound, errorResponse{Error: "notification not found"})
		return
	}
	if err != nil {
		h.serverError(c, "cancel notification", err)
		return
	}
	if !outcome.Cancelled {
		c.JSON(http.StatusConflict, errorResponse{
			Error: "notification can no longer be cancelled (status: " +
				string(outcome.Notification.Status) + ")"})
		return
	}
	c.JSON(http.StatusOK, outcome.Notification)
}

// GetBatch handles GET /api/v1/batches/:id.
func (h *Handler) GetBatch(c *gin.Context) {
	id, ok := h.parseID(c)
	if !ok {
		return
	}
	counts, err := h.Repo.BatchSummary(c.Request.Context(), id)
	if err != nil {
		h.serverError(c, "batch summary", err)
		return
	}
	total := 0
	for _, n := range counts {
		total += n
	}
	if total == 0 {
		c.JSON(http.StatusNotFound, errorResponse{Error: "batch not found"})
		return
	}
	c.JSON(http.StatusOK, batchSummaryResponse{BatchID: id, Total: total, Counts: counts})
}

// Healthz handles GET /healthz.
func (h *Handler) Healthz(c *gin.Context) {
	if err := h.Repo.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "database": "unreachable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) parseID(c *gin.Context) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid id, must be a UUID"})
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handler) serverError(c *gin.Context, op string, err error) {
	h.Log.ErrorContext(c.Request.Context(), op+" failed",
		"error", err, "correlation_id", ctxutil.CorrelationID(c.Request.Context()))
	c.JSON(http.StatusInternalServerError, errorResponse{Error: "internal server error"})
}
