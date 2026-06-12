package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rstmyldrm7/go-notify/internal/domain"
	"github.com/rstmyldrm7/go-notify/internal/queue"
	"github.com/rstmyldrm7/go-notify/internal/storage"
)

// fakeRepo is a hand-rolled stand-in for storage.Repository. Each test wires
// only the methods it exercises; the rest return zero values. A hand-rolled
// fake keeps the dependency explicit and reads more clearly than a generated
// mock for a surface this small.
type fakeRepo struct {
	createResult storage.CreateResult
	createErr    error
	getResult    *domain.Notification
	getErr       error
	cancelResult storage.CancelOutcome
	cancelErr    error
}

func (f *fakeRepo) Create(context.Context, *domain.Notification) (storage.CreateResult, error) {
	return f.createResult, f.createErr
}
func (f *fakeRepo) CreateBatch(context.Context, []*domain.Notification) ([]storage.CreateResult, error) {
	return nil, nil
}
func (f *fakeRepo) GetByID(context.Context, uuid.UUID) (*domain.Notification, error) {
	return f.getResult, f.getErr
}
func (f *fakeRepo) List(context.Context, storage.ListFilter) ([]*domain.Notification, error) {
	return nil, nil
}
func (f *fakeRepo) Cancel(context.Context, uuid.UUID) (storage.CancelOutcome, error) {
	return f.cancelResult, f.cancelErr
}
func (f *fakeRepo) BatchSummary(context.Context, uuid.UUID) (map[domain.Status]int, error) {
	return nil, nil
}
func (f *fakeRepo) MarkQueued(context.Context, []uuid.UUID) error { return nil }
func (f *fakeRepo) Ping(context.Context) error                    { return nil }

func newTestRouter(repo Repository) *gin.Engine {
	gin.SetMode(gin.TestMode)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &Handler{Repo: repo, Publisher: &queue.NoopPublisher{Log: log}, Log: log}
	return NewRouter(h, log)
}

func doRequest(t *testing.T, router *gin.Engine, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func validCreateBody() map[string]any {
	return map[string]any{
		"recipient": "+905551234567",
		"channel":   "sms",
		"content":   "hello",
		"priority":  "high",
	}
}

func newNotification(t *testing.T, content string) *domain.Notification {
	t.Helper()
	n, err := domain.NewNotification(domain.NewNotificationParams{
		Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: content, Priority: domain.PriorityHigh,
	}, time.Now())
	require.NoError(t, err)
	return n
}

// TestCreateNotification covers the happy path: the row is persisted, published
// and returned as 'queued' (publishAndQueue flips it once the NoopPublisher
// "accepts" it).
func TestCreateNotification(t *testing.T) {
	router := newTestRouter(&fakeRepo{createResult: storage.CreateResult{Notification: newNotification(t, "hello")}})

	rec := doRequest(t, router, http.MethodPost, "/api/v1/notifications", validCreateBody(), nil)

	require.Equal(t, http.StatusCreated, rec.Code, "body=%s", rec.Body)
	var got domain.Notification
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, domain.StatusQueued, got.Status, "status should flip to queued after publish")
}

func TestCreateNotificationInvalidJSON(t *testing.T) {
	router := newTestRouter(&fakeRepo{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateNotificationValidationError(t *testing.T) {
	body := validCreateBody()
	body["content"] = "" // required
	router := newTestRouter(&fakeRepo{})

	rec := doRequest(t, router, http.MethodPost, "/api/v1/notifications", body, nil)

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}

// TestIdempotencyReplay: a duplicate key with the same payload returns the
// original record as 200 with the replay header, not a new 201.
func TestIdempotencyReplay(t *testing.T) {
	router := newTestRouter(&fakeRepo{
		createResult: storage.CreateResult{Notification: newNotification(t, "hello"), Duplicate: true},
	})

	rec := doRequest(t, router, http.MethodPost, "/api/v1/notifications",
		validCreateBody(), map[string]string{"Idempotency-Key": "order-42"})

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body)
	assert.Equal(t, "true", rec.Header().Get("Idempotency-Replayed"))
}

// TestIdempotencyConflict: reusing a key with a different payload is a client
// bug and must surface as 409, not a silent replay.
func TestIdempotencyConflict(t *testing.T) {
	router := newTestRouter(&fakeRepo{
		createResult: storage.CreateResult{Notification: newNotification(t, "ORIGINAL"), Duplicate: true},
	})

	body := validCreateBody()
	body["content"] = "DIFFERENT" // same key (header), different payload
	rec := doRequest(t, router, http.MethodPost, "/api/v1/notifications",
		body, map[string]string{"Idempotency-Key": "order-42"})

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestGetNotificationNotFound(t *testing.T) {
	router := newTestRouter(&fakeRepo{getErr: storage.ErrNotFound})

	rec := doRequest(t, router, http.MethodGet, "/api/v1/notifications/"+uuid.NewString(), nil, nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetNotificationInvalidID(t *testing.T) {
	router := newTestRouter(&fakeRepo{})

	rec := doRequest(t, router, http.MethodGet, "/api/v1/notifications/not-a-uuid", nil, nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCancelTooLate: once a row is no longer cancellable the conditional UPDATE
// matches nothing; the handler must report 409 with the current status, not a
// success.
func TestCancelTooLate(t *testing.T) {
	processing := &domain.Notification{ID: uuid.New(), Status: domain.StatusProcessing}
	router := newTestRouter(&fakeRepo{
		cancelResult: storage.CancelOutcome{Notification: processing, Cancelled: false},
	})

	rec := doRequest(t, router, http.MethodDelete, "/api/v1/notifications/"+processing.ID.String(), nil, nil)

	assert.Equal(t, http.StatusConflict, rec.Code)
}
