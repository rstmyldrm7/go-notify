package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rstmyldrm7/go-notify/internal/domain"
)

func testRequest() Request {
	return Request{To: "+905551234567", Channel: domain.ChannelSMS, Content: "hi"}
}

// serverReturning spins up a provider stub that always responds with code.
func serverReturning(t *testing.T, code int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSendAccepted(t *testing.T) {
	srv := serverReturning(t, http.StatusAccepted,
		`{"messageId":"abc-123","status":"accepted","timestamp":"2026-06-12T00:00:00Z"}`)

	resp, err := New(srv.URL, time.Second).Send(context.Background(), testRequest())
	require.NoError(t, err)
	assert.Equal(t, "abc-123", resp.MessageID)
}

func TestSendServerErrorIsRetryable(t *testing.T) {
	srv := serverReturning(t, http.StatusServiceUnavailable, "")

	_, err := New(srv.URL, time.Second).Send(context.Background(), testRequest())
	var perr *Error
	require.ErrorAs(t, err, &perr)
	assert.True(t, perr.Retryable, "503 should be retryable")
}

func TestSendClientErrorIsPermanent(t *testing.T) {
	srv := serverReturning(t, http.StatusBadRequest, "")

	_, err := New(srv.URL, time.Second).Send(context.Background(), testRequest())
	var perr *Error
	require.ErrorAs(t, err, &perr)
	assert.False(t, perr.Retryable, "400 should not be retryable")
}

func TestSendTooManyRequestsIsRetryable(t *testing.T) {
	srv := serverReturning(t, http.StatusTooManyRequests, "")

	_, err := New(srv.URL, time.Second).Send(context.Background(), testRequest())
	var perr *Error
	require.ErrorAs(t, err, &perr)
	assert.True(t, perr.Retryable, "429 should be retryable")
}
