package provider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rstmyldrm7/go-notify/internal/domain"
)

func testRequest() Request {
	return Request{To: "+905551234567", Channel: domain.ChannelSMS, Content: "hi"}
}

func TestSendAccepted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"abc-123","status":"accepted","timestamp":"2026-06-12T00:00:00Z"}`))
	}))
	defer srv.Close()

	resp, err := New(srv.URL, time.Second).Send(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MessageID != "abc-123" {
		t.Fatalf("got messageId %q, want abc-123", resp.MessageID)
	}
}

func TestSendServerErrorIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	_, err := New(srv.URL, time.Second).Send(context.Background(), testRequest())
	var perr *Error
	if !errors.As(err, &perr) {
		t.Fatalf("expected *provider.Error, got %T", err)
	}
	if !perr.Retryable {
		t.Fatal("503 should be retryable")
	}
}

func TestSendClientErrorIsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	_, err := New(srv.URL, time.Second).Send(context.Background(), testRequest())
	var perr *Error
	if !errors.As(err, &perr) {
		t.Fatalf("expected *provider.Error, got %T", err)
	}
	if perr.Retryable {
		t.Fatal("400 should not be retryable")
	}
}

func TestSendTooManyRequestsIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	_, err := New(srv.URL, time.Second).Send(context.Background(), testRequest())
	var perr *Error
	if !errors.As(err, &perr) || !perr.Retryable {
		t.Fatalf("429 should be a retryable provider error, got %v", err)
	}
}
