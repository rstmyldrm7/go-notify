// Package provider contains the HTTP client for the external notification
// provider. In this assessment the provider is simulated by webhook.site, which
// is configured to return HTTP 202 with an accepted-acknowledgement body.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/rstmyldrm7/go-notify/internal/domain"
)

// Request is the JSON payload posted to the provider, matching the format the
// assessment specifies:
//
//	{ "to": "+905551234567", "channel": "sms", "content": "Your message" }
type Request struct {
	To      string         `json:"to"`
	Channel domain.Channel `json:"channel"`
	Content string         `json:"content"`
}

// Response is the provider's accepted acknowledgement (HTTP 202):
//
//	{ "messageId": "uuid", "status": "accepted", "timestamp": "ISO8601" }
type Response struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// Error describes a failed delivery. Retryable distinguishes transient failures
// (timeouts, 5xx, 429) that an in-memory retry might recover from, from
// permanent ones (4xx) that will never succeed and should go straight to the
// DLQ.
type Error struct {
	StatusCode int
	Retryable  bool
	msg        string
}

func (e *Error) Error() string { return e.msg }

// Client posts notifications to the external provider over HTTP.
type Client struct {
	url  string
	http *http.Client
}

// New builds a client targeting url (the caller's webhook.site URL) with a
// per-request timeout.
func New(url string, timeout time.Duration) *Client {
	return &Client{
		url: url,
		http: &http.Client{
			Timeout: timeout,
			// otelhttp emits a client span per delivery and injects the trace
			// context, so the provider call shows up under the worker's
			// processing span in the trace waterfall.
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

// Send delivers one notification. A nil error means the provider accepted the
// message (HTTP 202, or 200 for an unconfigured webhook.site). Transport errors
// and 5xx/429 responses come back as a retryable *Error; other 4xx responses as
// a non-retryable *Error.
func (c *Client) Send(ctx context.Context, req Request) (*Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal provider request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build provider request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		// Timeouts and connection failures are transient by nature.
		return nil, &Error{Retryable: true, msg: fmt.Sprintf("provider request failed: %v", err)}
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK:
		var pr Response
		if err := json.Unmarshal(payload, &pr); err != nil {
			// Accepted but the body is not the expected JSON: still a success,
			// we just have no provider message id to record.
			return &Response{Status: "accepted"}, nil
		}
		return &pr, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return nil, &Error{
			StatusCode: resp.StatusCode, Retryable: true,
			msg: fmt.Sprintf("provider returned %d: %s", resp.StatusCode, truncate(payload)),
		}
	default:
		return nil, &Error{
			StatusCode: resp.StatusCode, Retryable: false,
			msg: fmt.Sprintf("provider rejected with %d: %s", resp.StatusCode, truncate(payload)),
		}
	}
}

func truncate(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}
