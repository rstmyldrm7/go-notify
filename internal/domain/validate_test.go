package domain

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// base returns params that pass validation, so each test can mutate exactly one
// field and assert the effect of that field in isolation.
func base() NewNotificationParams {
	return NewNotificationParams{
		Recipient: "+905551234567",
		Channel:   ChannelSMS,
		Content:   "hello",
		Priority:  PriorityHigh,
	}
}

func TestNewNotificationValid(t *testing.T) {
	now := time.Now()
	n, err := NewNotification(base(), now)
	require.NoError(t, err)
	assert.Equal(t, StatusPending, n.Status)
	assert.NotEqual(t, [16]byte{}, [16]byte(n.ID), "expected a generated ID")
	assert.True(t, n.CreatedAt.Equal(now) && n.UpdatedAt.Equal(now), "timestamps should be now")
}

func TestNewNotificationValidationRules(t *testing.T) {
	longRecipient := strings.Repeat("a", maxRecipientLength+1)

	tests := []struct {
		name      string
		mutate    func(*NewNotificationParams)
		wantField string // the field expected to fail; "" means expect success
	}{
		{"missing recipient", func(p *NewNotificationParams) { p.Recipient = "  " }, "recipient"},
		{"recipient too long", func(p *NewNotificationParams) { p.Recipient = longRecipient }, "recipient"},
		{"unknown channel", func(p *NewNotificationParams) { p.Channel = "carrier-pigeon" }, "channel"},
		{"missing content", func(p *NewNotificationParams) { p.Content = "" }, "content"},
		{"content over sms limit", func(p *NewNotificationParams) { p.Content = strings.Repeat("x", 161) }, "content"},
		{"invalid priority", func(p *NewNotificationParams) { p.Priority = "urgent" }, "priority"},
		{"blank idempotency key", func(p *NewNotificationParams) { k := "  "; p.IdempotencyKey = &k }, "idempotency_key"},
		{"valid", func(p *NewNotificationParams) {}, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := base()
			tc.mutate(&p)

			_, err := NewNotification(p, time.Now())
			if tc.wantField == "" {
				require.NoError(t, err)
				return
			}
			var verrs ValidationErrors
			require.ErrorAs(t, err, &verrs)
			assert.True(t, hasField(verrs, tc.wantField),
				"expected an error on field %q, got %v", tc.wantField, verrs)
		})
	}
}

// TestContentLimitsPerChannel locks in that each channel enforces its own cap
// and that the limit counts runes, not bytes (so multi-byte content is not
// rejected early).
func TestContentLimitsPerChannel(t *testing.T) {
	for ch, limit := range contentLimits {
		p := base()
		p.Channel = ch

		p.Content = strings.Repeat("a", limit)
		_, err := NewNotification(p, time.Now())
		assert.NoErrorf(t, err, "%s: content at the limit (%d) should pass", ch, limit)

		p.Content = strings.Repeat("a", limit+1)
		_, err = NewNotification(p, time.Now())
		assert.Errorf(t, err, "%s: content over the limit (%d) should fail", ch, limit)

		// Runes, not bytes: `limit` two-byte runes is within the limit even
		// though it is 2×limit bytes.
		p.Content = strings.Repeat("é", limit)
		_, err = NewNotification(p, time.Now())
		assert.NoErrorf(t, err, "%s: %d runes should pass regardless of byte length", ch, limit)
	}
}

func TestNewNotificationDefaultsPriority(t *testing.T) {
	p := base()
	p.Priority = ""
	n, err := NewNotification(p, time.Now())
	require.NoError(t, err)
	assert.Equal(t, PriorityNormal, n.Priority)
}

// TestScheduledAt drives the branch that decides the initial status: a future
// time makes the row 'scheduled' (the scheduler will dispatch it), while a past
// time is rejected.
func TestScheduledAt(t *testing.T) {
	now := time.Now()

	t.Run("future schedules", func(t *testing.T) {
		p := base()
		future := now.Add(time.Hour)
		p.ScheduledAt = &future
		n, err := NewNotification(p, now)
		require.NoError(t, err)
		assert.Equal(t, StatusScheduled, n.Status)
	})

	t.Run("past is rejected", func(t *testing.T) {
		p := base()
		past := now.Add(-time.Second)
		p.ScheduledAt = &past
		_, err := NewNotification(p, now)
		require.Error(t, err)
	})
}

func TestValidationErrorsAggregate(t *testing.T) {
	// Several rules fail at once; the API should learn about all of them in one
	// response rather than one error per round trip.
	p := NewNotificationParams{Recipient: "", Channel: "nope", Content: ""}
	_, err := NewNotification(p, time.Now())

	var verrs ValidationErrors
	require.ErrorAs(t, err, &verrs)
	assert.GreaterOrEqual(t, len(verrs), 3, "expected aggregated errors: %v", verrs)
}

func TestSamePayload(t *testing.T) {
	n := &Notification{Recipient: "+905551234567", Channel: ChannelSMS, Content: "hi"}
	assert.True(t, n.SamePayload("+905551234567", ChannelSMS, "hi"))
	assert.False(t, n.SamePayload("+905551234567", ChannelSMS, "different"))
}

func hasField(verrs ValidationErrors, field string) bool {
	for _, fe := range verrs {
		if fe.Field == field {
			return true
		}
	}
	return false
}
