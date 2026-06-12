CREATE TABLE notifications (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            UUID,
    idempotency_key     TEXT,

    recipient           TEXT NOT NULL,
    channel             TEXT NOT NULL,
    content             TEXT NOT NULL,
    priority            TEXT NOT NULL DEFAULT 'normal',
    status              TEXT NOT NULL DEFAULT 'pending',

    attempt_count       INT  NOT NULL DEFAULT 0,
    scheduled_at        TIMESTAMPTZ,
    last_error          TEXT,

    provider_message_id TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at             TIMESTAMPTZ,

    CONSTRAINT chk_notifications_channel
        CHECK (channel IN ('sms', 'email', 'push')),
    CONSTRAINT chk_notifications_priority
        CHECK (priority IN ('high', 'normal', 'low')),
    CONSTRAINT chk_notifications_status
        CHECK (status IN ('pending', 'queued', 'processing', 'sent', 'dead', 'cancelled', 'scheduled'))
);

-- Idempotency: enforced by the database, not application code. Partial index
-- so requests without a key are not constrained (NULLs never collide anyway).
CREATE UNIQUE INDEX ux_notifications_idempotency_key
    ON notifications (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Batch status lookups (GET /batches/:id).
CREATE INDEX ix_notifications_batch_id
    ON notifications (batch_id)
    WHERE batch_id IS NOT NULL;

-- List endpoint: filter by status/channel, newest first, cursor pagination.
CREATE INDEX ix_notifications_list
    ON notifications (status, channel, created_at DESC, id DESC);

-- Scheduler polls: future-dated notifications that are due.
CREATE INDEX ix_notifications_scheduled
    ON notifications (scheduled_at)
    WHERE status = 'scheduled';
