-- Supports the reconciliation reaper, which scans for notifications stuck in a
-- non-terminal state ordered by updated_at. Partial so it stays small: terminal
-- rows (sent/dead/cancelled) and future-dated scheduled rows are excluded.
CREATE INDEX ix_notifications_reaper
    ON notifications (status, updated_at)
    WHERE status IN ('pending', 'queued', 'processing');
