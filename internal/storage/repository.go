package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rstmyldrm7/go-notify/internal/domain"
)

// ErrNotFound is returned when a notification does not exist.
var ErrNotFound = errors.New("notification not found")

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

const notificationColumns = `
	id, batch_id, idempotency_key, recipient, channel, content, priority, status,
	attempt_count, scheduled_at, last_error, provider_message_id,
	created_at, updated_at, sent_at`

func scanNotification(row pgx.Row) (*domain.Notification, error) {
	var n domain.Notification
	err := row.Scan(
		&n.ID, &n.BatchID, &n.IdempotencyKey, &n.Recipient, &n.Channel, &n.Content,
		&n.Priority, &n.Status, &n.AttemptCount, &n.ScheduledAt,
		&n.LastError, &n.ProviderMessageID, &n.CreatedAt, &n.UpdatedAt, &n.SentAt,
	)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

// The conflict target must match the partial unique index on idempotency_key,
// hence the WHERE clause. Rows without a key never conflict.
const insertNotificationSQL = `
	INSERT INTO notifications
		(id, batch_id, idempotency_key, recipient, channel, content, priority, status, scheduled_at, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
	ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
	RETURNING` + notificationColumns

func insertArgs(n *domain.Notification) []any {
	return []any{
		n.ID, n.BatchID, n.IdempotencyKey, n.Recipient, n.Channel, n.Content,
		n.Priority, n.Status, n.ScheduledAt, n.CreatedAt,
	}
}

// CreateResult distinguishes a fresh insert from an idempotency replay.
type CreateResult struct {
	Notification *domain.Notification
	// Duplicate is true when the idempotency key was already used; in that
	// case Notification holds the previously stored record.
	Duplicate bool
}

// Create inserts a notification. Idempotency is enforced atomically by the
// unique index: concurrent requests with the same key can never both insert.
func (r *Repository) Create(ctx context.Context, n *domain.Notification) (CreateResult, error) {
	created, err := scanNotification(r.pool.QueryRow(ctx, insertNotificationSQL, insertArgs(n)...))
	if err == nil {
		return CreateResult{Notification: created}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CreateResult{}, fmt.Errorf("insert notification: %w", err)
	}
	// DO NOTHING fired: the key exists. Return the original record.
	existing, err := r.getByIdempotencyKey(ctx, *n.IdempotencyKey)
	if err != nil {
		return CreateResult{}, err
	}
	return CreateResult{Notification: existing, Duplicate: true}, nil
}

// CreateBatch inserts up to the API's batch limit in a single pipelined
// round trip inside one transaction. Per-item idempotency conflicts are
// resolved to the existing records, mirroring Create.
func (r *Repository) CreateBatch(ctx context.Context, ns []*domain.Notification) ([]CreateResult, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, n := range ns {
		batch.Queue(insertNotificationSQL, insertArgs(n)...)
	}
	br := tx.SendBatch(ctx, batch)

	results := make([]CreateResult, len(ns))
	var duplicateKeys []string
	for i := range ns {
		created, err := scanNotification(br.QueryRow())
		switch {
		case err == nil:
			results[i] = CreateResult{Notification: created}
		case errors.Is(err, pgx.ErrNoRows):
			results[i] = CreateResult{Duplicate: true}
			duplicateKeys = append(duplicateKeys, *ns[i].IdempotencyKey)
		default:
			br.Close()
			return nil, fmt.Errorf("batch insert item %d: %w", i, err)
		}
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("close batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	if len(duplicateKeys) > 0 {
		existing, err := r.getByIdempotencyKeys(ctx, duplicateKeys)
		if err != nil {
			return nil, err
		}
		for i, res := range results {
			if res.Duplicate {
				results[i].Notification = existing[*ns[i].IdempotencyKey]
			}
		}
	}
	return results, nil
}

func (r *Repository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := scanNotification(r.pool.QueryRow(ctx,
		`SELECT`+notificationColumns+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get notification: %w", err)
	}
	return n, nil
}

func (r *Repository) getByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	n, err := scanNotification(r.pool.QueryRow(ctx,
		`SELECT`+notificationColumns+` FROM notifications WHERE idempotency_key = $1`, key))
	if err != nil {
		return nil, fmt.Errorf("get by idempotency key: %w", err)
	}
	return n, nil
}

func (r *Repository) getByIdempotencyKeys(ctx context.Context, keys []string) (map[string]*domain.Notification, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT`+notificationColumns+` FROM notifications WHERE idempotency_key = ANY($1)`, keys)
	if err != nil {
		return nil, fmt.Errorf("get by idempotency keys: %w", err)
	}
	defer rows.Close()

	out := make(map[string]*domain.Notification, len(keys))
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out[*n.IdempotencyKey] = n
	}
	return out, rows.Err()
}

// MarkQueued transitions freshly published notifications from pending to
// queued. The status guard keeps scheduled/cancelled rows untouched.
func (r *Repository) MarkQueued(ctx context.Context, ids []uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE notifications SET status = 'queued', updated_at = now()
		 WHERE id = ANY($1) AND status = 'pending'`, ids)
	if err != nil {
		return fmt.Errorf("mark queued: %w", err)
	}
	return nil
}

// MarkProcessing atomically claims a notification for delivery, moving it to
// 'processing' and bumping attempt_count. It returns false when the row is no
// longer claimable — already sent/dead, or cancelled by the client while it sat
// in Kafka — so the worker can skip the send and just advance the offset. This
// conditional UPDATE is the real guard against delivering a cancelled message.
func (r *Repository) MarkProcessing(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE notifications
		    SET status = 'processing', attempt_count = attempt_count + 1, updated_at = now()
		  WHERE id = $1 AND status IN ('queued', 'pending')`, id)
	if err != nil {
		return false, fmt.Errorf("mark processing: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkSent records a successful delivery and the provider's message id.
func (r *Repository) MarkSent(ctx context.Context, id uuid.UUID, providerMessageID string) error {
	var provID *string
	if providerMessageID != "" {
		provID = &providerMessageID
	}
	_, err := r.pool.Exec(ctx,
		`UPDATE notifications
		    SET status = 'sent', provider_message_id = $2, sent_at = now(),
		        last_error = NULL, updated_at = now()
		  WHERE id = $1`, id, provID)
	if err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	return nil
}

// MarkDead records a notification whose in-memory retries were exhausted (or
// that failed permanently) and has been offloaded to the DLQ.
func (r *Repository) MarkDead(ctx context.Context, id uuid.UUID, lastError string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE notifications
		    SET status = 'dead', last_error = $2, updated_at = now()
		  WHERE id = $1`, id, lastError)
	if err != nil {
		return fmt.Errorf("mark dead: %w", err)
	}
	return nil
}

// DispatchDue claims up to limit notifications that are due — scheduled and
// past their scheduled_at — publishes them via the supplied callback while they
// are still locked, and on success transitions them to 'queued'. It returns the
// number dispatched.
//
// Correctness/concurrency notes:
//   - FOR UPDATE SKIP LOCKED lets several scheduler instances run concurrently
//     without ever claiming the same row, so the poller scales horizontally.
//   - publish runs inside the transaction (rows locked, not yet 'queued'). If
//     it fails we roll back and the rows stay 'scheduled' for the next tick, so
//     a message is never marked queued without having reached Kafka. The cost
//     is holding the transaction open across the Kafka write; acceptable for a
//     bounded batch on a low-frequency poller.
//   - If publish succeeds but the commit is lost, the rows replay next tick and
//     are published again — at-least-once. The worker's MarkProcessing guard
//     makes the duplicate harmless.
func (r *Repository) DispatchDue(
	ctx context.Context,
	now time.Time,
	limit int,
	publish func(context.Context, []*domain.Notification) error,
) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT`+notificationColumns+`
		   FROM notifications
		  WHERE status = 'scheduled' AND scheduled_at <= $1
		  ORDER BY scheduled_at
		  LIMIT $2
		  FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return 0, fmt.Errorf("select due notifications: %w", err)
	}

	var due []*domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		due = append(due, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(due) == 0 {
		return 0, nil
	}

	if err := publish(ctx, due); err != nil {
		return 0, fmt.Errorf("publish due notifications: %w", err) // rolled back by defer
	}

	ids := make([]uuid.UUID, len(due))
	for i, n := range due {
		ids[i] = n.ID
	}
	if _, err := tx.Exec(ctx,
		`UPDATE notifications SET status = 'queued', updated_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark due queued: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit due dispatch: %w", err)
	}
	return len(due), nil
}

// ReapStuck re-dispatches notifications stranded in a non-terminal state by an
// edge case, making the database — not the Kafka offset — the source of truth:
//
//   - 'pending'    : the API failed to publish it. It never reached Kafka, so
//                    re-publishing cannot duplicate it (short pendingAfter).
//   - 'queued'     : published but never delivered (e.g. lost to an out-of-order
//                    offset commit, or a topic with no live consumer).
//   - 'processing' : a worker claimed it then died before a terminal state.
//
// inflightAfter must exceed the worst-case delivery time, since a 'queued' or
// 'processing' row may still legitimately be in flight; reclaiming one too early
// would double-send. A late duplicate is otherwise harmless — the worker's
// MarkProcessing guard dedups it. Matched rows are re-published and reset to
// 'queued', which refreshes updated_at and so leases them for another window
// before they could be reaped again. FOR UPDATE SKIP LOCKED keeps concurrent
// reapers from reclaiming the same row. Returns the reclaimed rows with their
// original (pre-reset) status.
func (r *Repository) ReapStuck(
	ctx context.Context,
	now time.Time,
	pendingAfter, inflightAfter time.Duration,
	limit int,
	publish func(context.Context, []*domain.Notification) error,
) ([]*domain.Notification, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT`+notificationColumns+`
		   FROM notifications
		  WHERE (status = 'pending' AND updated_at < $1)
		     OR (status IN ('queued', 'processing') AND updated_at < $2)
		  ORDER BY updated_at
		  LIMIT $3
		  FOR UPDATE SKIP LOCKED`,
		now.Add(-pendingAfter), now.Add(-inflightAfter), limit)
	if err != nil {
		return nil, fmt.Errorf("select stuck notifications: %w", err)
	}

	var stuck []*domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		stuck = append(stuck, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(stuck) == 0 {
		return nil, nil
	}

	if err := publish(ctx, stuck); err != nil {
		return nil, fmt.Errorf("re-publish stuck notifications: %w", err) // rolled back by defer
	}

	ids := make([]uuid.UUID, len(stuck))
	for i, n := range stuck {
		ids[i] = n.ID
	}
	if _, err := tx.Exec(ctx,
		`UPDATE notifications SET status = 'queued', updated_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return nil, fmt.Errorf("requeue stuck notifications: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit reap: %w", err)
	}
	return stuck, nil
}

// CancelOutcome describes what happened during a cancel attempt.
type CancelOutcome struct {
	Notification *domain.Notification
	Cancelled    bool
}

// Cancel atomically cancels a notification if it is still cancellable.
// The conditional UPDATE is the real guard against racing workers: once a
// worker claims the row (status=processing) this matches zero rows.
func (r *Repository) Cancel(ctx context.Context, id uuid.UUID) (CancelOutcome, error) {
	n, err := scanNotification(r.pool.QueryRow(ctx,
		`UPDATE notifications SET status = 'cancelled', updated_at = now()
		 WHERE id = $1 AND status IN ('pending', 'queued', 'scheduled')
		 RETURNING`+notificationColumns, id))
	if err == nil {
		return CancelOutcome{Notification: n, Cancelled: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CancelOutcome{}, fmt.Errorf("cancel notification: %w", err)
	}
	// Either it does not exist or it is past the point of no return.
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return CancelOutcome{}, err
	}
	return CancelOutcome{Notification: current, Cancelled: false}, nil
}

// ListFilter narrows and paginates the list endpoint (page/offset based).
type ListFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	From    *time.Time
	To      *time.Time

	Limit  int
	Offset int
}

func (r *Repository) List(ctx context.Context, f ListFilter) ([]*domain.Notification, error) {
	var (
		conds []string
		args  []any
	)
	next := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if f.Status != nil {
		conds = append(conds, "status = "+next(*f.Status))
	}
	if f.Channel != nil {
		conds = append(conds, "channel = "+next(*f.Channel))
	}
	if f.From != nil {
		conds = append(conds, "created_at >= "+next(*f.From))
	}
	if f.To != nil {
		conds = append(conds, "created_at <= "+next(*f.To))
	}
	query := `SELECT` + notificationColumns + ` FROM notifications`
	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	query += " ORDER BY created_at DESC, id DESC LIMIT " + next(f.Limit) + " OFFSET " + next(f.Offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()

	var out []*domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// BatchSummary returns the status breakdown of a batch.
func (r *Repository) BatchSummary(ctx context.Context, batchID uuid.UUID) (map[domain.Status]int, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, batchID)
	if err != nil {
		return nil, fmt.Errorf("batch summary: %w", err)
	}
	defer rows.Close()

	out := make(map[domain.Status]int)
	for rows.Next() {
		var status domain.Status
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		out[status] = count
	}
	return out, rows.Err()
}
