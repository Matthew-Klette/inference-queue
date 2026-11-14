// Package queue implements a durable work queue on top of Postgres.
//
// Items are leased with FOR UPDATE SKIP LOCKED. A lease carries an owner id
// and an expiry; an item whose lease has expired is eligible to be leased
// again, which is how work from a crashed worker is recovered. Every state
// transition is fenced on (lease_owner, attempts) so a worker that lost its
// lease cannot overwrite the result of the worker that took over.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrLeaseLost is returned when a transition is attempted by a worker that no
// longer holds the lease for the item.
var ErrLeaseLost = errors.New("lease lost")

// Leased is an item handed to a worker.
type Leased struct {
	ID          int64
	BatchID     uuid.UUID
	Index       int
	Prompt      string
	Model       string
	Params      json.RawMessage
	Attempts    int
	MaxAttempts int
	// QueueWait is the time between the item becoming runnable and being leased.
	QueueWait time.Duration
	Owner     string
}

// Completion carries the outcome of a successful model call.
type Completion struct {
	Output           string
	Cached           bool
	PromptTokens     int
	CompletionTokens int
	Latency          time.Duration
}

// Depth is a snapshot of queue sizes.
type Depth struct {
	Ready   int // queued and runnable now
	Delayed int // queued, waiting for backoff
	Leased  int // currently leased (including expired but not reclaimed)
	Dead    int
}

// Queue is the Postgres-backed queue.
type Queue struct {
	Pool *pgxpool.Pool
	// DefaultWebhook receives batch.completed events for batches without their own webhook_url.
	DefaultWebhook string
}

// Lease claims up to n runnable items for owner. Items whose previous lease
// expired are included while they still have attempts left.
func (q *Queue) Lease(ctx context.Context, owner string, n int, lease time.Duration) ([]Leased, error) {
	if n <= 0 {
		return nil, nil
	}
	tx, err := q.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		WITH c AS (
			SELECT id FROM items
			WHERE (status = 'queued' AND run_after <= now())
			   OR (status = 'leased' AND lease_expires_at < now() AND attempts < max_attempts)
			ORDER BY run_after, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), u AS (
			UPDATE items i SET
				last_error = CASE WHEN i.status = 'leased' THEN 'lease expired' ELSE i.last_error END,
				status = 'leased',
				attempts = i.attempts + 1,
				lease_owner = $2,
				lease_expires_at = now() + make_interval(secs => $3)
			FROM c WHERE i.id = c.id
			RETURNING i.id, i.batch_id, i.idx, i.prompt, i.attempts, i.max_attempts,
			          GREATEST(EXTRACT(EPOCH FROM now() - i.run_after), 0) AS wait
		)
		SELECT u.id, u.batch_id, u.idx, u.prompt, u.attempts, u.max_attempts, u.wait, b.model, b.params
		FROM u JOIN batches b ON b.id = u.batch_id
		ORDER BY u.id`, n, owner, lease.Seconds())
	if err != nil {
		return nil, err
	}
	var out []Leased
	batchSeen := map[uuid.UUID]struct{}{}
	for rows.Next() {
		var l Leased
		var wait float64
		if err := rows.Scan(&l.ID, &l.BatchID, &l.Index, &l.Prompt, &l.Attempts, &l.MaxAttempts, &wait, &l.Model, &l.Params); err != nil {
			rows.Close()
			return nil, err
		}
		l.QueueWait = time.Duration(wait * float64(time.Second))
		l.Owner = owner
		out = append(out, l)
		batchSeen[l.BatchID] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(batchSeen) > 0 {
		ids := make([]uuid.UUID, 0, len(batchSeen))
		for id := range batchSeen {
			ids = append(ids, id)
		}
		// SKIP LOCKED: if another worker is flipping the same batch to running, that is enough.
		if _, err := tx.Exec(ctx, `
			UPDATE batches SET status = 'running'
			WHERE id IN (SELECT id FROM batches WHERE id = ANY($1) AND status = 'queued' ORDER BY id FOR UPDATE SKIP LOCKED)`, ids); err != nil {
			return nil, err
		}
	}
	return out, tx.Commit(ctx)
}

// Extend pushes out the lease expiry for items still owned by owner.
func (q *Queue) Extend(ctx context.Context, owner string, ids []int64, lease time.Duration) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := q.Pool.Exec(ctx, `
		UPDATE items SET lease_expires_at = now() + make_interval(secs => $3)
		WHERE id = ANY($1) AND status = 'leased' AND lease_owner = $2`, ids, owner, lease.Seconds())
	return err
}

// Complete records a successful result. It returns ErrLeaseLost if the lease
// is no longer held, in which case nothing is written.
func (q *Queue) Complete(ctx context.Context, l Leased, c Completion) error {
	return q.inTx(ctx, func(tx pgx.Tx) error {
		var matched *bool
		err := tx.QueryRow(ctx, `
			UPDATE items SET status = 'succeeded', result = $4, cached = $5, prompt_tokens = $6,
				completion_tokens = $7, latency_ms = $8, completed_at = now(), last_error = NULL,
				lease_owner = NULL, lease_expires_at = NULL,
				matched = CASE WHEN expected IS NULL THEN NULL ELSE btrim($4) = btrim(expected) END
			WHERE id = $1 AND status = 'leased' AND lease_owner = $2 AND attempts = $3
			RETURNING matched`,
			l.ID, l.Owner, l.Attempts, c.Output, c.Cached, c.PromptTokens, c.CompletionTokens,
			int(c.Latency.Milliseconds())).Scan(&matched)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		scored, hit := 0, 0
		if matched != nil {
			scored = 1
			if *matched {
				hit = 1
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE batches SET succeeded = succeeded + 1, scored = scored + $2, matched = matched + $3 WHERE id = $1`,
			l.BatchID, scored, hit); err != nil {
			return err
		}
		return q.finishBatch(ctx, tx, l.BatchID)
	})
}

// Fail records a failed attempt. A retryable failure with attempts remaining
// goes back to the queue after backoff; anything else becomes dead.
func (q *Queue) Fail(ctx context.Context, l Leased, cause error, retryable bool, backoff time.Duration) error {
	msg := cause.Error()
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	if retryable && l.Attempts < l.MaxAttempts {
		tag, err := q.Pool.Exec(ctx, `
			UPDATE items SET status = 'queued', run_after = now() + make_interval(secs => $5), last_error = $4,
				lease_owner = NULL, lease_expires_at = NULL
			WHERE id = $1 AND status = 'leased' AND lease_owner = $2 AND attempts = $3`,
			l.ID, l.Owner, l.Attempts, msg, backoff.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		return nil
	}
	return q.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE items SET status = 'dead', last_error = $4, completed_at = now(),
				lease_owner = NULL, lease_expires_at = NULL
			WHERE id = $1 AND status = 'leased' AND lease_owner = $2 AND attempts = $3`,
			l.ID, l.Owner, l.Attempts, msg)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		if _, err := tx.Exec(ctx, `UPDATE batches SET failed = failed + 1 WHERE id = $1`, l.BatchID); err != nil {
			return err
		}
		return q.finishBatch(ctx, tx, l.BatchID)
	})
}

// Requeue returns an item to the queue without charging an attempt. It is
// used when the failure is not the item's fault (circuit open, shutdown).
func (q *Queue) Requeue(ctx context.Context, l Leased, delay time.Duration) error {
	tag, err := q.Pool.Exec(ctx, `
		UPDATE items SET status = 'queued', attempts = attempts - 1, run_after = now() + make_interval(secs => $4),
			lease_owner = NULL, lease_expires_at = NULL
		WHERE id = $1 AND status = 'leased' AND lease_owner = $2 AND attempts = $3`,
		l.ID, l.Owner, l.Attempts, delay.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Release hands back every item still leased by owner, without charging an
// attempt. Workers call this on shutdown.
func (q *Queue) Release(ctx context.Context, owner string) (int, error) {
	tag, err := q.Pool.Exec(ctx, `
		UPDATE items SET status = 'queued', attempts = attempts - 1, run_after = now(),
			lease_owner = NULL, lease_expires_at = NULL
		WHERE status = 'leased' AND lease_owner = $1`, owner)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// ReapExpired dead-letters items whose lease expired after their final
// attempt. Items with attempts left are not touched here: Lease reclaims them.
func (q *Queue) ReapExpired(ctx context.Context) (int, error) {
	total := 0
	err := q.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH d AS (
				UPDATE items SET status = 'dead', completed_at = now(), lease_owner = NULL, lease_expires_at = NULL,
					last_error = 'lease expired after final attempt'
				WHERE id IN (
					SELECT id FROM items
					WHERE status = 'leased' AND lease_expires_at < now() AND attempts >= max_attempts
					ORDER BY id LIMIT 500 FOR UPDATE SKIP LOCKED)
				RETURNING batch_id)
			SELECT batch_id, count(*) FROM d GROUP BY batch_id ORDER BY batch_id`)
		if err != nil {
			return err
		}
		counts := map[uuid.UUID]int{}
		var order []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			var n int
			if err := rows.Scan(&id, &n); err != nil {
				rows.Close()
				return err
			}
			counts[id] = n
			order = append(order, id)
			total += n
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, id := range order {
			if _, err := tx.Exec(ctx, `UPDATE batches SET failed = failed + $2 WHERE id = $1`, id, counts[id]); err != nil {
				return err
			}
			if err := q.finishBatch(ctx, tx, id); err != nil {
				return err
			}
		}
		return nil
	})
	return total, err
}

// Depth returns current queue sizes. It only touches partial indexes.
func (q *Queue) Depth(ctx context.Context) (Depth, error) {
	var d Depth
	err := q.Pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status = 'queued' AND run_after <= now()),
			count(*) FILTER (WHERE status = 'queued' AND run_after > now()),
			count(*) FILTER (WHERE status = 'leased')
		FROM items WHERE status IN ('queued', 'leased')`).Scan(&d.Ready, &d.Delayed, &d.Leased)
	if err != nil {
		return d, err
	}
	err = q.Pool.QueryRow(ctx, `SELECT count(*) FROM items WHERE status = 'dead'`).Scan(&d.Dead)
	return d, err
}

// finishBatch marks the batch complete when every item is terminal and, in the
// same transaction, writes the batch.completed outbox row. Because the outbox
// insert shares the transaction, the event exists if and only if the batch
// state change committed.
func (q *Queue) finishBatch(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	var webhook *string
	err := tx.QueryRow(ctx, `
		UPDATE batches SET
			status = CASE WHEN failed = 0 THEN 'completed' ELSE 'completed_with_errors' END,
			completed_at = now()
		WHERE id = $1 AND succeeded + failed = total AND completed_at IS NULL
		RETURNING COALESCE(webhook_url, $2)`, id, q.DefaultWebhook).Scan(&webhook)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // not finished yet
	}
	if err != nil {
		return err
	}
	if webhook == nil || *webhook == "" {
		return nil
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (batch_id, event_type, payload, webhook_url)
		SELECT id, 'batch.completed', jsonb_build_object(
			'batch_id', id, 'status', status, 'model', model, 'total', total,
			'succeeded', succeeded, 'failed', failed, 'scored', scored, 'matched', matched,
			'created_at', created_at, 'completed_at', completed_at), $2
		FROM batches WHERE id = $1`, id, *webhook)
	return err
}

func (q *Queue) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := q.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
