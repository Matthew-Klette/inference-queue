package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrIdempotencyConflict means the Idempotency-Key was already used with a different request body.
var ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")

// NewItem is one prompt to enqueue.
type NewItem struct {
	Ref      string
	Prompt   string
	Expected *string
}

// NewBatch is the input to Batches.Create.
type NewBatch struct {
	APIKeyID       uuid.UUID
	Model          string
	Params         json.RawMessage
	Items          []NewItem
	WebhookURL     string
	IdempotencyKey string
	RequestHash    []byte
	MaxAttempts    int
}

// Batch is a batch with its progress counters.
type Batch struct {
	ID          uuid.UUID       `json:"id"`
	Model       string          `json:"model"`
	Params      json.RawMessage `json:"params"`
	Status      string          `json:"status"`
	Total       int             `json:"total"`
	Succeeded   int             `json:"succeeded"`
	Failed      int             `json:"failed"`
	Scored      int             `json:"scored"`
	Matched     int             `json:"matched"`
	WebhookURL  string          `json:"webhook_url,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	APIKeyID    uuid.UUID       `json:"-"`
}

// Result is one finished (or dead) item.
type Result struct {
	Index            int     `json:"index"`
	Ref              string  `json:"ref,omitempty"`
	Status           string  `json:"status"`
	Prompt           string  `json:"prompt,omitempty"`
	Output           *string `json:"output,omitempty"`
	Expected         *string `json:"expected,omitempty"`
	Matched          *bool   `json:"matched,omitempty"`
	Cached           bool    `json:"cached"`
	Attempts         int     `json:"attempts"`
	PromptTokens     *int    `json:"prompt_tokens,omitempty"`
	CompletionTokens *int    `json:"completion_tokens,omitempty"`
	LatencyMS        *int    `json:"latency_ms,omitempty"`
	Error            *string `json:"error,omitempty"`
}

// Batches is the batch repository.
type Batches struct{ Pool *pgxpool.Pool }

const batchCols = `id, api_key_id, model, params, status, total, succeeded, failed, scored, matched,
	COALESCE(webhook_url, ''), created_at, completed_at`

func scanBatch(row pgx.Row) (Batch, error) {
	var b Batch
	err := row.Scan(&b.ID, &b.APIKeyID, &b.Model, &b.Params, &b.Status, &b.Total, &b.Succeeded, &b.Failed,
		&b.Scored, &b.Matched, &b.WebhookURL, &b.CreatedAt, &b.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, ErrNotFound
	}
	return b, err
}

// Create inserts a batch and its items in one transaction. If the idempotency
// key was already used by the same API key with an identical request hash, the
// original batch is returned with replayed=true. A different hash yields
// ErrIdempotencyConflict.
func (s Batches) Create(ctx context.Context, nb NewBatch) (b Batch, replayed bool, err error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return b, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	id := uuid.New()
	var idem any
	if nb.IdempotencyKey != "" {
		idem = nb.IdempotencyKey
	}
	var webhook any
	if nb.WebhookURL != "" {
		webhook = nb.WebhookURL
	}
	params := nb.Params
	if len(params) == 0 {
		params = json.RawMessage(`{}`)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO batches (id, api_key_id, model, params, total, webhook_url, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (api_key_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		id, nb.APIKeyID, nb.Model, params, len(nb.Items), webhook, idem, nb.RequestHash)
	if err != nil {
		return b, false, err
	}

	if tag.RowsAffected() == 0 {
		// Another request with this key already won. Compare request hashes.
		var existingID uuid.UUID
		var existingHash []byte
		if err := tx.QueryRow(ctx, `SELECT id, request_hash FROM batches WHERE api_key_id = $1 AND idempotency_key = $2`,
			nb.APIKeyID, nb.IdempotencyKey).Scan(&existingID, &existingHash); err != nil {
			return b, false, err
		}
		if !bytes.Equal(existingHash, nb.RequestHash) {
			return b, false, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return b, false, err
		}
		b, err = s.Get(ctx, existingID)
		return b, true, err
	}

	maxAttempts := nb.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	rows := make([][]any, len(nb.Items))
	for i, it := range nb.Items {
		var ref any
		if it.Ref != "" {
			ref = it.Ref
		}
		rows[i] = []any{id, i, ref, it.Prompt, it.Expected, maxAttempts}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"items"},
		[]string{"batch_id", "idx", "ref", "prompt", "expected", "max_attempts"}, pgx.CopyFromRows(rows)); err != nil {
		return b, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return b, false, err
	}
	b, err = s.Get(ctx, id)
	return b, false, err
}

// Get returns a batch by id without an ownership check.
func (s Batches) Get(ctx context.Context, id uuid.UUID) (Batch, error) {
	return scanBatch(s.Pool.QueryRow(ctx, `SELECT `+batchCols+` FROM batches WHERE id = $1`, id))
}

// List returns batches for an API key, newest first. A nil owner lists all.
func (s Batches) List(ctx context.Context, owner *uuid.UUID, limit int) ([]Batch, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+batchCols+` FROM batches
		WHERE ($1::uuid IS NULL OR api_key_id = $1) ORDER BY created_at DESC LIMIT $2`, owner, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Batch{}
	for rows.Next() {
		b, err := scanBatch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// Results returns finished items with idx greater than after, ordered by idx.
// Items that are still queued or leased are skipped.
func (s Batches) Results(ctx context.Context, batchID uuid.UUID, after, limit int, includePrompt bool) ([]Result, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT idx, COALESCE(ref, ''), status, CASE WHEN $4 THEN prompt END, result, expected, matched, cached,
		       attempts, prompt_tokens, completion_tokens, latency_ms, last_error
		FROM items
		WHERE batch_id = $1 AND idx > $2 AND status IN ('succeeded', 'dead')
		ORDER BY idx LIMIT $3`, batchID, after, limit, includePrompt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Result{}
	for rows.Next() {
		var r Result
		var prompt *string
		if err := rows.Scan(&r.Index, &r.Ref, &r.Status, &prompt, &r.Output, &r.Expected, &r.Matched, &r.Cached,
			&r.Attempts, &r.PromptTokens, &r.CompletionTokens, &r.LatencyMS, &r.Error); err != nil {
			return nil, err
		}
		if prompt != nil {
			r.Prompt = *prompt
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ReplayDead moves every dead item of a batch back to the queue with a fresh
// attempt budget and reopens the batch. It returns the number of items replayed.
func (s Batches) ReplayDead(ctx context.Context, batchID uuid.UUID) (int, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		UPDATE items SET status = 'queued', attempts = 0, run_after = now(), lease_owner = NULL,
		       lease_expires_at = NULL, completed_at = NULL, last_error = NULL
		WHERE batch_id = $1 AND status = 'dead'`, batchID)
	if err != nil {
		return 0, err
	}
	n := int(tag.RowsAffected())
	if n > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE batches SET failed = failed - $2, status = 'running', completed_at = NULL
			WHERE id = $1`, batchID, n); err != nil {
			return 0, err
		}
	}
	return n, tx.Commit(ctx)
}

// HashRequest returns a stable hash of a request body for idempotency checks.
// The caller passes a canonical encoding (the re-marshalled parsed request).
func HashRequest(canonical []byte) []byte {
	h := sha256.Sum256(canonical)
	return h[:]
}
