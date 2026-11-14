package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// APIKey is the stored record for a key. The plaintext key is never stored.
type APIKey struct {
	ID        uuid.UUID  `json:"id"`
	Name      string     `json:"name"`
	KeyHash   []byte     `json:"-"`
	Scopes    []string   `json:"scopes"`
	RateRPS   float64    `json:"rate_rps"`
	RateBurst int        `json:"rate_burst"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// ErrNotFound is returned when a row does not exist or is not visible to the caller.
var ErrNotFound = errors.New("not found")

// Keys is the API key repository.
type Keys struct{ Pool *pgxpool.Pool }

// Create inserts a new key record.
func (k Keys) Create(ctx context.Context, key APIKey) (APIKey, error) {
	if key.ID == uuid.Nil {
		key.ID = uuid.New()
	}
	err := k.Pool.QueryRow(ctx, `
		INSERT INTO api_keys (id, name, key_hash, scopes, rate_rps, rate_burst)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at`,
		key.ID, key.Name, key.KeyHash, key.Scopes, key.RateRPS, key.RateBurst).Scan(&key.CreatedAt)
	return key, err
}

// EnsureBootstrap inserts the key if its hash is not present yet. It is used
// to seed the first admin key from the environment.
func (k Keys) EnsureBootstrap(ctx context.Context, key APIKey) error {
	key.ID = uuid.New()
	_, err := k.Pool.Exec(ctx, `
		INSERT INTO api_keys (id, name, key_hash, scopes, rate_rps, rate_burst)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (key_hash) DO NOTHING`,
		key.ID, key.Name, key.KeyHash, key.Scopes, key.RateRPS, key.RateBurst)
	return err
}

// ByHash looks up an active key by the sha256 of its plaintext.
func (k Keys) ByHash(ctx context.Context, hash []byte) (APIKey, error) {
	var out APIKey
	err := k.Pool.QueryRow(ctx, `
		SELECT id, name, key_hash, scopes, rate_rps, rate_burst, created_at, revoked_at
		FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, hash).
		Scan(&out.ID, &out.Name, &out.KeyHash, &out.Scopes, &out.RateRPS, &out.RateBurst, &out.CreatedAt, &out.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	return out, err
}

// List returns all keys, newest first.
func (k Keys) List(ctx context.Context) ([]APIKey, error) {
	rows, err := k.Pool.Query(ctx, `
		SELECT id, name, key_hash, scopes, rate_rps, rate_burst, created_at, revoked_at
		FROM api_keys ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		var a APIKey
		if err := rows.Scan(&a.ID, &a.Name, &a.KeyHash, &a.Scopes, &a.RateRPS, &a.RateBurst, &a.CreatedAt, &a.RevokedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Revoke marks a key as revoked. It is idempotent.
func (k Keys) Revoke(ctx context.Context, id uuid.UUID) error {
	tag, err := k.Pool.Exec(ctx, `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
