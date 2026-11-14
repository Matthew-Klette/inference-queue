package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Matthew-Klette/inference-queue/internal/store"
	"github.com/Matthew-Klette/inference-queue/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func newKey(t *testing.T, pool *pgxpool.Pool, name string) store.APIKey {
	t.Helper()
	k, err := store.Keys{Pool: pool}.Create(context.Background(), store.APIKey{
		Name: name, KeyHash: []byte(uuid.NewString()), Scopes: []string{"submit"}, RateRPS: 1, RateBurst: 1})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func nb(owner uuid.UUID, idem string, hash string) store.NewBatch {
	return store.NewBatch{APIKeyID: owner, Model: "m", IdempotencyKey: idem, RequestHash: store.HashRequest([]byte(hash)),
		Items: []store.NewItem{{Prompt: "a"}, {Prompt: "b"}}}
}

func TestMigrateIsIdempotent(t *testing.T) {
	pool := testutil.NewDB(t)
	if err := store.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&n)
	if n != 1 {
		t.Fatalf("recorded %d migrations", n)
	}
}

func TestIdempotentCreate(t *testing.T) {
	pool := testutil.NewDB(t)
	s := store.Batches{Pool: pool}
	ctx := context.Background()
	k1, k2 := newKey(t, pool, "one"), newKey(t, pool, "two")

	first, replayed, err := s.Create(ctx, nb(k1.ID, "abc", "body-1"))
	if err != nil || replayed {
		t.Fatalf("first: replayed=%v err=%v", replayed, err)
	}
	again, replayed, err := s.Create(ctx, nb(k1.ID, "abc", "body-1"))
	if err != nil || !replayed || again.ID != first.ID {
		t.Fatalf("same key + same body: id=%v replayed=%v err=%v", again.ID, replayed, err)
	}
	if _, _, err := s.Create(ctx, nb(k1.ID, "abc", "body-2")); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("same key + different body: %v", err)
	}
	other, replayed, err := s.Create(ctx, nb(k2.ID, "abc", "body-1"))
	if err != nil || replayed || other.ID == first.ID {
		t.Fatalf("idempotency keys are scoped per API key: %v %v %v", other.ID, replayed, err)
	}

	var batches, items int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM batches`).Scan(&batches)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM items`).Scan(&items)
	if batches != 2 || items != 4 {
		t.Fatalf("batches=%d items=%d; a replay must not create rows", batches, items)
	}
}

func TestEmptyIdempotencyKeysNeverCollide(t *testing.T) {
	pool := testutil.NewDB(t)
	s := store.Batches{Pool: pool}
	k := newKey(t, pool, "k")
	for i := 0; i < 3; i++ {
		if _, replayed, err := s.Create(context.Background(), nb(k.ID, "", "same")); err != nil || replayed {
			t.Fatalf("request %d: replayed=%v err=%v", i, replayed, err)
		}
	}
}

func TestConcurrentIdempotentCreatesYieldOneBatch(t *testing.T) {
	pool := testutil.NewDB(t)
	s := store.Batches{Pool: pool}
	k := newKey(t, pool, "k")
	const n = 12
	ids := make(chan uuid.UUID, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _, err := s.Create(context.Background(), nb(k.ID, "race", "body"))
			if err != nil {
				t.Error(err)
				return
			}
			ids <- b.ID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[uuid.UUID]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("%d distinct batches from %d concurrent identical requests", len(seen), n)
	}
}

func TestKeysLifecycle(t *testing.T) {
	pool := testutil.NewDB(t)
	keys := store.Keys{Pool: pool}
	ctx := context.Background()
	k := newKey(t, pool, "k")
	if _, err := keys.ByHash(ctx, k.KeyHash); err != nil {
		t.Fatal(err)
	}
	if err := keys.Revoke(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.ByHash(ctx, k.KeyHash); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("revoked key must not resolve: %v", err)
	}
	if err := keys.Revoke(ctx, uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("revoking unknown key: %v", err)
	}
	if err := keys.EnsureBootstrap(ctx, store.APIKey{Name: "b", KeyHash: []byte("h"), Scopes: []string{"admin"}, RateRPS: 1, RateBurst: 1}); err != nil {
		t.Fatal(err)
	}
	if err := keys.EnsureBootstrap(ctx, store.APIKey{Name: "b", KeyHash: []byte("h"), Scopes: []string{"admin"}, RateRPS: 1, RateBurst: 1}); err != nil {
		t.Fatalf("bootstrap must be idempotent: %v", err)
	}
}
