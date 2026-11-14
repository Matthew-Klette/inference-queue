package queue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Matthew-Klette/inference-queue/internal/queue"
	"github.com/Matthew-Klette/inference-queue/internal/store"
	"github.com/Matthew-Klette/inference-queue/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

type env struct {
	pool  *pgxpool.Pool
	q     *queue.Queue
	owner uuid.UUID
}

func setup(t *testing.T) env {
	t.Helper()
	pool := testutil.NewDB(t)
	k, err := store.Keys{Pool: pool}.Create(context.Background(), store.APIKey{
		Name: "t", KeyHash: []byte(uuid.NewString()), Scopes: []string{"admin"}, RateRPS: 1, RateBurst: 1})
	if err != nil {
		t.Fatal(err)
	}
	return env{pool: pool, q: &queue.Queue{Pool: pool}, owner: k.ID}
}

func (e env) batch(t *testing.T, n int, maxAttempts int, mod func(*store.NewBatch)) store.Batch {
	t.Helper()
	items := make([]store.NewItem, n)
	for i := range items {
		items[i] = store.NewItem{Prompt: fmt.Sprintf("prompt %d", i)}
	}
	nb := store.NewBatch{APIKeyID: e.owner, Model: "m", Items: items, RequestHash: []byte(uuid.NewString()), MaxAttempts: maxAttempts}
	if mod != nil {
		mod(&nb)
	}
	b, _, err := store.Batches{Pool: e.pool}.Create(context.Background(), nb)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (e env) item(t *testing.T, id int64) (status string, attempts int, lastErr *string) {
	t.Helper()
	if err := e.pool.QueryRow(context.Background(), `SELECT status, attempts, last_error FROM items WHERE id = $1`, id).
		Scan(&status, &attempts, &lastErr); err != nil {
		t.Fatal(err)
	}
	return
}

func TestLeaseHandsOutEachItemOnce(t *testing.T) {
	e := setup(t)
	e.batch(t, 10, 3, nil)
	ctx := context.Background()

	a, err := e.q.Lease(ctx, "w1", 6, time.Minute)
	if err != nil || len(a) != 6 {
		t.Fatalf("lease a: %d %v", len(a), err)
	}
	b, _ := e.q.Lease(ctx, "w2", 6, time.Minute)
	if len(b) != 4 {
		t.Fatalf("lease b: got %d, want the remaining 4", len(b))
	}
	if c, _ := e.q.Lease(ctx, "w3", 6, time.Minute); len(c) != 0 {
		t.Fatalf("nothing should be left, got %d", len(c))
	}
	seen := map[int64]bool{}
	for _, it := range append(a, b...) {
		if seen[it.ID] {
			t.Fatalf("item %d leased twice", it.ID)
		}
		seen[it.ID] = true
		if it.Attempts != 1 || it.Model != "m" {
			t.Fatalf("unexpected item %+v", it)
		}
	}
	var status string
	_ = e.pool.QueryRow(ctx, `SELECT status FROM batches`).Scan(&status)
	if status != "running" {
		t.Fatalf("batch status = %s, want running", status)
	}
}

func TestConcurrentLeasersNeverOverlap(t *testing.T) {
	e := setup(t)
	const total = 300
	e.batch(t, total, 3, nil)
	var mu sync.Mutex
	seen := map[int64]string{}
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		owner := fmt.Sprintf("w%d", w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				items, err := e.q.Lease(context.Background(), owner, 7, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(items) == 0 {
					return
				}
				mu.Lock()
				for _, it := range items {
					if prev, dup := seen[it.ID]; dup {
						t.Errorf("item %d leased by %s and %s", it.ID, prev, owner)
					}
					seen[it.ID] = owner
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("leased %d of %d items", len(seen), total)
	}
}

func TestExpiredLeaseIsReclaimedAndAttemptCounted(t *testing.T) {
	e := setup(t)
	e.batch(t, 1, 3, nil)
	ctx := context.Background()

	first, _ := e.q.Lease(ctx, "dead-worker", 1, 150*time.Millisecond)
	if len(first) != 1 {
		t.Fatal("no item leased")
	}
	if got, _ := e.q.Lease(ctx, "w2", 1, time.Minute); len(got) != 0 {
		t.Fatal("item must not be available while its lease is valid")
	}
	time.Sleep(250 * time.Millisecond)
	second, err := e.q.Lease(ctx, "w2", 1, time.Minute)
	if err != nil || len(second) != 1 {
		t.Fatalf("expired lease not reclaimed: %v %d", err, len(second))
	}
	if second[0].ID != first[0].ID || second[0].Attempts != 2 {
		t.Fatalf("reclaimed %+v", second[0])
	}
	_, _, lastErr := e.item(t, second[0].ID)
	if lastErr == nil || *lastErr != "lease expired" {
		t.Fatalf("last_error = %v", lastErr)
	}
}

func TestStaleWorkerCannotOverwriteNewOwner(t *testing.T) {
	e := setup(t)
	b := e.batch(t, 1, 3, nil)
	ctx := context.Background()
	old, _ := e.q.Lease(ctx, "slow", 1, 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	cur, _ := e.q.Lease(ctx, "fast", 1, time.Minute)

	if err := e.q.Complete(ctx, old[0], queue.Completion{Output: "stale"}); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale complete: %v", err)
	}
	if err := e.q.Fail(ctx, old[0], errors.New("x"), true, 0); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale fail: %v", err)
	}
	if err := e.q.Complete(ctx, cur[0], queue.Completion{Output: "fresh"}); err != nil {
		t.Fatal(err)
	}
	var out string
	var succeeded int
	_ = e.pool.QueryRow(ctx, `SELECT result FROM items`).Scan(&out)
	_ = e.pool.QueryRow(ctx, `SELECT succeeded FROM batches WHERE id = $1`, b.ID).Scan(&succeeded)
	if out != "fresh" || succeeded != 1 {
		t.Fatalf("result=%q succeeded=%d; counters must count the item exactly once", out, succeeded)
	}
}

func TestRetryBackoffThenDeadLetterAfterMaxAttempts(t *testing.T) {
	e := setup(t)
	b := e.batch(t, 1, 2, nil)
	ctx := context.Background()

	l1, _ := e.q.Lease(ctx, "w", 1, time.Minute)
	if err := e.q.Fail(ctx, l1[0], errors.New("boom 1"), true, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.q.Lease(ctx, "w", 1, time.Minute); len(got) != 0 {
		t.Fatal("item must wait out its backoff")
	}
	time.Sleep(400 * time.Millisecond)
	l2, _ := e.q.Lease(ctx, "w", 1, time.Minute)
	if len(l2) != 1 || l2[0].Attempts != 2 {
		t.Fatalf("second attempt: %+v", l2)
	}
	if err := e.q.Fail(ctx, l2[0], errors.New("boom 2"), true, time.Second); err != nil {
		t.Fatal(err)
	}
	status, attempts, lastErr := e.item(t, l2[0].ID)
	if status != "dead" || attempts != 2 || lastErr == nil || *lastErr != "boom 2" {
		t.Fatalf("status=%s attempts=%d err=%v", status, attempts, lastErr)
	}
	got, _ := store.Batches{Pool: e.pool}.Get(ctx, b.ID)
	if got.Status != "completed_with_errors" || got.Failed != 1 || got.CompletedAt == nil {
		t.Fatalf("batch = %+v", got)
	}
}

func TestPermanentFailureDeadLettersImmediately(t *testing.T) {
	e := setup(t)
	e.batch(t, 1, 5, nil)
	l, _ := e.q.Lease(context.Background(), "w", 1, time.Minute)
	if err := e.q.Fail(context.Background(), l[0], errors.New("rejected"), false, 0); err != nil {
		t.Fatal(err)
	}
	if status, attempts, _ := e.item(t, l[0].ID); status != "dead" || attempts != 1 {
		t.Fatalf("status=%s attempts=%d", status, attempts)
	}
}

func TestReapDeadLettersExpiredFinalAttempt(t *testing.T) {
	e := setup(t)
	b := e.batch(t, 1, 1, nil)
	ctx := context.Background()
	l, _ := e.q.Lease(ctx, "crashed", 1, 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	if got, _ := e.q.Lease(ctx, "w", 1, time.Minute); len(got) != 0 {
		t.Fatal("an item with no attempts left must not be leased again")
	}
	n, err := e.q.ReapExpired(ctx)
	if err != nil || n != 1 {
		t.Fatalf("reaped %d, %v", n, err)
	}
	if status, _, _ := e.item(t, l[0].ID); status != "dead" {
		t.Fatalf("status = %s", status)
	}
	got, _ := store.Batches{Pool: e.pool}.Get(ctx, b.ID)
	if got.Failed != 1 || got.Status != "completed_with_errors" {
		t.Fatalf("batch = %+v", got)
	}
}

func TestRequeueAndReleaseDoNotChargeAttempts(t *testing.T) {
	e := setup(t)
	e.batch(t, 2, 3, nil)
	ctx := context.Background()
	ls, _ := e.q.Lease(ctx, "w", 2, time.Minute)

	if err := e.q.Requeue(ctx, ls[0], 0); err != nil {
		t.Fatal(err)
	}
	n, err := e.q.Release(ctx, "w")
	if err != nil || n != 1 {
		t.Fatalf("released %d, %v", n, err)
	}
	for _, l := range ls {
		if status, attempts, _ := e.item(t, l.ID); status != "queued" || attempts != 0 {
			t.Fatalf("item %d: status=%s attempts=%d", l.ID, status, attempts)
		}
	}
}

func TestExtendKeepsLeaseAlive(t *testing.T) {
	e := setup(t)
	e.batch(t, 1, 3, nil)
	ctx := context.Background()
	l, _ := e.q.Lease(ctx, "w", 1, 200*time.Millisecond)
	for i := 0; i < 4; i++ {
		time.Sleep(100 * time.Millisecond)
		if err := e.q.Extend(ctx, "w", []int64{l[0].ID}, 200*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := e.q.Lease(ctx, "other", 1, time.Minute); len(got) != 0 {
		t.Fatal("a heartbeating lease must not be reclaimed")
	}
}

func TestCompletionWritesOutboxExactlyOnce(t *testing.T) {
	e := setup(t)
	e.q.DefaultWebhook = "http://example.invalid/default"
	b := e.batch(t, 2, 3, func(nb *store.NewBatch) { nb.WebhookURL = "http://example.invalid/own" })
	ctx := context.Background()
	ls, _ := e.q.Lease(ctx, "w", 2, time.Minute)

	_ = e.q.Complete(ctx, ls[0], queue.Completion{Output: "a"})
	var n int
	_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n)
	if n != 0 {
		t.Fatalf("outbox has %d rows before the batch finished", n)
	}
	_ = e.q.Complete(ctx, ls[1], queue.Completion{Output: "b"})
	_ = e.q.Complete(ctx, ls[1], queue.Completion{Output: "b"}) // duplicate attempt: lease lost
	var url, typ string
	_ = e.pool.QueryRow(ctx, `SELECT count(*), max(webhook_url), max(event_type) FROM outbox WHERE batch_id = $1`, b.ID).Scan(&n, &url, &typ)
	if n != 1 || url != "http://example.invalid/own" || typ != "batch.completed" {
		t.Fatalf("outbox n=%d url=%s type=%s", n, url, typ)
	}
}

func TestEvalScoring(t *testing.T) {
	e := setup(t)
	exp := func(s string) *string { return &s }
	b := e.batch(t, 0, 3, func(nb *store.NewBatch) {
		nb.Items = []store.NewItem{{Prompt: "p1", Expected: exp("yes")}, {Prompt: "p2", Expected: exp("no")}, {Prompt: "p3"}}
	})
	ctx := context.Background()
	ls, _ := e.q.Lease(ctx, "w", 3, time.Minute)
	for _, l := range ls {
		out := "yes "
		if l.Prompt == "p2" {
			out = "maybe"
		}
		if err := e.q.Complete(ctx, l, queue.Completion{Output: out}); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := store.Batches{Pool: e.pool}.Get(ctx, b.ID)
	if got.Scored != 2 || got.Matched != 1 || got.Status != "completed" {
		t.Fatalf("batch = %+v", got)
	}
}

func TestReplayDeadRequeuesAndReopensBatch(t *testing.T) {
	e := setup(t)
	b := e.batch(t, 2, 1, nil)
	ctx := context.Background()
	ls, _ := e.q.Lease(ctx, "w", 2, time.Minute)
	_ = e.q.Complete(ctx, ls[0], queue.Completion{Output: "ok"})
	_ = e.q.Fail(ctx, ls[1], errors.New("x"), true, 0)

	n, err := store.Batches{Pool: e.pool}.ReplayDead(ctx, b.ID)
	if err != nil || n != 1 {
		t.Fatalf("replayed %d, %v", n, err)
	}
	got, _ := store.Batches{Pool: e.pool}.Get(ctx, b.ID)
	if got.Status != "running" || got.Failed != 0 || got.CompletedAt != nil {
		t.Fatalf("batch = %+v", got)
	}
	again, _ := e.q.Lease(ctx, "w", 1, time.Minute)
	if len(again) != 1 || again[0].Attempts != 1 {
		t.Fatalf("replayed item must get a fresh attempt budget: %+v", again)
	}
	_ = e.q.Complete(ctx, again[0], queue.Completion{Output: "ok"})
	got, _ = store.Batches{Pool: e.pool}.Get(ctx, b.ID)
	if got.Status != "completed" || got.Succeeded != 2 {
		t.Fatalf("batch after replay = %+v", got)
	}
}

func TestDepth(t *testing.T) {
	e := setup(t)
	e.batch(t, 5, 1, nil)
	ctx := context.Background()
	ls, _ := e.q.Lease(ctx, "w", 2, time.Minute)
	_ = e.q.Fail(ctx, ls[0], errors.New("x"), false, 0)
	_ = e.q.Fail(ctx, ls[1], errors.New("x"), true, time.Hour) // attempts exhausted -> dead too
	d, err := e.q.Depth(ctx)
	if err != nil || d.Ready != 3 || d.Dead != 2 || d.Leased != 0 {
		t.Fatalf("depth = %+v %v", d, err)
	}
}
