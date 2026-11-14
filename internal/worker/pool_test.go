package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Matthew-Klette/inference-queue/internal/cache"
	"github.com/Matthew-Klette/inference-queue/internal/model"
	"github.com/Matthew-Klette/inference-queue/internal/obs"
	"github.com/Matthew-Klette/inference-queue/internal/queue"
	"github.com/Matthew-Klette/inference-queue/internal/store"
	"github.com/Matthew-Klette/inference-queue/internal/testutil"
	"github.com/Matthew-Klette/inference-queue/internal/worker"
)

func TestMain(m *testing.M) { testutil.Main(m) }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

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

func (e env) submit(t *testing.T, prompts []string, maxAttempts int) store.Batch {
	t.Helper()
	items := make([]store.NewItem, len(prompts))
	for i, p := range prompts {
		items[i] = store.NewItem{Prompt: p}
	}
	b, _, err := store.Batches{Pool: e.pool}.Create(context.Background(), store.NewBatch{
		APIKeyID: e.owner, Model: "m", Items: items, RequestHash: []byte(uuid.NewString()), MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (e env) pool_(backend model.Backend, c cache.Cache, cfg worker.Config) *worker.Pool {
	return &worker.Pool{Queue: e.q, Backend: backend, Cache: c, Metrics: obs.NewMetrics(), Log: quiet, Cfg: cfg}
}

func (e env) wait(t *testing.T, id uuid.UUID) store.Batch {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		b, err := store.Batches{Pool: e.pool}.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if b.CompletedAt != nil {
			return b
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("batch did not complete in time")
	return store.Batch{}
}

// run starts the pool and returns a stop function that cancels and waits.
func run(t *testing.T, p *worker.Pool) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

// counting records how many times each prompt reached the model.
type counting struct {
	next  model.Backend
	mu    sync.Mutex
	calls map[string]int
}

func newCounting(next model.Backend) *counting { return &counting{next: next, calls: map[string]int{}} }

func (c *counting) Complete(ctx context.Context, r model.Request) (model.Response, error) {
	c.mu.Lock()
	c.calls[r.Prompt]++
	c.mu.Unlock()
	return c.next.Complete(ctx, r)
}

func (c *counting) total() (n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.calls {
		n += v
	}
	return
}

func TestEveryItemProcessedExactlyOnceUnderConcurrency(t *testing.T) {
	e := setup(t)
	const items, pools, perPool = 400, 4, 4 // 16 concurrent workers across 4 pools
	prompts := make([]string, items)
	for i := range prompts {
		prompts[i] = fmt.Sprintf("prompt-%04d", i)
	}
	b := e.submit(t, prompts, 3)

	backend := newCounting(model.NewFake(model.FakeConfig{Latency: 3 * time.Millisecond, Jitter: 2 * time.Millisecond, Seed: 1}))
	for i := 0; i < pools; i++ {
		run(t, e.pool_(backend, nil, worker.Config{Owner: fmt.Sprintf("w%d", i), Concurrency: perPool,
			Poll: 20 * time.Millisecond, Lease: 30 * time.Second}))
	}
	done := e.wait(t, b.ID)

	if done.Succeeded != items || done.Failed != 0 || done.Status != "completed" {
		t.Fatalf("batch = %+v", done)
	}
	if backend.total() != items {
		t.Fatalf("model called %d times for %d items (no retries, no cache: must be equal)", backend.total(), items)
	}
	for p, n := range backend.calls {
		if n != 1 {
			t.Errorf("prompt %s processed %d times", p, n)
		}
	}
	var wrong int
	_ = e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM items WHERE status <> 'succeeded' OR attempts <> 1 OR result IS NULL`).Scan(&wrong)
	if wrong != 0 {
		t.Fatalf("%d items are not (succeeded, attempts=1, result set)", wrong)
	}
}

// A worker that dies mid-lease never completes or heartbeats. The item must be
// picked up by a healthy worker once the lease expires, and processed once.
func TestCrashedWorkerItemIsRetriedAfterLeaseExpiry(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"survivor"}, 3)

	crashed, err := e.q.Lease(context.Background(), "crashed-worker", 1, 400*time.Millisecond)
	if err != nil || len(crashed) != 1 {
		t.Fatalf("lease: %v %d", err, len(crashed))
	}
	backend := newCounting(model.NewFake(model.FakeConfig{}))
	run(t, e.pool_(backend, nil, worker.Config{Owner: "healthy", Concurrency: 2, Poll: 20 * time.Millisecond, Lease: 5 * time.Second}))

	time.Sleep(150 * time.Millisecond)
	if backend.total() != 0 {
		t.Fatal("item was taken while the crashed worker's lease was still valid")
	}
	done := e.wait(t, b.ID)
	if done.Succeeded != 1 {
		t.Fatalf("batch = %+v", done)
	}
	var attempts int
	var lastErr *string
	_ = e.pool.QueryRow(context.Background(), `SELECT attempts, last_error FROM items`).Scan(&attempts, &lastErr)
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (crashed attempt + successful retry)", attempts)
	}
	if backend.calls["survivor"] != 1 {
		t.Fatalf("model calls = %d", backend.calls["survivor"])
	}
}

// A crashed worker on the final attempt is dead-lettered by the reaper rather than retried forever.
func TestCrashOnFinalAttemptIsDeadLettered(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"poison"}, 1)
	if _, err := e.q.Lease(context.Background(), "crashed", 1, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	run(t, e.pool_(model.NewFake(model.FakeConfig{}), nil, worker.Config{Owner: "w", Concurrency: 1,
		Poll: 20 * time.Millisecond, ReapInterval: 50 * time.Millisecond}))
	done := e.wait(t, b.ID)
	if done.Failed != 1 || done.Status != "completed_with_errors" {
		t.Fatalf("batch = %+v", done)
	}
}

type scripted struct {
	mu    sync.Mutex
	fails map[string]int // prompt -> remaining transient failures
}

func (s *scripted) Complete(_ context.Context, r model.Request) (model.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fails[r.Prompt] > 0 {
		s.fails[r.Prompt]--
		return model.Response{}, errors.New("upstream 503")
	}
	return model.Response{Text: "ok:" + r.Prompt}, nil
}

func TestTransientFailuresRetryWithBackoffThenSucceed(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"flaky", "steady"}, 5)
	run(t, e.pool_(&scripted{fails: map[string]int{"flaky": 2}}, nil, worker.Config{
		Concurrency: 2, Poll: 10 * time.Millisecond, BackoffBase: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond}))
	done := e.wait(t, b.ID)
	if done.Succeeded != 2 || done.Failed != 0 {
		t.Fatalf("batch = %+v", done)
	}
	var attempts int
	_ = e.pool.QueryRow(context.Background(), `SELECT attempts FROM items WHERE prompt = 'flaky'`).Scan(&attempts)
	if attempts != 3 {
		t.Fatalf("flaky attempts = %d, want 3", attempts)
	}
}

func TestExhaustedRetriesAndPermanentErrorsGoToDeadLetterWithoutBlockingOthers(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"always-down", "bad [[permanent]]", "fine-1", "fine-2"}, 3)
	back := model.NewFake(model.FakeConfig{})
	sc := &scripted{fails: map[string]int{"always-down": 100}}
	mux := backendFunc(func(ctx context.Context, r model.Request) (model.Response, error) {
		if r.Prompt == "always-down" {
			return sc.Complete(ctx, r)
		}
		return back.Complete(ctx, r)
	})
	run(t, e.pool_(mux, nil, worker.Config{Concurrency: 2, Poll: 10 * time.Millisecond,
		BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}))
	done := e.wait(t, b.ID)
	if done.Succeeded != 2 || done.Failed != 2 || done.Status != "completed_with_errors" {
		t.Fatalf("batch = %+v", done)
	}
	rows, _ := e.pool.Query(context.Background(), `SELECT prompt, attempts FROM items WHERE status = 'dead'`)
	defer rows.Close()
	for rows.Next() {
		var p string
		var a int
		_ = rows.Scan(&p, &a)
		want := map[string]int{"always-down": 3, "bad [[permanent]]": 1}[p]
		if a != want {
			t.Errorf("%s: attempts = %d, want %d", p, a, want)
		}
	}
}

type backendFunc func(context.Context, model.Request) (model.Response, error)

func (f backendFunc) Complete(ctx context.Context, r model.Request) (model.Response, error) {
	return f(ctx, r)
}

func TestCacheServesDuplicatePrompts(t *testing.T) {
	e := setup(t)
	prompts := make([]string, 30)
	for i := range prompts {
		prompts[i] = fmt.Sprintf("dup-%d", i%3)
	}
	b := e.submit(t, prompts, 3)
	redisClient := testutil.NewRedis(t)
	backend := newCounting(model.NewFake(model.FakeConfig{Latency: 5 * time.Millisecond}))
	run(t, e.pool_(backend, cache.Redis{Client: redisClient}, worker.Config{Concurrency: 1, Poll: 10 * time.Millisecond}))
	done := e.wait(t, b.ID)
	if done.Succeeded != 30 {
		t.Fatalf("batch = %+v", done)
	}
	if backend.total() != 3 {
		t.Fatalf("model called %d times for 3 distinct prompts", backend.total())
	}
	var cached int
	_ = e.pool.QueryRow(context.Background(), `SELECT count(*) FROM items WHERE cached`).Scan(&cached)
	if cached != 27 {
		t.Fatalf("cached items = %d, want 27", cached)
	}
}

func TestConcurrentIdenticalPromptsShareOneModelCall(t *testing.T) {
	e := setup(t)
	prompts := make([]string, 8)
	for i := range prompts {
		prompts[i] = "same prompt"
	}
	b := e.submit(t, prompts, 3)
	backend := newCounting(model.NewFake(model.FakeConfig{Latency: 150 * time.Millisecond}))
	run(t, e.pool_(backend, cache.NewMemory(100), worker.Config{Concurrency: 8, Poll: 10 * time.Millisecond}))
	e.wait(t, b.ID)
	if backend.total() != 1 {
		t.Fatalf("model called %d times; concurrent duplicates should be collapsed (stampede protection)", backend.total())
	}
}

type failingCache struct{}

func (failingCache) Get(context.Context, string) (cache.Entry, bool, error) {
	return cache.Entry{}, false, errors.New("redis down")
}
func (failingCache) Set(context.Context, string, cache.Entry, time.Duration) error {
	return errors.New("redis down")
}

func TestCacheFailureFallsThroughToModel(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"a", "b"}, 3)
	run(t, e.pool_(model.NewFake(model.FakeConfig{}), failingCache{}, worker.Config{Concurrency: 2, Poll: 10 * time.Millisecond}))
	if done := e.wait(t, b.ID); done.Succeeded != 2 {
		t.Fatalf("batch = %+v", done)
	}
}

func TestOpenCircuitRequeuesWithoutChargingAttempts(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"x", "y"}, 2)
	var open atomic.Bool
	open.Store(true)
	backend := backendFunc(func(context.Context, model.Request) (model.Response, error) {
		if open.Load() {
			return model.Response{}, model.ErrCircuitOpen
		}
		return model.Response{Text: "ok"}, nil
	})
	run(t, e.pool_(backend, nil, worker.Config{Concurrency: 2, Poll: 10 * time.Millisecond, CircuitRequeue: 30 * time.Millisecond}))
	time.Sleep(400 * time.Millisecond) // many requeue rounds with only 2 attempts allowed
	open.Store(false)
	done := e.wait(t, b.ID)
	if done.Succeeded != 2 || done.Failed != 0 {
		t.Fatalf("batch = %+v; an outage must not burn the attempt budget", done)
	}
	var maxAttempts int
	_ = e.pool.QueryRow(context.Background(), `SELECT max(attempts) FROM items`).Scan(&maxAttempts)
	if maxAttempts != 1 {
		t.Fatalf("max attempts = %d, want 1", maxAttempts)
	}
}

func TestShutdownAbortsSlowItemsAndReleasesLeases(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"slow-1", "slow-2", "slow-3"}, 3)
	started := make(chan struct{}, 3)
	backend := backendFunc(func(ctx context.Context, r model.Request) (model.Response, error) {
		started <- struct{}{}
		<-ctx.Done() // never finishes on its own
		return model.Response{}, ctx.Err()
	})
	stop := run(t, e.pool_(backend, nil, worker.Config{Owner: "w", Concurrency: 3, Poll: 10 * time.Millisecond,
		DrainTimeout: 200 * time.Millisecond, Lease: time.Hour}))
	for i := 0; i < 3; i++ {
		<-started
	}
	begin := time.Now()
	stop()
	if d := time.Since(begin); d > 5*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
	var queued, leased, attempts int
	_ = e.pool.QueryRow(context.Background(), `
		SELECT count(*) FILTER (WHERE status = 'queued'), count(*) FILTER (WHERE status = 'leased'), sum(attempts)
		FROM items WHERE batch_id = $1`, b.ID).Scan(&queued, &leased, &attempts)
	if queued != 3 || leased != 0 || attempts != 0 {
		t.Fatalf("queued=%d leased=%d attempts=%d; leases must be released and attempts refunded", queued, leased, attempts)
	}
}

func TestShutdownDrainsInFlightWork(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"a", "b"}, 3)
	started := make(chan struct{}, 2)
	backend := backendFunc(func(ctx context.Context, r model.Request) (model.Response, error) {
		started <- struct{}{}
		select {
		case <-time.After(300 * time.Millisecond):
			return model.Response{Text: "done"}, nil
		case <-ctx.Done():
			return model.Response{}, ctx.Err()
		}
	})
	stop := run(t, e.pool_(backend, nil, worker.Config{Concurrency: 2, Poll: 10 * time.Millisecond, DrainTimeout: 5 * time.Second}))
	<-started
	<-started
	stop() // cancel while both calls are in flight
	got, _ := store.Batches{Pool: e.pool}.Get(context.Background(), b.ID)
	if got.Succeeded != 2 || got.Status != "completed" {
		t.Fatalf("batch = %+v; in-flight items should finish during the drain window", got)
	}
}

func TestSlowCallsKeepTheirLeaseViaHeartbeat(t *testing.T) {
	e := setup(t)
	b := e.submit(t, []string{"slow"}, 3)
	backend := newCounting(model.NewFake(model.FakeConfig{Latency: 900 * time.Millisecond}))
	// Lease (300ms) is shorter than the model call. A second worker polling aggressively must not steal it.
	run(t, e.pool_(backend, nil, worker.Config{Owner: "a", Concurrency: 1, Poll: 10 * time.Millisecond, Lease: 300 * time.Millisecond}))
	run(t, e.pool_(backend, nil, worker.Config{Owner: "b", Concurrency: 1, Poll: 10 * time.Millisecond, Lease: 300 * time.Millisecond}))
	e.wait(t, b.ID)
	if backend.total() != 1 {
		t.Fatalf("model called %d times; heartbeat should have kept the lease", backend.total())
	}
}
