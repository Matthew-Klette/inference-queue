package events_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Matthew-Klette/inference-queue/internal/events"
	"github.com/Matthew-Klette/inference-queue/internal/store"
	"github.com/Matthew-Klette/inference-queue/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func seedOutbox(t *testing.T, pool *pgxpool.Pool, url string) int64 {
	t.Helper()
	ctx := context.Background()
	k, err := store.Keys{Pool: pool}.Create(ctx, store.APIKey{Name: "k", KeyHash: []byte(uuid.NewString()), Scopes: []string{"admin"}, RateRPS: 1, RateBurst: 1})
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := store.Batches{Pool: pool}.Create(ctx, store.NewBatch{APIKeyID: k.ID, Model: "m",
		Items: []store.NewItem{{Prompt: "p"}}, RequestHash: []byte("h")})
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	err = pool.QueryRow(ctx, `INSERT INTO outbox (batch_id, event_type, payload, webhook_url)
		VALUES ($1, 'batch.completed', '{"batch_id":"x","status":"completed"}', $2) RETURNING id`, b.ID, url).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func state(t *testing.T, pool *pgxpool.Pool, id int64) (delivered bool, dead bool, attempts int) {
	t.Helper()
	if err := pool.QueryRow(context.Background(),
		`SELECT delivered_at IS NOT NULL, dead, attempts FROM outbox WHERE id = $1`, id).Scan(&delivered, &dead, &attempts); err != nil {
		t.Fatal(err)
	}
	return
}

func TestDeliversSignedEvent(t *testing.T) {
	pool := testutil.NewDB(t)
	var mu sync.Mutex
	var body []byte
	var hdr http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ = io.ReadAll(r.Body)
		hdr = r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	id := seedOutbox(t, pool, srv.URL)

	p := &events.Publisher{Pool: pool, Secret: "whsec"}
	n, err := p.Once(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("Once = %d, %v", n, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if err := events.Verify("whsec", hdr.Get("X-IQ-Signature"), body, time.Now(), time.Minute); err != nil {
		t.Fatalf("signature does not verify: %v", err)
	}
	if hdr.Get("X-IQ-Event") != "batch.completed" || hdr.Get("X-IQ-Event-Id") == "" {
		t.Fatalf("headers = %v", hdr)
	}
	if delivered, _, attempts := state(t, pool, id); !delivered || attempts != 1 {
		t.Fatalf("delivered=%v attempts=%d", delivered, attempts)
	}
	if n, _ := p.Once(context.Background()); n != 0 {
		t.Fatal("a delivered event must not be sent again")
	}
}

func TestRetriesFailuresThenDelivers(t *testing.T) {
	pool := testutil.NewDB(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	id := seedOutbox(t, pool, srv.URL)
	p := &events.Publisher{Pool: pool, BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}

	for i := 0; i < 2; i++ {
		_, _ = p.Once(context.Background())
		if delivered, _, _ := state(t, pool, id); delivered {
			t.Fatal("delivered too early")
		}
		time.Sleep(40 * time.Millisecond) // let the backoff elapse
	}
	_, _ = p.Once(context.Background())
	if delivered, dead, attempts := state(t, pool, id); !delivered || dead || attempts != 3 {
		t.Fatalf("delivered=%v dead=%v attempts=%d", delivered, dead, attempts)
	}
}

func TestBackoffDelaysRedelivery(t *testing.T) {
	pool := testutil.NewDB(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	seedOutbox(t, pool, srv.URL)
	p := &events.Publisher{Pool: pool, BackoffBase: time.Hour, BackoffMax: time.Hour}
	_, _ = p.Once(context.Background())
	if n, _ := p.Once(context.Background()); n != 0 || calls.Load() != 1 {
		t.Fatalf("event retried before its backoff elapsed: n=%d calls=%d", n, calls.Load())
	}
}

func TestDeadLettersAfterMaxAttempts(t *testing.T) {
	pool := testutil.NewDB(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusGone) }))
	defer srv.Close()
	id := seedOutbox(t, pool, srv.URL)
	p := &events.Publisher{Pool: pool, MaxAttempts: 2, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond}
	for i := 0; i < 4; i++ {
		_, _ = p.Once(context.Background())
		time.Sleep(10 * time.Millisecond)
	}
	if delivered, dead, attempts := state(t, pool, id); delivered || !dead || attempts != 2 {
		t.Fatalf("delivered=%v dead=%v attempts=%d", delivered, dead, attempts)
	}
}

func TestTwoPublishersNeverDeliverTheSameEventTwice(t *testing.T) {
	pool := testutil.NewDB(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()
	seedOutbox(t, pool, srv.URL)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = (&events.Publisher{Pool: pool}).Once(context.Background())
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("event delivered %d times by concurrent publishers", calls.Load())
	}
}

func TestSafeClientBlocksPrivateAddresses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close() // listens on 127.0.0.1

	_, err := events.SafeClient(time.Second, false).Get(srv.URL)
	if err == nil || !errors.Is(err, events.ErrPrivateAddress) {
		t.Fatalf("loopback webhook must be refused, got %v", err)
	}
	res, err := events.SafeClient(time.Second, true).Get(srv.URL)
	if err != nil {
		t.Fatalf("allowPrivate: %v", err)
	}
	res.Body.Close()
}
