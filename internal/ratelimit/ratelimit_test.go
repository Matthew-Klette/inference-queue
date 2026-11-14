package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Matthew-Klette/inference-queue/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func TestMemoryBurstThenRefill(t *testing.T) {
	m := NewMemory()
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if ok, _, _ := m.Allow(ctx, "k", 2, 3); !ok {
			t.Fatalf("request %d within burst was denied", i)
		}
	}
	ok, wait, _ := m.Allow(ctx, "k", 2, 3)
	if ok {
		t.Fatal("request beyond burst was allowed")
	}
	if wait != 500*time.Millisecond {
		t.Fatalf("retry-after = %v, want 500ms (1 token at 2/s)", wait)
	}
	now = now.Add(500 * time.Millisecond)
	if ok, _, _ := m.Allow(ctx, "k", 2, 3); !ok {
		t.Fatal("token should have refilled after 500ms")
	}
	if ok, _, _ := m.Allow(ctx, "k", 2, 3); ok {
		t.Fatal("only one token should have refilled")
	}
}

func TestMemoryKeysAreIndependentAndRefillCapsAtBurst(t *testing.T) {
	m := NewMemory()
	now := time.Unix(0, 0)
	m.now = func() time.Time { return now }
	ctx := context.Background()
	m.Allow(ctx, "a", 1, 1)
	if ok, _, _ := m.Allow(ctx, "b", 1, 1); !ok {
		t.Fatal("key b must have its own bucket")
	}
	now = now.Add(time.Hour)
	m.Allow(ctx, "a", 1, 2)
	m.Allow(ctx, "a", 1, 2)
	if ok, _, _ := m.Allow(ctx, "a", 1, 2); ok {
		t.Fatal("tokens must not accumulate beyond burst")
	}
}

func TestRedisBucketMatchesMemoryBehaviour(t *testing.T) {
	mr, client := testutil.NewMiniRedis(t)
	r := Redis{Client: client, Prefix: "t:"}
	ctx := context.Background()
	mr.SetTime(time.Unix(2000, 0))

	for i := 0; i < 3; i++ {
		ok, _, err := r.Allow(ctx, "k", 2, 3)
		if err != nil || !ok {
			t.Fatalf("request %d: ok=%v err=%v", i, ok, err)
		}
	}
	ok, wait, err := r.Allow(ctx, "k", 2, 3)
	if err != nil || ok {
		t.Fatalf("expected denial, ok=%v err=%v", ok, err)
	}
	if wait != 500*time.Millisecond {
		t.Fatalf("retry-after = %v, want 500ms", wait)
	}
	mr.SetTime(time.Unix(2000, 0).Add(time.Second))
	if ok, _, _ := r.Allow(ctx, "k", 2, 3); !ok {
		t.Fatal("expected refill after one second")
	}
	if ttl := mr.TTL("t:k"); ttl <= 0 {
		t.Fatal("bucket keys must expire so idle keys do not accumulate")
	}
}

type failing struct{}

func (failing) Allow(context.Context, string, float64, int) (bool, time.Duration, error) {
	return false, 0, errors.New("redis down")
}

func TestFailOpenFallsBackToMemory(t *testing.T) {
	var errs int
	l := FailOpen{Primary: failing{}, Fallback: NewMemory(), OnError: func(error) { errs++ }}
	ok, _, err := l.Allow(context.Background(), "k", 1, 1)
	if err != nil || !ok {
		t.Fatalf("expected fallback to allow: ok=%v err=%v", ok, err)
	}
	if ok, _, _ := l.Allow(context.Background(), "k", 1, 1); ok {
		t.Fatal("fallback limiter should still enforce the limit")
	}
	if errs != 2 {
		t.Fatalf("OnError called %d times", errs)
	}
}
