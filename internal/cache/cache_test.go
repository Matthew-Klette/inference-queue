package cache

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Matthew-Klette/inference-queue/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func TestKeyIsStableAndSensitiveToEveryField(t *testing.T) {
	base := Key("m", []byte(`{"t":0}`), "hello")
	if base != Key("m", []byte(`{"t":0}`), "hello") {
		t.Fatal("key is not deterministic")
	}
	for name, other := range map[string]string{
		"model":  Key("m2", []byte(`{"t":0}`), "hello"),
		"params": Key("m", []byte(`{"t":1}`), "hello"),
		"prompt": Key("m", []byte(`{"t":0}`), "hello!"),
	} {
		if other == base {
			t.Errorf("changing %s did not change the key", name)
		}
	}
}

func TestKeyFieldBoundariesCannotCollide(t *testing.T) {
	if Key("ab", []byte("c"), "x") == Key("a", []byte("bc"), "x") {
		t.Fatal("model/params boundary collision")
	}
	if Key("m", []byte("ab"), "c") == Key("m", []byte("a"), "bc") {
		t.Fatal("params/prompt boundary collision")
	}
}

func TestMemoryCacheTTLAndCap(t *testing.T) {
	m := NewMemory(2)
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	ctx := context.Background()

	_ = m.Set(ctx, "a", Entry{Text: "A"}, time.Minute)
	if e, ok, _ := m.Get(ctx, "a"); !ok || e.Text != "A" {
		t.Fatalf("expected hit, got %v %v", e, ok)
	}
	now = now.Add(2 * time.Minute)
	if _, ok, _ := m.Get(ctx, "a"); ok {
		t.Fatal("expected expiry")
	}

	for _, k := range []string{"x", "y", "z"} {
		_ = m.Set(ctx, k, Entry{Text: k}, time.Hour)
	}
	if n := len(m.entries); n > 2 {
		t.Fatalf("cache grew past its cap: %d", n)
	}
	if _, ok, _ := m.Get(ctx, "z"); !ok {
		t.Fatal("most recent entry must be present")
	}
}

func TestRedisCacheRoundTripAndTTL(t *testing.T) {
	mr, client := testutil.NewMiniRedis(t)
	c := Redis{Client: client}
	ctx := context.Background()

	if _, ok, err := c.Get(ctx, "missing"); ok || err != nil {
		t.Fatalf("miss: ok=%v err=%v", ok, err)
	}
	want := Entry{Text: "hi", PromptTokens: 3, CompletionTokens: 8}
	if err := c.Set(ctx, "k", want, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || got != want {
		t.Fatalf("got %v %v %v", got, ok, err)
	}
	mr.FastForward(2 * time.Minute)
	if _, ok, _ := c.Get(ctx, "k"); ok {
		t.Fatal("expected TTL expiry")
	}
}

func TestRedisCacheCorruptEntryIsMiss(t *testing.T) {
	_, client := testutil.NewMiniRedis(t)
	client.Set(context.Background(), "bad", "{not json", 0)
	if _, ok, err := (Redis{Client: client}).Get(context.Background(), "bad"); ok || err != nil {
		t.Fatalf("corrupt entry: ok=%v err=%v", ok, err)
	}
}

func TestRedisCacheErrorsSurface(t *testing.T) {
	c := Redis{Client: redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 100 * time.Millisecond})}
	if _, _, err := c.Get(context.Background(), "k"); err == nil {
		t.Fatal("expected an error when redis is down")
	}
}
