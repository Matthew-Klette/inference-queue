package app_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Matthew-Klette/inference-queue/internal/app"
	"github.com/Matthew-Klette/inference-queue/internal/cache"
	"github.com/Matthew-Klette/inference-queue/internal/config"
	"github.com/Matthew-Klette/inference-queue/internal/model"
	"github.com/Matthew-Klette/inference-queue/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func dbURL(t *testing.T) string {
	t.Helper()
	return testutil.NewDBURL(t)
}

func baseCfg(t *testing.T) config.Config {
	t.Setenv("DATABASE_URL", dbURL(t))
	t.Setenv("BOOTSTRAP_ADMIN_KEY", "iq_admin_for_app_test")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestNewWithoutRedisUsesInMemoryFallbacks(t *testing.T) {
	a, err := app.New(context.Background(), baseCfg(t), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	if a.Redis != nil {
		t.Fatal("redis client created without REDIS_URL")
	}
	if _, ok := a.Cache.(*cache.Memory); !ok {
		t.Fatalf("cache = %T, want in-memory", a.Cache)
	}
	if _, ok := a.Backend.(*model.Fake); !ok {
		t.Fatalf("backend = %T, want fake", a.Backend)
	}
	if err := a.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNewWithRedisAndBootstrapKey(t *testing.T) {
	mr, _ := testutil.NewMiniRedis(t)
	cfg := baseCfg(t)
	cfg.RedisURL = "redis://" + mr.Addr()
	a, err := app.New(context.Background(), cfg, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	if _, ok := a.Cache.(cache.Redis); !ok {
		t.Fatalf("cache = %T, want redis", a.Cache)
	}

	ts := httptest.NewServer(a.NewServer(context.Background()).Handler())
	defer ts.Close()
	req, _ := http.NewRequest("POST", ts.URL+"/v1/keys", strings.NewReader(`{"name":"x"}`))
	req.Header.Set("Authorization", "Bearer iq_admin_for_app_test")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out struct{ Key string }
	_ = json.NewDecoder(res.Body).Decode(&out)
	if res.StatusCode != 201 || out.Key == "" {
		t.Fatalf("bootstrap admin key did not work: %d", res.StatusCode)
	}
}

func TestNewFailsFastOnBadDatabase(t *testing.T) {
	cfg := baseCfg(t)
	cfg.DatabaseURL = "postgres://nobody@127.0.0.1:1/none?connect_timeout=1"
	if _, err := app.New(context.Background(), cfg, quiet); err == nil {
		t.Fatal("expected an error")
	}
}

func TestOpenAIBackendSelection(t *testing.T) {
	cfg := baseCfg(t)
	cfg.ModelBackend, cfg.ModelBaseURL = "openai", "http://127.0.0.1:1"
	a, err := app.New(context.Background(), cfg, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	if _, ok := a.Backend.(*model.Fake); ok {
		t.Fatal("expected the openai backend")
	}
}
