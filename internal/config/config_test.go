package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("WORKER_CONCURRENCY", "12")
	t.Setenv("LEASE_DURATION", "90s")
	t.Setenv("FAKE_ERROR_RATE", "0.25")
	t.Setenv("CACHE_DISABLED", "true")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Workers != 12 || c.LeaseDuration != 90*time.Second || c.FakeErrorRate != 0.25 || !c.CacheDisabled {
		t.Fatalf("overrides not applied: %+v", c)
	}
	if c.HTTPAddr != ":8080" || c.MaxAttempts != 5 || c.ModelBackend != "fake" || c.RedisURL != "" {
		t.Fatalf("defaults wrong: %+v", c)
	}
}

func TestLoadReportsAllProblemsAtOnce(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("WORKER_CONCURRENCY", "many")
	t.Setenv("LEASE_DURATION", "soon")
	t.Setenv("MODEL_BACKEND", "openai")
	_, err := Load()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"DATABASE_URL", "WORKER_CONCURRENCY", "LEASE_DURATION", "MODEL_BASE_URL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}
