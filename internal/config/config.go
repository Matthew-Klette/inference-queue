// Package config reads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every tunable. Field comments give the environment variable.
type Config struct {
	HTTPAddr        string        // HTTP_ADDR, default :8080
	MetricsAddr     string        // METRICS_ADDR, worker health and metrics listener, default :9090
	DatabaseURL     string        // DATABASE_URL, required
	DBMaxConns      int           // DB_MAX_CONNS, default 20
	RedisURL        string        // REDIS_URL, empty means in-memory cache and limiter
	BootstrapKey    string        // BOOTSTRAP_ADMIN_KEY, optional admin key seeded at startup
	LogLevel        string        // LOG_LEVEL, default info
	WebhookURL      string        // WEBHOOK_URL, default target for batch.completed events
	WebhookSecret   string        // WEBHOOK_SECRET, HMAC key for webhook signatures
	WebhookPrivate  bool          // WEBHOOK_ALLOW_PRIVATE, allow webhooks to private addresses
	ShutdownTimeout time.Duration // SHUTDOWN_TIMEOUT, default 30s

	Workers       int           // WORKER_CONCURRENCY, default 8
	LeaseDuration time.Duration // LEASE_DURATION, default 60s
	PollInterval  time.Duration // POLL_INTERVAL, default 200ms
	MaxAttempts   int           // MAX_ATTEMPTS, default 5
	BackoffBase   time.Duration // BACKOFF_BASE, default 2s
	BackoffMax    time.Duration // BACKOFF_MAX, default 5m
	CacheTTL      time.Duration // CACHE_TTL, default 24h
	CacheDisabled bool          // CACHE_DISABLED

	ModelBackend    string        // MODEL_BACKEND: fake (default) or openai
	ModelBaseURL    string        // MODEL_BASE_URL for the openai backend
	ModelAPIKey     string        // MODEL_API_KEY, optional
	ModelTimeout    time.Duration // MODEL_TIMEOUT, default 60s
	ModelRetries    int           // MODEL_RETRIES, default 3
	BreakerFailures int           // BREAKER_FAILURES, default 5
	BreakerCooldown time.Duration // BREAKER_COOLDOWN, default 15s
	FakeLatency     time.Duration // FAKE_LATENCY, default 50ms
	FakeErrorRate   float64       // FAKE_ERROR_RATE, default 0

	OTLPEndpoint string // OTEL_EXPORTER_OTLP_ENDPOINT, tracing is a no-op when empty
}

// Load reads the environment. It returns an error for malformed values.
func Load() (Config, error) {
	var errs []error
	dur := func(name string, def time.Duration) time.Duration {
		v := os.Getenv(name)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			return def
		}
		return d
	}
	num := func(name string, def int) int {
		v := os.Getenv(name)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			return def
		}
		return n
	}
	flt := func(name string, def float64) float64 {
		v := os.Getenv(name)
		if v == "" {
			return def
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			return def
		}
		return f
	}
	str := func(name, def string) string {
		if v := os.Getenv(name); v != "" {
			return v
		}
		return def
	}
	boolean := func(name string) bool {
		b, _ := strconv.ParseBool(os.Getenv(name))
		return b
	}

	c := Config{
		HTTPAddr:        str("HTTP_ADDR", ":8080"),
		MetricsAddr:     str("METRICS_ADDR", ":9090"),
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		DBMaxConns:      num("DB_MAX_CONNS", 20),
		RedisURL:        os.Getenv("REDIS_URL"),
		BootstrapKey:    os.Getenv("BOOTSTRAP_ADMIN_KEY"),
		LogLevel:        str("LOG_LEVEL", "info"),
		WebhookURL:      os.Getenv("WEBHOOK_URL"),
		WebhookSecret:   os.Getenv("WEBHOOK_SECRET"),
		WebhookPrivate:  boolean("WEBHOOK_ALLOW_PRIVATE"),
		ShutdownTimeout: dur("SHUTDOWN_TIMEOUT", 30*time.Second),
		Workers:         num("WORKER_CONCURRENCY", 8),
		LeaseDuration:   dur("LEASE_DURATION", 60*time.Second),
		PollInterval:    dur("POLL_INTERVAL", 200*time.Millisecond),
		MaxAttempts:     num("MAX_ATTEMPTS", 5),
		BackoffBase:     dur("BACKOFF_BASE", 2*time.Second),
		BackoffMax:      dur("BACKOFF_MAX", 5*time.Minute),
		CacheTTL:        dur("CACHE_TTL", 24*time.Hour),
		CacheDisabled:   boolean("CACHE_DISABLED"),
		ModelBackend:    str("MODEL_BACKEND", "fake"),
		ModelBaseURL:    os.Getenv("MODEL_BASE_URL"),
		ModelAPIKey:     os.Getenv("MODEL_API_KEY"),
		ModelTimeout:    dur("MODEL_TIMEOUT", 60*time.Second),
		ModelRetries:    num("MODEL_RETRIES", 3),
		BreakerFailures: num("BREAKER_FAILURES", 5),
		BreakerCooldown: dur("BREAKER_COOLDOWN", 15*time.Second),
		FakeLatency:     dur("FAKE_LATENCY", 50*time.Millisecond),
		FakeErrorRate:   flt("FAKE_ERROR_RATE", 0),
		OTLPEndpoint:    os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}
	if c.DatabaseURL == "" {
		errs = append(errs, fmt.Errorf("DATABASE_URL is required"))
	}
	if c.ModelBackend == "openai" && c.ModelBaseURL == "" {
		errs = append(errs, fmt.Errorf("MODEL_BASE_URL is required when MODEL_BACKEND=openai"))
	}
	if c.ModelBackend != "fake" && c.ModelBackend != "openai" {
		errs = append(errs, fmt.Errorf("MODEL_BACKEND must be fake or openai"))
	}
	if c.Workers < 1 {
		errs = append(errs, fmt.Errorf("WORKER_CONCURRENCY must be at least 1"))
	}
	return c, joinErrs(errs)
}

func joinErrs(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	msg := "invalid configuration:"
	for _, e := range errs {
		msg += "\n  " + e.Error()
	}
	return fmt.Errorf("%s", msg)
}
