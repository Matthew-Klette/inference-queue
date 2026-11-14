// Package app assembles the service from configuration. The cmd/ binaries are
// thin wrappers around it.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Matthew-Klette/inference-queue/internal/auth"
	"github.com/Matthew-Klette/inference-queue/internal/cache"
	"github.com/Matthew-Klette/inference-queue/internal/config"
	"github.com/Matthew-Klette/inference-queue/internal/events"
	"github.com/Matthew-Klette/inference-queue/internal/model"
	"github.com/Matthew-Klette/inference-queue/internal/obs"
	"github.com/Matthew-Klette/inference-queue/internal/queue"
	"github.com/Matthew-Klette/inference-queue/internal/ratelimit"
	"github.com/Matthew-Klette/inference-queue/internal/server"
	"github.com/Matthew-Klette/inference-queue/internal/store"
	"github.com/Matthew-Klette/inference-queue/internal/worker"
)

// App holds the long-lived dependencies.
type App struct {
	Cfg     config.Config
	Log     *slog.Logger
	Pool    *pgxpool.Pool
	Redis   redis.UniversalClient // nil when REDIS_URL is empty
	Cache   cache.Cache           // nil when caching is disabled
	Limiter ratelimit.Limiter
	Metrics *obs.Metrics
	Queue   *queue.Queue
	Backend model.Backend

	shutdownTracing func(context.Context) error
}

// New connects to dependencies, runs migrations and builds the model backend.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	a := &App{Cfg: cfg, Log: log, Metrics: obs.NewMetrics()}

	shutdown, err := obs.SetupTracing(ctx, "inference-queue", cfg.OTLPEndpoint)
	if err != nil {
		return nil, fmt.Errorf("tracing: %w", err)
	}
	a.shutdownTracing = shutdown

	a.Pool, err = store.Open(ctx, cfg.DatabaseURL, int32(cfg.DBMaxConns))
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx, a.Pool); err != nil {
		a.Pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	a.Queue = &queue.Queue{Pool: a.Pool, DefaultWebhook: cfg.WebhookURL}
	a.Metrics.RegisterQueueDepth(func(ctx context.Context) (int, int, int, int, error) {
		d, err := a.Queue.Depth(ctx)
		return d.Ready, d.Delayed, d.Leased, d.Dead, err
	})

	mem := ratelimit.NewMemory()
	a.Limiter = mem
	if cfg.RedisURL != "" {
		opt, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("REDIS_URL: %w", err)
		}
		a.Redis = redis.NewClient(opt)
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := a.Redis.Ping(pingCtx).Err(); err != nil {
			log.Warn("redis not reachable at startup; continuing, errors fail open", "err", err)
		}
		cancel()
		a.Limiter = ratelimit.FailOpen{
			Primary: ratelimit.Redis{Client: a.Redis, Prefix: "iq:rl:"}, Fallback: mem,
			OnError: func(error) { a.Metrics.DependencyError.WithLabelValues("redis").Inc() },
		}
		if !cfg.CacheDisabled {
			a.Cache = cache.Redis{Client: a.Redis}
		}
	} else {
		log.Info("REDIS_URL not set: using in-memory cache and rate limiter (not shared between processes)")
		if !cfg.CacheDisabled {
			a.Cache = cache.NewMemory(100_000)
		}
	}

	a.Backend, err = a.buildBackend()
	if err != nil {
		return nil, err
	}

	if cfg.BootstrapKey != "" {
		err := store.Keys{Pool: a.Pool}.EnsureBootstrap(ctx, store.APIKey{
			Name: "bootstrap-admin", KeyHash: auth.Hash(cfg.BootstrapKey),
			Scopes: []string{auth.ScopeAdmin}, RateRPS: 1000, RateBurst: 2000,
		})
		if err != nil {
			return nil, fmt.Errorf("bootstrap key: %w", err)
		}
	}
	return a, nil
}

func (a *App) buildBackend() (model.Backend, error) {
	cfg := a.Cfg
	switch cfg.ModelBackend {
	case "fake":
		return model.NewFake(model.FakeConfig{
			Latency: cfg.FakeLatency, Jitter: cfg.FakeLatency / 4, ErrorRate: cfg.FakeErrorRate, Seed: 1,
		}), nil
	case "openai":
		client := model.NewOpenAI(model.OpenAIConfig{
			BaseURL: cfg.ModelBaseURL, APIKey: cfg.ModelAPIKey, Timeout: cfg.ModelTimeout, MaxRetries: cfg.ModelRetries,
		})
		br := model.NewBreaker(cfg.BreakerFailures, cfg.BreakerCooldown)
		br.OnChange = func(state string) {
			v := 0.0
			if state == "open" {
				v = 1
			}
			a.Metrics.BreakerState.WithLabelValues("openai").Set(v)
			a.Log.Warn("model circuit breaker changed state", "state", state)
		}
		return model.WithBreaker(client, br), nil
	}
	return nil, fmt.Errorf("unknown model backend %q", cfg.ModelBackend)
}

// Close releases connections.
func (a *App) Close(ctx context.Context) {
	if a.Redis != nil {
		_ = a.Redis.Close()
	}
	a.Pool.Close()
	if a.shutdownTracing != nil {
		_ = a.shutdownTracing(ctx)
	}
}

// Ready checks that Postgres answers. Redis is optional and not part of readiness.
func (a *App) Ready(ctx context.Context) error {
	if err := a.Pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}

// NewWorker builds a worker pool from configuration.
func (a *App) NewWorker() *worker.Pool {
	owner, _ := os.Hostname()
	return &worker.Pool{
		Queue: a.Queue, Backend: a.Backend, Cache: a.Cache, Metrics: a.Metrics, Log: a.Log,
		Cfg: worker.Config{
			Owner: fmt.Sprintf("%s-%d", owner, os.Getpid()), Concurrency: a.Cfg.Workers, Lease: a.Cfg.LeaseDuration,
			Poll: a.Cfg.PollInterval, BackoffBase: a.Cfg.BackoffBase, BackoffMax: a.Cfg.BackoffMax,
			CacheTTL: a.Cfg.CacheTTL, DrainTimeout: a.Cfg.ShutdownTimeout - 5*time.Second,
		},
	}
}

// NewPublisher builds the outbox publisher.
func (a *App) NewPublisher() *events.Publisher {
	return &events.Publisher{
		Pool: a.Pool, Secret: a.Cfg.WebhookSecret, Log: a.Log, Metrics: a.Metrics,
		Client: events.SafeClient(10*time.Second, a.Cfg.WebhookPrivate),
	}
}

// NewServer builds the REST API.
func (a *App) NewServer(shutdown context.Context) *server.Server {
	return &server.Server{
		Keys: store.Keys{Pool: a.Pool}, Batches: store.Batches{Pool: a.Pool}, Queue: a.Queue,
		Limiter: a.Limiter, Metrics: a.Metrics, Log: a.Log, Ready: a.Ready,
		Opts: server.Options{MaxAttempts: a.Cfg.MaxAttempts, ShutdownContext: shutdown},
	}
}

// ServeAPI runs the HTTP API until ctx is cancelled, then drains connections.
func (a *App) ServeAPI(ctx context.Context) error {
	sseCtx, stopSSE := context.WithCancel(context.Background())
	defer stopSSE()
	srv := &http.Server{
		Addr: a.Cfg.HTTPAddr, Handler: a.NewServer(sseCtx).Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 60 * time.Second, IdleTimeout: 90 * time.Second,
	}
	srv.RegisterOnShutdown(stopSSE) // tell open SSE streams to finish so Shutdown does not wait on them
	return serve(ctx, a.Log, srv, a.Cfg.ShutdownTimeout)
}

// ServeOps runs a small listener with /healthz, /readyz and /metrics, for
// worker-only processes that do not run the public API.
func (a *App) ServeOps(ctx context.Context) error {
	mux := http.NewServeMux()
	s := &server.Server{Metrics: a.Metrics, Log: a.Log, Ready: a.Ready}
	mux.Handle("/", s.Handler())
	srv := &http.Server{Addr: a.Cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return serve(ctx, a.Log, srv, 5*time.Second)
}

func serve(ctx context.Context, log *slog.Logger, srv *http.Server, grace time.Duration) error {
	errc := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", srv.Addr)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("http shutting down", "addr", srv.Addr)
	sctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}
