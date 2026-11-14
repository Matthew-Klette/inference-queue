// Package worker runs the pool that leases items, calls the model backend and
// records results.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/singleflight"

	"github.com/Matthew-Klette/inference-queue/internal/cache"
	"github.com/Matthew-Klette/inference-queue/internal/model"
	"github.com/Matthew-Klette/inference-queue/internal/obs"
	"github.com/Matthew-Klette/inference-queue/internal/queue"
)

// Config tunes a Pool.
type Config struct {
	Owner          string        // unique lease owner id; defaults to a random id
	Concurrency    int           // items processed at once
	Lease          time.Duration // lease duration; heartbeats extend it while work is in flight
	Poll           time.Duration // idle poll interval
	ReapInterval   time.Duration // how often to dead-letter items that expired on their last attempt
	BackoffBase    time.Duration
	BackoffMax     time.Duration
	CacheTTL       time.Duration
	CallTimeout    time.Duration // upper bound for one model call
	DrainTimeout   time.Duration // how long shutdown waits for in-flight items
	CircuitRequeue time.Duration // delay before an item blocked by an open breaker is retried
}

func (c *Config) defaults() {
	if c.Owner == "" {
		c.Owner = fmt.Sprintf("worker-%08x", rand.Uint32())
	}
	if c.Concurrency < 1 {
		c.Concurrency = 4
	}
	if c.Lease <= 0 {
		c.Lease = 60 * time.Second
	}
	if c.Poll <= 0 {
		c.Poll = 200 * time.Millisecond
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = 5 * time.Second
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 2 * time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 5 * time.Minute
	}
	if c.CacheTTL <= 0 {
		c.CacheTTL = 24 * time.Hour
	}
	if c.CallTimeout <= 0 {
		c.CallTimeout = 2 * time.Minute
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
	if c.CircuitRequeue <= 0 {
		c.CircuitRequeue = 5 * time.Second
	}
}

// Pool leases and processes items.
type Pool struct {
	Queue   *queue.Queue
	Backend model.Backend
	Cache   cache.Cache // optional
	Metrics *obs.Metrics
	Log     *slog.Logger
	Cfg     Config

	flight   singleflight.Group
	mu       sync.Mutex
	inflight map[int64]struct{}
}

// Owner returns the lease owner id.
func (p *Pool) Owner() string { return p.Cfg.Owner }

// Run processes items until ctx is cancelled. On cancellation it stops
// leasing, waits up to DrainTimeout for in-flight items to finish, aborts
// whatever is left and releases those leases so another worker can take them
// immediately instead of waiting for expiry.
func (p *Pool) Run(ctx context.Context) error {
	p.Cfg.defaults()
	if p.Log == nil {
		p.Log = slog.Default()
	}
	if p.Metrics == nil {
		p.Metrics = obs.NewMetrics()
	}
	p.inflight = map[int64]struct{}{}
	log := p.Log.With("worker", p.Cfg.Owner)

	// Work keeps running after ctx is cancelled so it can drain.
	runCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()

	var wg sync.WaitGroup
	sem := make(chan struct{}, p.Cfg.Concurrency)
	wake := make(chan struct{}, 1)

	bgCtx, stopBG := context.WithCancel(runCtx)
	var bg sync.WaitGroup
	bg.Add(2)
	go func() { defer bg.Done(); p.heartbeat(bgCtx) }()
	go func() { defer bg.Done(); p.reap(bgCtx) }()

	log.Info("worker started", "concurrency", p.Cfg.Concurrency, "lease", p.Cfg.Lease.String())
loop:
	for ctx.Err() == nil {
		free := cap(sem) - len(sem)
		if free > 0 {
			start := time.Now()
			items, err := p.Queue.Lease(ctx, p.Cfg.Owner, free, p.Cfg.Lease)
			p.Metrics.LeaseDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				if ctx.Err() != nil {
					break loop
				}
				log.Error("lease failed", "err", err)
			}
			for _, it := range items {
				sem <- struct{}{}
				p.track(it.ID, true)
				p.Metrics.QueueWait.Observe(it.QueueWait.Seconds())
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() {
						p.track(it.ID, false)
						<-sem
						select {
						case wake <- struct{}{}:
						default:
						}
					}()
					p.process(runCtx, it)
				}()
			}
			if len(items) == free && err == nil {
				continue // there may be more work; lease again straight away
			}
		}
		// Wait for work, a free slot, or shutdown. Jitter avoids workers polling in lockstep.
		wait := p.Cfg.Poll + time.Duration(rand.Int64N(int64(p.Cfg.Poll)/2+1))
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
		case <-wake:
		case <-t.C:
		}
		t.Stop()
	}

	log.Info("worker draining", "inflight", len(sem))
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(p.Cfg.DrainTimeout):
		log.Warn("drain timeout, aborting in-flight items", "inflight", len(sem))
		abort()
		<-done
	}
	stopBG()
	bg.Wait()

	relCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := p.Queue.Release(relCtx, p.Cfg.Owner)
	if err != nil {
		log.Error("release leases failed; they will be reclaimed after expiry", "err", err)
		return err
	}
	log.Info("worker stopped", "released", n)
	return nil
}

func (p *Pool) track(id int64, add bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if add {
		p.inflight[id] = struct{}{}
	} else {
		delete(p.inflight, id)
	}
}

// heartbeat extends the lease of everything in flight so slow model calls do
// not lose their lease. A crashed worker stops heartbeating and its leases
// lapse, which is what makes recovery work.
func (p *Pool) heartbeat(ctx context.Context) {
	t := time.NewTicker(p.Cfg.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.mu.Lock()
			ids := make([]int64, 0, len(p.inflight))
			for id := range p.inflight {
				ids = append(ids, id)
			}
			p.mu.Unlock()
			if err := p.Queue.Extend(ctx, p.Cfg.Owner, ids, p.Cfg.Lease); err != nil && ctx.Err() == nil {
				p.Log.Error("lease heartbeat failed", "err", err)
			}
		}
	}
}

func (p *Pool) reap(ctx context.Context) {
	t := time.NewTicker(p.Cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := p.Queue.ReapExpired(ctx)
			if err != nil && ctx.Err() == nil {
				p.Log.Error("reap failed", "err", err)
			} else if n > 0 {
				p.Log.Warn("dead-lettered items with expired final lease", "count", n)
				p.Metrics.ItemsProcessed.WithLabelValues("dead").Add(float64(n))
			}
		}
	}
}

func (p *Pool) process(ctx context.Context, l queue.Leased) {
	ctx, span := obs.Tracer().Start(ctx, "item.process")
	defer span.End()
	span.SetAttributes(attribute.Int64("item.id", l.ID), attribute.String("batch.id", l.BatchID.String()),
		attribute.Int("item.attempt", l.Attempts), attribute.String("model", l.Model))
	log := p.Log.With("item_id", l.ID, "batch_id", l.BatchID, "attempt", l.Attempts)

	start := time.Now()
	entry, cached, err := p.resolve(ctx, l)
	latency := time.Since(start)

	// Use a context that survives shutdown for the bookkeeping write so a
	// finished result is not lost just because the pool is stopping.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	switch {
	case err == nil:
		cerr := p.Queue.Complete(wctx, l, queue.Completion{
			Output: entry.Text, Cached: cached, PromptTokens: entry.PromptTokens,
			CompletionTokens: entry.CompletionTokens, Latency: latency,
		})
		p.record(log, "succeeded", cerr)
	case ctx.Err() != nil:
		// Shutdown or abort: not the item's fault.
		p.record(log, "requeued", p.Queue.Requeue(wctx, l, 0))
	case errors.Is(err, model.ErrCircuitOpen):
		delay := p.Cfg.CircuitRequeue + time.Duration(rand.Int64N(int64(p.Cfg.CircuitRequeue)/2+1))
		p.record(log, "requeued", p.Queue.Requeue(wctx, l, delay))
	case model.IsPermanent(err):
		log.Warn("item failed permanently", "err", err)
		p.record(log, "dead", p.Queue.Fail(wctx, l, err, false, 0))
		span.SetStatus(codes.Error, err.Error())
	default:
		backoff := queue.Backoff(l.Attempts, p.Cfg.BackoffBase, p.Cfg.BackoffMax, nil)
		outcome := "retried"
		if l.Attempts >= l.MaxAttempts {
			outcome = "dead"
		}
		log.Warn("item attempt failed", "err", err, "outcome", outcome, "backoff", backoff.String())
		p.record(log, outcome, p.Queue.Fail(wctx, l, err, true, backoff))
		span.SetStatus(codes.Error, err.Error())
	}
}

func (p *Pool) record(log *slog.Logger, outcome string, err error) {
	if errors.Is(err, queue.ErrLeaseLost) {
		p.Metrics.LeasesLost.Inc()
		p.Metrics.ItemsProcessed.WithLabelValues("lease_lost").Inc()
		log.Warn("lease lost before result could be recorded; another worker owns this item now")
		return
	}
	if err != nil {
		log.Error("recording outcome failed; item will be retried after lease expiry", "outcome", outcome, "err", err)
		return
	}
	p.Metrics.ItemsProcessed.WithLabelValues(outcome).Inc()
}

// resolve returns the model output for the item, using the cache when present.
// Concurrent items with the same key inside this process share one model call.
func (p *Pool) resolve(ctx context.Context, l queue.Leased) (cache.Entry, bool, error) {
	key := cache.Key(l.Model, l.Params, l.Prompt)
	if p.Cache != nil {
		e, ok, err := p.Cache.Get(ctx, key)
		switch {
		case err != nil:
			p.Metrics.DependencyError.WithLabelValues("cache").Inc()
			p.Log.Warn("cache get failed, treating as miss", "err", err)
		case ok:
			p.Metrics.CacheLookups.WithLabelValues("hit").Inc()
			return e, true, nil
		}
		p.Metrics.CacheLookups.WithLabelValues("miss").Inc()
	}

	v, err, _ := p.flight.Do(key, func() (any, error) {
		callCtx, cancel := context.WithTimeout(ctx, p.Cfg.CallTimeout)
		defer cancel()
		start := time.Now()
		resp, err := p.Backend.Complete(callCtx, model.Request{Model: l.Model, Prompt: l.Prompt, Params: l.Params})
		outcome := "ok"
		if err != nil {
			outcome = "error"
		}
		p.Metrics.ModelDuration.WithLabelValues(l.Model, outcome).Observe(time.Since(start).Seconds())
		if err != nil {
			return nil, err
		}
		e := cache.Entry{Text: resp.Text, PromptTokens: resp.PromptTokens, CompletionTokens: resp.CompletionTokens}
		if p.Cache != nil {
			if err := p.Cache.Set(context.WithoutCancel(ctx), key, e, p.Cfg.CacheTTL); err != nil {
				p.Metrics.DependencyError.WithLabelValues("cache").Inc()
				p.Log.Warn("cache set failed", "err", err)
			}
		}
		return e, nil
	})
	if err != nil {
		return cache.Entry{}, false, err
	}
	return v.(cache.Entry), false, nil
}
