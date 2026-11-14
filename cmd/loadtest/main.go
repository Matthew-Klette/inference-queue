// Command loadtest drives the full stack (HTTP API, Postgres queue, worker
// pool, result cache) in one process against the fake model and reports
// throughput and end-to-end latency.
//
// Without -database-url it starts an embedded Postgres 16 with default
// durability settings (fsync on), so the numbers include real commit cost.
//
// Example:
//
//	go run ./cmd/loadtest -workers 4,16,64 -items 1000
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Matthew-Klette/inference-queue/internal/auth"
	"github.com/Matthew-Klette/inference-queue/internal/cache"
	"github.com/Matthew-Klette/inference-queue/internal/model"
	"github.com/Matthew-Klette/inference-queue/internal/obs"
	"github.com/Matthew-Klette/inference-queue/internal/queue"
	"github.com/Matthew-Klette/inference-queue/internal/server"
	"github.com/Matthew-Klette/inference-queue/internal/store"
	"github.com/Matthew-Klette/inference-queue/internal/worker"
)

type options struct {
	dbURL     string
	workers   []int
	items     int
	batchSize int
	latency   time.Duration
	dupRatio  float64
	rate      float64
	duration  time.Duration
}

type result struct {
	Workers    int     `json:"workers"`
	Cache      bool    `json:"cache"`
	Mode       string  `json:"mode"`
	Items      int     `json:"items"`
	ModelCalls int64   `json:"model_calls"`
	Seconds    float64 `json:"seconds"`
	Throughput float64 `json:"items_per_sec"`
	P50ms      float64 `json:"p50_ms"`
	P95ms      float64 `json:"p95_ms"`
	P99ms      float64 `json:"p99_ms"`
}

func main() {
	var o options
	var workers string
	flag.StringVar(&o.dbURL, "database-url", "", "Postgres URL; empty starts an embedded Postgres")
	flag.StringVar(&workers, "workers", "4,16,64", "comma-separated worker concurrency levels")
	flag.IntVar(&o.items, "items", 1000, "items per scenario")
	flag.IntVar(&o.batchSize, "batch-size", 100, "items per submitted batch")
	flag.DurationVar(&o.latency, "model-latency", 50*time.Millisecond, "mean fake model latency")
	flag.Float64Var(&o.dupRatio, "dup-ratio", 0.8, "share of prompts that repeat an earlier one (affects cache runs only in effect)")
	flag.Float64Var(&o.rate, "rate", 0, "offered load in items/sec; 0 submits everything at once (burst)")
	flag.DurationVar(&o.duration, "duration", 10*time.Second, "length of a paced run")
	asJSON := flag.Bool("json", false, "print results as JSON")
	flag.Parse()
	for _, w := range strings.Split(workers, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(w))
		if err != nil || n < 1 {
			fatal(fmt.Errorf("bad -workers value %q", w))
		}
		o.workers = append(o.workers, n)
	}

	ctx := context.Background()
	if o.dbURL == "" {
		url, stop, err := startEmbedded()
		if err != nil {
			fatal(err)
		}
		defer stop()
		o.dbURL = url
	}
	pool, err := store.Open(ctx, o.dbURL, 40)
	if err != nil {
		fatal(err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		fatal(err)
	}

	var results []result
	for _, w := range o.workers {
		for _, useCache := range []bool{false, true} {
			r, err := runScenario(ctx, pool, o, w, useCache)
			if err != nil {
				fatal(err)
			}
			results = append(results, r)
			if !*asJSON {
				fmt.Fprintf(os.Stderr, "done: workers=%d cache=%v %.0f items/s\n", w, useCache, r.Throughput)
			}
		}
	}
	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(results)
		return
	}
	fmt.Printf("| workers | cache | mode | items | model calls | seconds | items/s | p50 ms | p95 ms | p99 ms |\n")
	fmt.Printf("|---:|:---:|:---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range results {
		fmt.Printf("| %d | %v | %s | %d | %d | %.2f | %.0f | %.0f | %.0f | %.0f |\n", r.Workers, onOff(r.Cache), r.Mode, r.Items,
			r.ModelCalls, r.Seconds, r.Throughput, r.P50ms, r.P95ms, r.P99ms)
	}
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func startEmbedded() (string, func(), error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	dir, err := os.MkdirTemp("", "iq-loadtest-*")
	if err != nil {
		return "", nil, err
	}
	cfg := embeddedpostgres.DefaultConfig().Version(embeddedpostgres.V16).Port(uint32(port)).
		DataPath(filepath.Join(dir, "data")).RuntimePath(filepath.Join(dir, "run")).
		Logger(io.Discard).StartTimeout(90 * time.Second)
	pg := embeddedpostgres.NewDatabase(cfg)
	if err := pg.Start(); err != nil {
		return "", nil, err
	}
	return cfg.GetConnectionURL() + "?sslmode=disable", func() { _ = pg.Stop(); _ = os.RemoveAll(dir) }, nil
}

func runScenario(ctx context.Context, pool *pgxpool.Pool, o options, workers int, useCache bool) (result, error) {
	if _, err := pool.Exec(ctx, `TRUNCATE outbox, items, batches, api_keys RESTART IDENTITY CASCADE`); err != nil {
		return result{}, err
	}
	plain, hash, _ := auth.Generate()
	if _, err := (store.Keys{Pool: pool}).Create(ctx, store.APIKey{Name: "load", KeyHash: hash,
		Scopes: []string{auth.ScopeSubmit, auth.ScopeRead}, RateRPS: 1e6, RateBurst: 1e6}); err != nil {
		return result{}, err
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := obs.NewMetrics()
	q := &queue.Queue{Pool: pool}
	api := httptest.NewServer((&server.Server{Keys: store.Keys{Pool: pool}, Batches: store.Batches{Pool: pool}, Queue: q,
		Metrics: metrics, Log: log, Opts: server.Options{}}).Handler())
	defer api.Close()

	fake := model.NewFake(model.FakeConfig{Latency: o.latency, Jitter: o.latency / 5, Seed: 42})
	var c cache.Cache
	if useCache {
		c = cache.NewMemory(1_000_000)
	}
	pl := &worker.Pool{Queue: q, Backend: fake, Cache: c, Metrics: metrics, Log: log,
		Cfg: worker.Config{Concurrency: workers, Poll: 20 * time.Millisecond, Lease: 30 * time.Second}}
	wctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = pl.Run(wctx) }()
	defer func() { stop(); <-done }()

	distinct := int(float64(o.items) * (1 - o.dupRatio))
	if distinct < 1 {
		distinct = 1
	}
	prompt := func(i int) string {
		if useCache {
			return fmt.Sprintf("load prompt %d", i%distinct)
		}
		return fmt.Sprintf("load prompt %d", i) // all distinct
	}

	client := &http.Client{Timeout: 30 * time.Second}
	submit := func(from, n int) error {
		prompts := make([]string, n)
		for i := range prompts {
			prompts[i] = prompt(from + i)
		}
		body, _ := json.Marshal(map[string]any{"model": "fake-1", "prompts": prompts, "params": map[string]any{"temperature": 0}})
		req, _ := http.NewRequest("POST", api.URL+"/v1/batches", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+plain)
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusAccepted {
			b, _ := io.ReadAll(res.Body)
			return fmt.Errorf("submit: %d %s", res.StatusCode, b)
		}
		return nil
	}

	mode := "burst"
	total := o.items
	if o.rate > 0 {
		mode = fmt.Sprintf("paced %.0f/s", o.rate)
		const step = 5 // items per submission when paced
		total = int(o.rate * o.duration.Seconds())
		total -= total % step
		interval := time.Duration(float64(time.Second) * step / o.rate)
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for sent := 0; sent < total; sent += step {
			<-tick.C
			if err := submit(sent, step); err != nil {
				return result{}, err
			}
		}
	} else {
		var wg sync.WaitGroup
		errs := make(chan error, o.items)
		sem := make(chan struct{}, 8)
		for from := 0; from < o.items; from += o.batchSize {
			n := min(o.batchSize, o.items-from)
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if err := submit(from, n); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			return result{}, err
		}
	}

	deadline := time.Now().Add(5 * time.Minute)
	for {
		var pending int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM items WHERE status <> 'succeeded'`).Scan(&pending); err != nil {
			return result{}, err
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			return result{}, fmt.Errorf("timed out with %d items pending", pending)
		}
		time.Sleep(50 * time.Millisecond)
	}

	r := result{Workers: workers, Cache: useCache, Mode: mode, Items: total, ModelCalls: fake.Calls()}
	var p50, p95, p99 float64
	err := pool.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM max(completed_at) - min(created_at)),
		       percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM completed_at - created_at)) * 1000,
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM completed_at - created_at)) * 1000,
		       percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM completed_at - created_at)) * 1000
		FROM items`).Scan(&r.Seconds, &p50, &p95, &p99)
	if err != nil {
		return result{}, err
	}
	r.P50ms, r.P95ms, r.P99ms = p50, p95, p99
	r.Throughput = float64(total) / r.Seconds
	return r, nil
}
