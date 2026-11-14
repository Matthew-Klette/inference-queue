// Package testutil starts real Postgres and Redis servers for integration
// tests.
//
// Postgres: if TEST_DATABASE_URL is set it is used (CI service container,
// docker compose); otherwise an embedded Postgres binary is downloaded once
// and started for the test process. Redis: if TEST_REDIS_URL is set it is
// used; otherwise an in-process miniredis server (a Go implementation of the
// Redis protocol including Lua scripting) is started per test.
package testutil

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Matthew-Klette/inference-queue/internal/store"
)

var (
	pgOnce  sync.Once
	pgURL   string
	pgErr   error
	pgStop  func() error
	pgDir   string
	dbCount atomic.Int64
)

// Main is for TestMain: it runs the tests and then stops the embedded Postgres.
func Main(m *testing.M) {
	code := m.Run()
	if pgStop != nil {
		_ = pgStop()
	}
	if pgDir != "" {
		_ = os.RemoveAll(pgDir)
	}
	os.Exit(code)
}

func adminURL() (string, error) {
	pgOnce.Do(func() {
		if u := os.Getenv("TEST_DATABASE_URL"); u != "" {
			pgURL = u
			return
		}
		port, err := freePort()
		if err != nil {
			pgErr = err
			return
		}
		pgDir, err = os.MkdirTemp("", "iq-pg-*")
		if err != nil {
			pgErr = err
			return
		}
		cfg := embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.V16).
			Port(uint32(port)).
			DataPath(filepath.Join(pgDir, "data")).
			RuntimePath(filepath.Join(pgDir, "run")).
			Logger(io.Discard).
			StartTimeout(90 * time.Second).
			StartParameters(map[string]string{"fsync": "off", "synchronous_commit": "off", "full_page_writes": "off"})
		pg := embeddedpostgres.NewDatabase(cfg)
		if err := pg.Start(); err != nil {
			pgErr = fmt.Errorf("start embedded postgres: %w", err)
			return
		}
		pgStop = pg.Stop
		pgURL = cfg.GetConnectionURL() + "?sslmode=disable"
	})
	return pgURL, pgErr
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// NewDB creates an empty, migrated database for one test and drops it afterwards.
func NewDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool, _ := newDB(t)
	return pool
}

// NewDBURL is like NewDB but also returns a connection URL for the new
// database, for tests that build the whole application from configuration.
func NewDBURL(t testing.TB) string {
	t.Helper()
	_, u := newDB(t)
	return u
}

func newDB(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	base, err := adminURL()
	if err != nil {
		t.Fatalf("postgres unavailable: %v", err)
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close()

	name := fmt.Sprintf("iq_test_%d_%d", os.Getpid(), dbCount.Add(1))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	cfg.MaxConns = 30
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		a, err := pgxpool.New(ctx, base)
		if err != nil {
			return
		}
		defer a.Close()
		_, _ = a.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return pool, u.String()
}

// NewRedis returns a Redis client for one test. It is flushed and closed on cleanup.
func NewRedis(t testing.TB) *redis.Client {
	t.Helper()
	if u := os.Getenv("TEST_REDIS_URL"); u != "" {
		opt, err := redis.ParseURL(u)
		if err != nil {
			t.Fatalf("TEST_REDIS_URL: %v", err)
		}
		c := redis.NewClient(opt)
		c.FlushDB(context.Background())
		t.Cleanup(func() { c.FlushDB(context.Background()); _ = c.Close() })
		return c
	}
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// NewMiniRedis returns the miniredis server itself, for tests that need to
// control time or inject failures.
func NewMiniRedis(t testing.TB) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}
