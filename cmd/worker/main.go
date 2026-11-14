// Command worker runs the worker pool and the outbox publisher, plus a small
// listener for /healthz, /readyz and /metrics.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Matthew-Klette/inference-queue/internal/app"
	"github.com/Matthew-Klette/inference-queue/internal/config"
	"github.com/Matthew-Klette/inference-queue/internal/obs"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := obs.NewLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := app.New(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer a.Close(context.Background())

	pool := a.NewWorker()
	pub := a.NewPublisher()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pub.Run(ctx) }()
	go func() { defer wg.Done(); _ = a.ServeOps(ctx) }()
	err = pool.Run(ctx) // returns after draining and releasing leases
	stop()
	wg.Wait()
	return err
}
