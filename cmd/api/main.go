// Command api serves the REST API. With -with-worker it also runs the worker
// pool and outbox publisher in the same process.
package main

import (
	"context"
	"flag"
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
	withWorker := flag.Bool("with-worker", false, "also run the worker pool and outbox publisher in this process")
	flag.Parse()
	if err := run(*withWorker); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(withWorker bool) error {
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

	var wg sync.WaitGroup
	if withWorker {
		pool := a.NewWorker()
		pub := a.NewPublisher()
		wg.Add(2)
		go func() { defer wg.Done(); _ = pool.Run(ctx) }()
		go func() { defer wg.Done(); pub.Run(ctx) }()
	}
	err = a.ServeAPI(ctx)
	stop() // make sure the worker also stops if the API exited first
	wg.Wait()
	return err
}
