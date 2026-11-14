package obs

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestQueueDepthCollector(t *testing.T) {
	m := NewMetrics()
	m.RegisterQueueDepth(func(context.Context) (int, int, int, int, error) { return 5, 2, 3, 1, nil })
	got, err := testutil.GatherAndCount(m.Registry, "iq_queue_items")
	if err != nil || got != 4 {
		t.Fatalf("series = %d, err = %v", got, err)
	}
	want := `
# HELP iq_queue_items Items in the queue by state.
# TYPE iq_queue_items gauge
iq_queue_items{state="dead"} 1
iq_queue_items{state="delayed"} 2
iq_queue_items{state="leased"} 3
iq_queue_items{state="ready"} 5
`
	if err := testutil.GatherAndCompare(m.Registry, strings.NewReader(want), "iq_queue_items"); err != nil {
		t.Fatal(err)
	}
}

func TestQueueDepthCollectorOmitsSeriesOnError(t *testing.T) {
	m := NewMetrics()
	m.RegisterQueueDepth(func(context.Context) (int, int, int, int, error) { return 0, 0, 0, 0, errors.New("db down") })
	if n, _ := testutil.GatherAndCount(m.Registry, "iq_queue_items"); n != 0 {
		t.Fatalf("expected no series when the database is unreachable, got %d", n)
	}
}

func TestTracingIsNoopWithoutEndpoint(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), "test", "")
	if err != nil {
		t.Fatal(err)
	}
	_, span := Tracer().Start(context.Background(), "x")
	if span.SpanContext().IsValid() {
		t.Fatal("default tracer must be a no-op")
	}
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoggerContext(t *testing.T) {
	l := NewLogger("debug").With("k", "v")
	if L(WithLogger(context.Background(), l)) != l {
		t.Fatal("logger not carried in context")
	}
	if L(context.Background()) != slog.Default() {
		t.Fatal("expected default logger fallback")
	}
}
