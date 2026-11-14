package model

import (
	"context"
	"errors"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(threshold int, cooldown time.Duration) (*Breaker, *clock) {
	c := &clock{t: time.Unix(0, 0)}
	b := NewBreaker(threshold, cooldown)
	b.Now = c.now
	return b, c
}

func fail(b *Breaker) { b.Allow(); b.Done(false) }

func TestBreakerOpensAfterThreshold(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	fail(b)
	fail(b)
	if b.State() != "closed" {
		t.Fatal("must stay closed below threshold")
	}
	fail(b)
	if b.State() != "open" || b.Allow() {
		t.Fatal("must open at threshold and reject calls")
	}
}

func TestBreakerSuccessResetsFailureCount(t *testing.T) {
	b, _ := newTestBreaker(2, time.Second)
	fail(b)
	b.Allow()
	b.Done(true)
	fail(b)
	if b.State() != "closed" {
		t.Fatal("non-consecutive failures must not open the breaker")
	}
}

func TestBreakerHalfOpenTrial(t *testing.T) {
	b, c := newTestBreaker(1, time.Second)
	fail(b)
	c.advance(time.Second)
	if !b.Allow() {
		t.Fatal("first call after cooldown is the trial")
	}
	if b.State() != "half-open" {
		t.Fatalf("state = %s", b.State())
	}
	if b.Allow() {
		t.Fatal("only one trial call may be in flight")
	}
	b.Done(false)
	if b.State() != "open" {
		t.Fatal("failed trial must re-open")
	}
	c.advance(time.Second)
	b.Allow()
	b.Done(true)
	if b.State() != "closed" || !b.Allow() {
		t.Fatal("successful trial must close the breaker")
	}
}

type stub struct {
	err   error
	calls int
}

func (s *stub) Complete(context.Context, Request) (Response, error) {
	s.calls++
	return Response{Text: "ok"}, s.err
}

func TestWithBreakerShortCircuits(t *testing.T) {
	b, _ := newTestBreaker(2, time.Minute)
	s := &stub{err: errors.New("boom")}
	be := WithBreaker(s, b)
	for i := 0; i < 2; i++ {
		_, _ = be.Complete(context.Background(), Request{})
	}
	_, err := be.Complete(context.Background(), Request{})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("got %v", err)
	}
	if s.calls != 2 {
		t.Fatalf("backend called %d times, want 2", s.calls)
	}
}

func TestWithBreakerIgnoresPermanentAndCancelled(t *testing.T) {
	b, _ := newTestBreaker(1, time.Minute)
	s := &stub{err: Permanent(errors.New("bad request"))}
	be := WithBreaker(s, b)
	_, _ = be.Complete(context.Background(), Request{})
	if b.State() != "closed" {
		t.Fatal("a permanent (4xx) error must not trip the breaker")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.err = context.Canceled
	_, _ = be.Complete(ctx, Request{})
	if b.State() != "closed" {
		t.Fatal("caller cancellation must not trip the breaker")
	}
}
