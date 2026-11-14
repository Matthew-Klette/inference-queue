package model

import (
	"context"
	"sync"
	"time"
)

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

// Breaker is a consecutive-failure circuit breaker. After Threshold
// consecutive failures it opens for Cooldown, rejecting calls with
// ErrCircuitOpen. After the cooldown one trial call is let through
// (half-open); success closes the circuit, failure re-opens it.
type Breaker struct {
	Threshold int
	Cooldown  time.Duration
	Now       func() time.Time // injectable clock; defaults to time.Now
	OnChange  func(state string)

	mu       sync.Mutex
	state    breakerState
	failures int
	openedAt time.Time
	trial    bool // a half-open trial call is in flight
}

// NewBreaker returns a breaker with the given threshold and cooldown.
func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{Threshold: threshold, Cooldown: cooldown}
}

func (b *Breaker) now() time.Time {
	if b.Now != nil {
		return b.Now()
	}
	return time.Now()
}

// Allow reports whether a call may proceed. Every Allow that returns true must
// be followed by exactly one Done.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateClosed:
		return true
	case stateOpen:
		if b.now().Sub(b.openedAt) < b.Cooldown {
			return false
		}
		b.setState(stateHalfOpen)
		b.trial = true
		return true
	default: // half-open: only one trial at a time
		if b.trial {
			return false
		}
		b.trial = true
		return true
	}
}

// Done records the outcome of an allowed call.
func (b *Breaker) Done(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if success {
		b.failures = 0
		b.trial = false
		if b.state != stateClosed {
			b.setState(stateClosed)
		}
		return
	}
	b.failures++
	b.trial = false
	if b.state == stateHalfOpen || b.failures >= b.Threshold {
		b.openedAt = b.now()
		b.setState(stateOpen)
	}
}

// State returns "closed", "open" or "half-open".
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return stateName(b.state)
}

func (b *Breaker) setState(s breakerState) {
	b.state = s
	if b.OnChange != nil {
		b.OnChange(stateName(s))
	}
}

func stateName(s breakerState) string {
	switch s {
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	}
	return "closed"
}

// WithBreaker wraps a backend with a circuit breaker. Permanent errors and
// context cancellation are not counted as backend failures: they say nothing
// about backend health.
func WithBreaker(next Backend, b *Breaker) Backend { return &breakerBackend{next: next, b: b} }

type breakerBackend struct {
	next Backend
	b    *Breaker
}

func (c *breakerBackend) Complete(ctx context.Context, req Request) (Response, error) {
	if !c.b.Allow() {
		return Response{}, ErrCircuitOpen
	}
	resp, err := c.next.Complete(ctx, req)
	switch {
	case err == nil, IsPermanent(err):
		c.b.Done(true)
	case ctx.Err() != nil:
		// Caller gave up; do not blame the backend. Free a half-open trial slot.
		c.b.Done(true)
	default:
		c.b.Done(false)
	}
	return resp, err
}
