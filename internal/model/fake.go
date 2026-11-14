package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

// FakeConfig tunes the deterministic fake model.
type FakeConfig struct {
	Latency   time.Duration // mean latency per call
	Jitter    time.Duration // uniform +/- jitter around Latency
	ErrorRate float64       // probability of a transient error per call, 0..1
	Seed      uint64        // seeds the latency and error source
}

// Fake is a deterministic model for tests and load tests. The output depends
// only on the prompt, so duplicate prompts produce identical text. Latency and
// transient errors are configurable and come from a seeded source.
//
// Prompt conventions:
//
//	"echo: foo"       returns "foo" (useful for eval datasets with expected answers)
//	"[[permanent]]"   always fails with a permanent error
type Fake struct {
	cfg   FakeConfig
	mu    sync.Mutex
	rnd   *rand.Rand
	calls int64
}

// NewFake returns a fake backend.
func NewFake(cfg FakeConfig) *Fake {
	return &Fake{cfg: cfg, rnd: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))}
}

// Calls returns how many times Complete has been invoked.
func (f *Fake) Calls() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// ErrFakeTransient is the error injected by ErrorRate.
var ErrFakeTransient = errors.New("fake model: injected transient error")

func (f *Fake) Complete(ctx context.Context, req Request) (Response, error) {
	f.mu.Lock()
	f.calls++
	delay := f.cfg.Latency
	if f.cfg.Jitter > 0 {
		delay += time.Duration((f.rnd.Float64()*2 - 1) * float64(f.cfg.Jitter))
	}
	fail := f.cfg.ErrorRate > 0 && f.rnd.Float64() < f.cfg.ErrorRate
	f.mu.Unlock()

	if delay > 0 {
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return Response{}, ctx.Err()
		case <-t.C:
		}
	}
	if strings.Contains(req.Prompt, "[[permanent]]") {
		return Response{}, Permanent(errors.New("fake model: prompt rejected"))
	}
	if fail {
		return Response{}, ErrFakeTransient
	}
	return Response{
		Text:             fakeText(req),
		PromptTokens:     len(strings.Fields(req.Prompt)),
		CompletionTokens: 8,
	}, nil
}

func fakeText(req Request) string {
	if rest, ok := strings.CutPrefix(req.Prompt, "echo:"); ok {
		return strings.TrimSpace(rest)
	}
	sum := sha256.Sum256([]byte(req.Model + "\x00" + req.Prompt))
	return fmt.Sprintf("fake-%s", hex.EncodeToString(sum[:6]))
}
