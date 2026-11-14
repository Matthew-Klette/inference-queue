package model

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFakeIsDeterministicPerPrompt(t *testing.T) {
	f := NewFake(FakeConfig{})
	a, _ := f.Complete(context.Background(), Request{Model: "m", Prompt: "one"})
	b, _ := f.Complete(context.Background(), Request{Model: "m", Prompt: "one"})
	c, _ := f.Complete(context.Background(), Request{Model: "m", Prompt: "two"})
	if a.Text != b.Text || a.Text == c.Text {
		t.Fatalf("a=%q b=%q c=%q", a.Text, b.Text, c.Text)
	}
	if f.Calls() != 3 {
		t.Fatalf("calls = %d", f.Calls())
	}
}

func TestFakeEchoAndPermanent(t *testing.T) {
	f := NewFake(FakeConfig{})
	r, _ := f.Complete(context.Background(), Request{Prompt: "echo:  4 "})
	if r.Text != "4" {
		t.Fatalf("echo = %q", r.Text)
	}
	_, err := f.Complete(context.Background(), Request{Prompt: "x [[permanent]]"})
	if !IsPermanent(err) {
		t.Fatalf("err = %v", err)
	}
}

func TestFakeErrorRateIsHonoured(t *testing.T) {
	f := NewFake(FakeConfig{ErrorRate: 0.3, Seed: 7})
	fails := 0
	const n = 2000
	for i := 0; i < n; i++ {
		if _, err := f.Complete(context.Background(), Request{Prompt: "p"}); errors.Is(err, ErrFakeTransient) {
			fails++
		}
	}
	if rate := float64(fails) / n; rate < 0.25 || rate > 0.35 {
		t.Fatalf("observed error rate %.3f, want about 0.30", rate)
	}
}

func TestFakeLatencyAndCancellation(t *testing.T) {
	f := NewFake(FakeConfig{Latency: 40 * time.Millisecond})
	start := time.Now()
	_, _ = f.Complete(context.Background(), Request{Prompt: "p"})
	if time.Since(start) < 35*time.Millisecond {
		t.Fatal("latency not applied")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	slow := NewFake(FakeConfig{Latency: time.Hour})
	if _, err := slow.Complete(ctx, Request{Prompt: "p"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}
