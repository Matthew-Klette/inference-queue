package model

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const okBody = `{"choices":[{"message":{"role":"assistant","content":"hello"}}],"usage":{"prompt_tokens":4,"completion_tokens":2}}`

type sleeps struct{ got []time.Duration }

func (s *sleeps) sleep(_ context.Context, d time.Duration) error {
	s.got = append(s.got, d)
	return nil
}

func newClient(url string, s *sleeps, retries int) *OpenAI {
	return NewOpenAI(OpenAIConfig{BaseURL: url, APIKey: "tok", MaxRetries: retries,
		BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second, Sleep: s.sleep})
}

func TestOpenAISuccessSendsExpectedRequest(t *testing.T) {
	var gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/v1/chat/completions" || r.Method != http.MethodPost {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(okBody))
	}))
	defer srv.Close()

	resp, err := newClient(srv.URL+"/", &sleeps{}, 0).Complete(context.Background(), Request{
		Model: "m1", Prompt: "hi", Params: json.RawMessage(`{"temperature":0,"model":"evil","messages":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "hello" || resp.PromptTokens != 4 || resp.CompletionTokens != 2 {
		t.Fatalf("resp = %+v", resp)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("auth header = %q", gotAuth)
	}
	if gotBody["model"] != "m1" || gotBody["temperature"] != float64(0) || gotBody["stream"] != false {
		t.Errorf("body = %v", gotBody)
	}
	msgs := gotBody["messages"].([]any)
	if len(msgs) != 1 || msgs[0].(map[string]any)["content"] != "hi" {
		t.Errorf("params must not override messages: %v", msgs)
	}
}

func TestOpenAIRetriesOn5xxWithBackoff(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) < 3 {
			http.Error(w, "overloaded", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(okBody))
	}))
	defer srv.Close()
	s := &sleeps{}
	resp, err := newClient(srv.URL, s, 3).Complete(context.Background(), Request{Model: "m", Prompt: "p"})
	if err != nil || resp.Text != "hello" {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	if n.Load() != 3 {
		t.Fatalf("calls = %d", n.Load())
	}
	if len(s.got) != 2 || s.got[0] != 100*time.Millisecond || s.got[1] != 200*time.Millisecond {
		t.Fatalf("backoff waits = %v", s.got)
	}
}

func TestOpenAIHonoursRetryAfterOn429(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(okBody))
	}))
	defer srv.Close()
	s := &sleeps{}
	if _, err := newClient(srv.URL, s, 2).Complete(context.Background(), Request{Model: "m", Prompt: "p"}); err != nil {
		t.Fatal(err)
	}
	if len(s.got) != 1 || s.got[0] != 7*time.Second {
		t.Fatalf("waits = %v, want [7s]", s.got)
	}
}

func TestOpenAICapsRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	s := &sleeps{}
	_, err := newClient(srv.URL, s, 1).Complete(context.Background(), Request{Model: "m", Prompt: "p"})
	if err == nil || IsPermanent(err) {
		t.Fatalf("expected transient error, got %v", err)
	}
	if len(s.got) != 1 || s.got[0] != 10*time.Second {
		t.Fatalf("waits = %v, want capped at 10s", s.got)
	}
}

func TestOpenAIGivesUpAfterMaxRetries(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	_, err := newClient(srv.URL, &sleeps{}, 2).Complete(context.Background(), Request{Model: "m", Prompt: "p"})
	if err == nil || n.Load() != 3 {
		t.Fatalf("err=%v calls=%d (want 3: first try plus 2 retries)", err, n.Load())
	}
}

func TestOpenAI4xxIsPermanentAndNotRetried(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		http.Error(w, `{"error":"bad model"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	_, err := newClient(srv.URL, &sleeps{}, 3).Complete(context.Background(), Request{Model: "m", Prompt: "p"})
	if !IsPermanent(err) || n.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, n.Load())
	}
}

func TestOpenAITimeoutIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()
	c := NewOpenAI(OpenAIConfig{BaseURL: srv.URL, Timeout: 50 * time.Millisecond, MaxRetries: 0})
	_, err := c.Complete(context.Background(), Request{Model: "m", Prompt: "p"})
	if err == nil || IsPermanent(err) {
		t.Fatalf("expected a retryable timeout, got %v", err)
	}
}

func TestOpenAIBadParamsArePermanent(t *testing.T) {
	c := NewOpenAI(OpenAIConfig{BaseURL: "http://127.0.0.1:1"})
	_, err := c.Complete(context.Background(), Request{Model: "m", Prompt: "p", Params: json.RawMessage(`[1,2]`)})
	if !IsPermanent(err) {
		t.Fatalf("got %v", err)
	}
}

func TestOpenAIEmptyChoicesIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"choices":[]}`)) }))
	defer srv.Close()
	if _, err := newClient(srv.URL, &sleeps{}, 0).Complete(context.Background(), Request{Model: "m", Prompt: "p"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{
		"":        0,
		"5":       5 * time.Second,
		"1.5":     1500 * time.Millisecond,
		"-3":      0,
		"garbage": 0,
		now.Add(9 * time.Second).Format(http.TimeFormat): 9 * time.Second,
		now.Add(-time.Hour).Format(http.TimeFormat):      0,
	}
	for in, want := range cases {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestOpenAIStopsRetryingWhenContextEnds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := NewOpenAI(OpenAIConfig{BaseURL: srv.URL, MaxRetries: 5, BaseDelay: time.Hour,
		Sleep: func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }})
	_, err := c.Complete(ctx, Request{Model: "m", Prompt: "p"})
	if err == nil || errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("got %v", err)
	}
}
