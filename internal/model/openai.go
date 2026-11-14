package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// OpenAIConfig configures the OpenAI-compatible client. It works with any
// server that implements POST /v1/chat/completions, such as llama.cpp's
// server, vLLM or Ollama.
type OpenAIConfig struct {
	BaseURL    string        // e.g. http://localhost:8080 (without /v1)
	APIKey     string        // optional bearer token
	Timeout    time.Duration // per HTTP attempt
	MaxRetries int           // retries after the first attempt on 429/5xx/network errors
	BaseDelay  time.Duration // delay before the first retry when no Retry-After is sent
	MaxDelay   time.Duration // cap on any single wait, including Retry-After
	HTTPClient *http.Client
	// Sleep waits for d or until ctx ends. Injectable for tests.
	Sleep func(ctx context.Context, d time.Duration) error
}

// OpenAI is a Backend speaking the chat completions protocol.
type OpenAI struct {
	cfg OpenAIConfig
	hc  *http.Client
}

// NewOpenAI returns a client with defaults applied.
func NewOpenAI(cfg OpenAIConfig) *OpenAI {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 500 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 30 * time.Second
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &OpenAI{cfg: cfg, hc: hc}
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// Complete sends the prompt as a single user message.
func (c *OpenAI) Complete(ctx context.Context, req Request) (Response, error) {
	body, err := buildBody(req)
	if err != nil {
		return Response{}, Permanent(err)
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		resp, retryAfter, err := c.once(ctx, body)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if IsPermanent(err) || ctx.Err() != nil || attempt >= c.cfg.MaxRetries {
			return Response{}, lastErr
		}
		wait := retryAfter
		if wait <= 0 {
			wait = c.cfg.BaseDelay << attempt
		}
		wait = min(wait, c.cfg.MaxDelay)
		if err := c.cfg.Sleep(ctx, wait); err != nil {
			return Response{}, lastErr
		}
	}
}

func buildBody(req Request) ([]byte, error) {
	m := map[string]json.RawMessage{}
	if len(req.Params) > 0 && string(req.Params) != "null" {
		if err := json.Unmarshal(req.Params, &m); err != nil {
			return nil, fmt.Errorf("params must be a JSON object: %w", err)
		}
	}
	// Params must not be able to override the model or messages.
	model, _ := json.Marshal(req.Model)
	msgs, _ := json.Marshal([]chatMessage{{Role: "user", Content: req.Prompt}})
	m["model"] = model
	m["messages"] = msgs
	m["stream"] = json.RawMessage("false")
	return json.Marshal(m)
}

// once performs a single HTTP attempt. Non-retryable HTTP statuses return a
// permanent error. The duration is the server's Retry-After, if any.
func (c *OpenAI) once(ctx context.Context, body []byte) (Response, time.Duration, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(actx, http.MethodPost, c.cfg.BaseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Response{}, 0, Permanent(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	res, err := c.hc.Do(httpReq)
	if err != nil {
		return Response{}, 0, fmt.Errorf("model request: %w", err) // network error or timeout: retryable
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return Response{}, 0, fmt.Errorf("read model response: %w", err)
	}

	switch {
	case res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500:
		return Response{}, parseRetryAfter(res.Header.Get("Retry-After"), time.Now()),
			fmt.Errorf("model returned %d: %s", res.StatusCode, snippet(data))
	case res.StatusCode >= 400:
		return Response{}, 0, Permanent(fmt.Errorf("model returned %d: %s", res.StatusCode, snippet(data)))
	}
	var cr chatResponse
	if err := json.Unmarshal(data, &cr); err != nil {
		return Response{}, 0, fmt.Errorf("decode model response: %w", err)
	}
	if len(cr.Choices) == 0 {
		return Response{}, 0, errors.New("model response had no choices")
	}
	return Response{
		Text:             cr.Choices[0].Message.Content,
		PromptTokens:     cr.Usage.PromptTokens,
		CompletionTokens: cr.Usage.CompletionTokens,
	}, 0, nil
}

// parseRetryAfter understands both delta-seconds and HTTP-date forms.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil && secs >= 0 {
		return time.Duration(secs * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
