// Package model defines the backend interface the workers call, plus a fake
// backend for tests and an OpenAI-compatible HTTP client.
package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Request is one completion call.
type Request struct {
	Model  string
	Prompt string
	Params json.RawMessage // opaque sampling parameters (temperature, max_tokens, ...)
}

// Response is the model output.
type Response struct {
	Text             string
	PromptTokens     int
	CompletionTokens int
}

// Backend produces a completion for a prompt.
type Backend interface {
	Complete(ctx context.Context, req Request) (Response, error)
}

// ErrPermanent marks errors that will not succeed on retry (bad request,
// content rejected). Items failing with a permanent error are dead-lettered
// immediately instead of burning their remaining attempts.
var ErrPermanent = errors.New("permanent model error")

// ErrCircuitOpen is returned without calling the backend while the circuit
// breaker is open. It says nothing about the item, so workers requeue the item
// without charging an attempt.
var ErrCircuitOpen = errors.New("model circuit breaker open")

// Permanent wraps err as a permanent failure.
func Permanent(err error) error { return fmt.Errorf("%w: %w", ErrPermanent, err) }

// IsPermanent reports whether err is a permanent failure.
func IsPermanent(err error) bool { return errors.Is(err, ErrPermanent) }
