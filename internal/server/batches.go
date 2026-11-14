package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/Matthew-Klette/inference-queue/internal/auth"
	"github.com/Matthew-Klette/inference-queue/internal/store"
)

type datasetItem struct {
	Ref      string  `json:"ref,omitempty"`
	Prompt   string  `json:"prompt"`
	Expected *string `json:"expected,omitempty"`
}

type createRequest struct {
	Model       string          `json:"model"`
	Params      json.RawMessage `json:"params,omitempty"`
	Prompts     []string        `json:"prompts,omitempty"`
	Dataset     []datasetItem   `json:"dataset,omitempty"`
	WebhookURL  string          `json:"webhook_url,omitempty"`
	MaxAttempts int             `json:"max_attempts,omitempty"`
}

// batchView is the public representation of a batch.
type batchView struct {
	store.Batch
	Pending int `json:"pending"`
	Eval    *struct {
		Scored  int     `json:"scored"`
		Matched int     `json:"matched"`
		Score   float64 `json:"exact_match"`
	} `json:"eval,omitempty"`
}

func view(b store.Batch) batchView {
	v := batchView{Batch: b, Pending: b.Total - b.Succeeded - b.Failed}
	if b.Scored > 0 {
		v.Eval = &struct {
			Scored  int     `json:"scored"`
			Matched int     `json:"matched"`
			Score   float64 `json:"exact_match"`
		}{b.Scored, b.Matched, float64(b.Matched) / float64(b.Scored)}
	}
	return v
}

// canonicalParams re-encodes a JSON object with sorted keys so that equivalent
// parameter sets share a cache key and an idempotency hash.
func canonicalParams(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return json.RawMessage(`{}`), nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, errors.New("params must be a JSON object")
	}
	out, err := json.Marshal(m)
	return out, err
}

func (s *Server) createBatch(w http.ResponseWriter, r *http.Request) {
	key := keyFrom(r.Context())
	r.Body = http.MaxBytesReader(w, r.Body, s.Opts.MaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req createRequest
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "too_large", "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON: "+err.Error())
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "unexpected data after JSON body")
		return
	}

	items, err := s.validate(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	idem := r.Header.Get("Idempotency-Key")
	if len(idem) > 255 {
		writeError(w, http.StatusBadRequest, "invalid_request", "Idempotency-Key must be at most 255 characters")
		return
	}

	// Hash the canonical form so key order and whitespace do not matter.
	canonical, _ := json.Marshal(struct {
		M string
		P json.RawMessage
		I []store.NewItem
		W string
		A int
	}{req.Model, req.Params, items, req.WebhookURL, req.MaxAttempts})

	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = s.Opts.MaxAttempts
	}
	b, replayed, err := s.Batches.Create(r.Context(), store.NewBatch{
		APIKeyID: key.ID, Model: req.Model, Params: req.Params, Items: items, WebhookURL: req.WebhookURL,
		IdempotencyKey: idem, RequestHash: store.HashRequest(canonical), MaxAttempts: maxAttempts,
	})
	switch {
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict",
			"this Idempotency-Key was already used with a different request body")
		return
	case err != nil:
		s.internal(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/batches/"+b.ID.String())
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, view(b))
		return
	}
	writeJSON(w, http.StatusAccepted, view(b))
}

func (s *Server) validate(req *createRequest) ([]store.NewItem, error) {
	if req.Model == "" || len(req.Model) > 200 {
		return nil, errors.New("model is required and must be at most 200 characters")
	}
	if (len(req.Prompts) > 0) == (len(req.Dataset) > 0) {
		return nil, errors.New("provide exactly one of prompts or dataset")
	}
	params, err := canonicalParams(req.Params)
	if err != nil {
		return nil, err
	}
	req.Params = params
	if req.MaxAttempts < 0 || req.MaxAttempts > 20 {
		return nil, errors.New("max_attempts must be between 1 and 20")
	}
	if req.WebhookURL != "" {
		u, err := url.Parse(req.WebhookURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, errors.New("webhook_url must be an absolute http or https URL")
		}
	}

	var items []store.NewItem
	if len(req.Prompts) > 0 {
		for _, p := range req.Prompts {
			items = append(items, store.NewItem{Prompt: p})
		}
	} else {
		for _, d := range req.Dataset {
			items = append(items, store.NewItem{Ref: d.Ref, Prompt: d.Prompt, Expected: d.Expected})
		}
	}
	if len(items) > s.Opts.MaxItems {
		return nil, fmt.Errorf("a batch may contain at most %d items", s.Opts.MaxItems)
	}
	for i, it := range items {
		if it.Prompt == "" {
			return nil, fmt.Errorf("item %d: prompt is empty", i)
		}
		if len(it.Prompt) > s.Opts.MaxPromptBytes {
			return nil, fmt.Errorf("item %d: prompt exceeds %d bytes", i, s.Opts.MaxPromptBytes)
		}
		if !utf8.ValidString(it.Prompt) {
			return nil, fmt.Errorf("item %d: prompt is not valid UTF-8", i)
		}
	}
	return items, nil
}

// loadOwned fetches a batch and enforces ownership: only the creating key or
// an admin may see it. Other keys get 404 so ids cannot be probed.
func (s *Server) loadOwned(w http.ResponseWriter, r *http.Request) (store.Batch, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "batch not found")
		return store.Batch{}, false
	}
	b, err := s.Batches.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "batch not found")
		return b, false
	}
	if err != nil {
		s.internal(w, r, err)
		return b, false
	}
	key := keyFrom(r.Context())
	if b.APIKeyID != key.ID && !auth.HasScope(key.Scopes, auth.ScopeAdmin) {
		writeError(w, http.StatusNotFound, "not_found", "batch not found")
		return b, false
	}
	return b, true
}

func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	if b, ok := s.loadOwned(w, r); ok {
		writeJSON(w, http.StatusOK, view(b))
	}
}

func (s *Server) listBatches(w http.ResponseWriter, r *http.Request) {
	limit := intParam(r, "limit", 50, 1, 200)
	key := keyFrom(r.Context())
	var owner *uuid.UUID
	if !auth.HasScope(key.Scopes, auth.ScopeAdmin) {
		owner = &key.ID
	}
	batches, err := s.Batches.List(r.Context(), owner, limit)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	out := make([]batchView, len(batches))
	for i, b := range batches {
		out[i] = view(b)
	}
	writeJSON(w, http.StatusOK, map[string]any{"batches": out})
}

func (s *Server) replayDead(w http.ResponseWriter, r *http.Request) {
	b, ok := s.loadOwned(w, r)
	if !ok {
		return
	}
	n, err := s.Batches.ReplayDead(r.Context(), b.ID)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"replayed": n})
}

func intParam(r *http.Request, name string, def, lo, hi int) int {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return max(lo, min(hi, n))
}
