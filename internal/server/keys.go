package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"

	"github.com/google/uuid"

	"github.com/Matthew-Klette/inference-queue/internal/auth"
	"github.com/Matthew-Klette/inference-queue/internal/store"
)

type createKeyRequest struct {
	Name      string   `json:"name"`
	Scopes    []string `json:"scopes"`
	RateRPS   float64  `json:"rate_rps"`
	RateBurst int      `json:"rate_burst"`
}

type createKeyResponse struct {
	store.APIKey
	// Key is the plaintext secret. It is returned once and never stored.
	Key string `json:"key"`
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req createKeyRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" || len(req.Name) > 100 {
		writeError(w, http.StatusBadRequest, "invalid_request", "name is required (max 100 characters)")
		return
	}
	if len(req.Scopes) == 0 {
		req.Scopes = []string{auth.ScopeSubmit, auth.ScopeRead}
	}
	for _, sc := range req.Scopes {
		if !auth.ValidScope(sc) {
			writeError(w, http.StatusBadRequest, "invalid_request", "unknown scope "+sc)
			return
		}
	}
	slices.Sort(req.Scopes)
	req.Scopes = slices.Compact(req.Scopes)
	if req.RateRPS <= 0 {
		req.RateRPS = 20
	}
	if req.RateBurst <= 0 {
		req.RateBurst = 40
	}
	plain, hash, err := auth.Generate()
	if err != nil {
		s.internal(w, r, err)
		return
	}
	rec, err := s.Keys.Create(r.Context(), store.APIKey{
		Name: req.Name, KeyHash: hash, Scopes: req.Scopes, RateRPS: req.RateRPS, RateBurst: req.RateBurst,
	})
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, createKeyResponse{APIKey: rec, Key: plain})
}

func (s *Server) listKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.Keys.List(r.Context())
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (s *Server) revokeKey(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "key not found")
		return
	}
	switch err := s.Keys.Revoke(r.Context(), id); {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "key not found")
	case err != nil:
		s.internal(w, r, err)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
