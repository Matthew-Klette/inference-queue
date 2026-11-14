package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Matthew-Klette/inference-queue/internal/obs"
)

// streamEvents serves batch progress as Server-Sent Events. It polls the batch
// row rather than using LISTEN/NOTIFY: that keeps it correct across API
// replicas and through connection poolers, and one indexed primary-key read
// per interval per open stream is cheap.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	b, ok := s.loadOwned(w, r)
	if !ok {
		return
	}
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	send := func(event string, v any) bool {
		data, _ := json.Marshal(v)
		_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil {
			return false
		}
		return rc.Flush() == nil
	}

	var shutdown <-chan struct{}
	if s.Opts.ShutdownContext != nil {
		shutdown = s.Opts.ShutdownContext.Done()
	}
	var lastSucceeded, lastFailed, lastStatus = -1, -1, ""
	tick := time.NewTicker(s.Opts.SSEInterval)
	defer tick.Stop()
	beat := time.NewTicker(s.Opts.SSEHeartbeat)
	defer beat.Stop()

	for {
		if b.Succeeded != lastSucceeded || b.Failed != lastFailed || b.Status != lastStatus {
			lastSucceeded, lastFailed, lastStatus = b.Succeeded, b.Failed, b.Status
			if !send("progress", view(b)) {
				return
			}
		}
		if b.CompletedAt != nil {
			send("done", view(b))
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-shutdown:
			send("shutdown", map[string]string{"reason": "server stopping, reconnect"})
			return
		case <-beat.C:
			_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		case <-tick.C:
			nb, err := s.Batches.Get(r.Context(), b.ID)
			if err != nil {
				if r.Context().Err() == nil {
					obs.L(r.Context()).Error("sse poll failed", "err", err)
				}
				return
			}
			b = nb
		}
	}
}

// streamResults writes finished items as newline-delimited JSON, ordered by
// index. Items that have not finished are skipped; clients resume with
// ?after=<last index seen>.
func (s *Server) streamResults(w http.ResponseWriter, r *http.Request) {
	b, ok := s.loadOwned(w, r)
	if !ok {
		return
	}
	after := intParam(r, "after", -1, -1, 1<<30)
	includePrompt := r.URL.Query().Get("include_prompt") == "true"
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "application/x-ndjson")
	h.Set("X-Batch-Status", b.Status)
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)
	for {
		page, err := s.Batches.Results(r.Context(), b.ID, after, 500, includePrompt)
		if err != nil {
			obs.L(r.Context()).Error("results page failed", "err", err)
			return
		}
		if len(page) == 0 {
			return
		}
		_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
		for _, res := range page {
			if err := enc.Encode(res); err != nil {
				return
			}
			after = res.Index
		}
		_ = rc.Flush()
	}
}
