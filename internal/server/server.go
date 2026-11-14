// Package server implements the REST API.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"

	"github.com/Matthew-Klette/inference-queue/api"
	"github.com/Matthew-Klette/inference-queue/internal/auth"
	"github.com/Matthew-Klette/inference-queue/internal/obs"
	"github.com/Matthew-Klette/inference-queue/internal/queue"
	"github.com/Matthew-Klette/inference-queue/internal/ratelimit"
	"github.com/Matthew-Klette/inference-queue/internal/store"
)

// Options tunes request limits.
type Options struct {
	MaxItems        int           // items per batch, default 10000
	MaxPromptBytes  int           // bytes per prompt, default 64 KiB
	MaxBodyBytes    int64         // request body, default 16 MiB
	MaxAttempts     int           // default attempts per item
	SSEInterval     time.Duration // progress poll interval, default 500ms
	SSEHeartbeat    time.Duration // keep-alive comment interval, default 15s
	DefaultWebhook  bool          // true if a service-wide webhook is configured, so webhook_url is optional
	ShutdownContext context.Context
}

func (o *Options) defaults() {
	if o.MaxItems <= 0 {
		o.MaxItems = 10000
	}
	if o.MaxPromptBytes <= 0 {
		o.MaxPromptBytes = 64 << 10
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 16 << 20
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.SSEInterval <= 0 {
		o.SSEInterval = 500 * time.Millisecond
	}
	if o.SSEHeartbeat <= 0 {
		o.SSEHeartbeat = 15 * time.Second
	}
}

// Server wires the HTTP handlers to the stores.
type Server struct {
	Keys    store.Keys
	Batches store.Batches
	Queue   *queue.Queue
	Limiter ratelimit.Limiter
	Metrics *obs.Metrics
	Log     *slog.Logger
	// Ready reports whether dependencies are reachable; used by /readyz.
	Ready func(ctx context.Context) error
	Opts  Options

	mux    *http.ServeMux
	routes []string
}

type ctxKey int

const (
	keyAPIKey ctxKey = iota
	keyRoute
	keyRequestID
)

// Handler builds the router and returns the root handler with middleware applied.
func (s *Server) Handler() http.Handler {
	s.Opts.defaults()
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.Metrics == nil {
		s.Metrics = obs.NewMetrics()
	}
	if s.Limiter == nil {
		s.Limiter = ratelimit.NewMemory()
	}
	s.mux = http.NewServeMux()
	s.routes = nil

	s.handle("GET /healthz", "", s.healthz)
	s.handle("GET /readyz", "", s.readyz)
	s.handle("GET /metrics", "", promhttp.HandlerFor(s.Metrics.Registry, promhttp.HandlerOpts{}).ServeHTTP)
	s.handle("GET /openapi.yaml", "", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(api.Spec)
	})

	s.handle("POST /v1/batches", auth.ScopeSubmit, s.createBatch)
	s.handle("GET /v1/batches", auth.ScopeRead, s.listBatches)
	s.handle("GET /v1/batches/{id}", auth.ScopeRead, s.getBatch)
	s.handle("GET /v1/batches/{id}/events", auth.ScopeRead, s.streamEvents)
	s.handle("GET /v1/batches/{id}/results", auth.ScopeRead, s.streamResults)
	s.handle("POST /v1/batches/{id}/replay", auth.ScopeAdmin, s.replayDead)

	s.handle("POST /v1/keys", auth.ScopeAdmin, s.createKey)
	s.handle("GET /v1/keys", auth.ScopeAdmin, s.listKeys)
	s.handle("DELETE /v1/keys/{id}", auth.ScopeAdmin, s.revokeKey)

	return s.middleware(s.mux)
}

// Routes returns the registered "METHOD /path" patterns, sorted.
func (s *Server) Routes() []string {
	out := append([]string(nil), s.routes...)
	sort.Strings(out)
	return out
}

// handle registers a route. An empty scope means the route is public.
func (s *Server) handle(pattern, scope string, h http.HandlerFunc) {
	s.routes = append(s.routes, pattern)
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if holder, ok := r.Context().Value(keyRoute).(*string); ok {
			*holder = pattern
		}
		if scope != "" {
			key, err := auth.Authenticate(r.Context(), s.Keys, r.Header.Get("Authorization"))
			if err != nil {
				if errors.Is(err, auth.ErrUnauthorized) {
					w.Header().Set("WWW-Authenticate", `Bearer realm="inference-queue"`)
					writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API key")
					return
				}
				s.Log.ErrorContext(r.Context(), "auth lookup failed", "err", err)
				writeError(w, http.StatusInternalServerError, "internal", "internal error")
				return
			}
			if !auth.HasScope(key.Scopes, scope) {
				writeError(w, http.StatusForbidden, "forbidden", "this key lacks the "+scope+" scope")
				return
			}
			ok, wait, err := s.Limiter.Allow(r.Context(), key.ID.String(), key.RateRPS, key.RateBurst)
			if err != nil {
				s.Log.WarnContext(r.Context(), "rate limiter error, allowing request", "err", err)
			} else if !ok {
				s.Metrics.RateLimited.Inc()
				w.Header().Set("Retry-After", strconv.Itoa(int((wait+time.Second-1)/time.Second)))
				writeError(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded for this key")
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), keyAPIKey, key))
		}
		h(w, r)
	})
}

func keyFrom(ctx context.Context) store.APIKey {
	k, _ := ctx.Value(keyAPIKey).(store.APIKey)
	return k
}

// middleware adds request ids, tracing, access logs and HTTP metrics.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-Id")
		if id == "" || len(id) > 100 {
			id = newRequestID()
		}
		w.Header().Set("X-Request-Id", id)
		var route string
		logger := s.Log.With("request_id", id)
		ctx := context.WithValue(r.Context(), keyRoute, &route)
		ctx = context.WithValue(ctx, keyRequestID, id)
		ctx = obs.WithLogger(ctx, logger)
		ctx, span := obs.Tracer().Start(ctx, r.Method+" "+r.URL.Path)
		defer span.End()

		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))

		if route == "" {
			route = "unmatched"
		}
		span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.status_code", sw.status))
		dur := time.Since(start)
		s.Metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(sw.status)).Inc()
		s.Metrics.HTTPDuration.WithLabelValues(route).Observe(dur.Seconds())
		if route != "GET /healthz" && route != "GET /metrics" {
			attrs := []any{"method", r.Method, "route", route, "status", sw.status, "duration_ms", dur.Milliseconds()}
			if k := keyFrom(ctx); k.ID != uuid.Nil {
				attrs = append(attrs, "api_key_id", k.ID)
			}
			logger.Info("request", attrs...)
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status, w.wrote = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer (flush, deadlines).
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if s.Ready != nil {
		if err := s.Ready(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not_ready", err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) internal(w http.ResponseWriter, r *http.Request, err error) {
	obs.L(r.Context()).ErrorContext(r.Context(), "request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal", "internal error")
}
