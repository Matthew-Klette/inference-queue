package events

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Matthew-Klette/inference-queue/internal/obs"
	"github.com/Matthew-Klette/inference-queue/internal/queue"
)

// Publisher drains the outbox table to webhooks. Rows are claimed with
// FOR UPDATE SKIP LOCKED and a short locked_until, so several publishers can
// run side by side. Delivery is at-least-once: receivers should de-duplicate
// on the X-IQ-Event-Id header.
type Publisher struct {
	Pool        *pgxpool.Pool
	Secret      string
	Client      *http.Client
	Log         *slog.Logger
	Metrics     *obs.Metrics
	Interval    time.Duration // poll interval, default 1s
	MaxAttempts int           // default 8
	BackoffBase time.Duration // default 5s
	BackoffMax  time.Duration // default 15m
	Timeout     time.Duration // per delivery, default 10s
	BatchSize   int           // rows claimed per poll, default 20
}

type row struct {
	ID       int64
	Type     string
	URL      string
	Payload  []byte
	Attempts int
}

func (p *Publisher) defaults() {
	if p.Interval <= 0 {
		p.Interval = time.Second
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 8
	}
	if p.BackoffBase <= 0 {
		p.BackoffBase = 5 * time.Second
	}
	if p.BackoffMax <= 0 {
		p.BackoffMax = 15 * time.Minute
	}
	if p.Timeout <= 0 {
		p.Timeout = 10 * time.Second
	}
	if p.BatchSize <= 0 {
		p.BatchSize = 20
	}
	if p.Client == nil {
		p.Client = &http.Client{Timeout: p.Timeout}
	}
	if p.Log == nil {
		p.Log = slog.Default()
	}
	if p.Metrics == nil {
		p.Metrics = obs.NewMetrics()
	}
}

// Run polls until ctx is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	p.defaults()
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		n, err := p.Once(ctx)
		if err != nil && ctx.Err() == nil {
			p.Log.Error("outbox poll failed", "err", err)
		}
		if n == p.BatchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Once claims and delivers one page of due events. It returns how many it handled.
func (p *Publisher) Once(ctx context.Context) (int, error) {
	p.defaults()
	// Claim: push locked_until forward so concurrent publishers skip these rows
	// while we deliver, and so a crashed publisher's rows become due again.
	rows, err := p.Pool.Query(ctx, `
		UPDATE outbox SET locked_until = now() + make_interval(secs => $2)
		WHERE id IN (
			SELECT id FROM outbox
			WHERE delivered_at IS NULL AND NOT dead AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY next_attempt_at, id LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING id, event_type, webhook_url, payload::text, attempts`, p.BatchSize, (p.Timeout + 5*time.Second).Seconds())
	if err != nil {
		return 0, err
	}
	var batch []row
	for rows.Next() {
		var r row
		var payload string
		if err := rows.Scan(&r.ID, &r.Type, &r.URL, &payload, &r.Attempts); err != nil {
			rows.Close()
			return 0, err
		}
		r.Payload = []byte(payload)
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, r := range batch {
		p.deliver(ctx, r)
	}
	return len(batch), nil
}

func (p *Publisher) deliver(ctx context.Context, r row) {
	err := p.post(ctx, r)
	if err == nil {
		p.Metrics.WebhookAttempts.WithLabelValues("delivered").Inc()
		if _, e := p.Pool.Exec(ctx, `UPDATE outbox SET delivered_at = now(), attempts = attempts + 1, locked_until = NULL, last_error = NULL WHERE id = $1`, r.ID); e != nil {
			p.Log.Error("mark delivered failed; event may be sent again", "outbox_id", r.ID, "err", e)
		}
		return
	}
	attempts := r.Attempts + 1
	dead := attempts >= p.MaxAttempts
	outcome := "retry"
	if dead {
		outcome = "dead"
	}
	p.Metrics.WebhookAttempts.WithLabelValues(outcome).Inc()
	p.Log.Warn("webhook delivery failed", "outbox_id", r.ID, "attempt", attempts, "dead", dead, "err", err)
	delay := queue.Backoff(attempts, p.BackoffBase, p.BackoffMax, nil)
	if _, e := p.Pool.Exec(ctx, `
		UPDATE outbox SET attempts = $2, dead = $3, last_error = $4, locked_until = NULL,
			next_attempt_at = now() + make_interval(secs => $5)
		WHERE id = $1`, r.ID, attempts, dead, err.Error(), delay.Seconds()); e != nil {
		p.Log.Error("record webhook failure failed", "outbox_id", r.ID, "err", e)
	}
}

func (p *Publisher) post(ctx context.Context, r row) error {
	ctx, cancel := context.WithTimeout(ctx, p.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(r.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "inference-queue/1")
	req.Header.Set("X-IQ-Event", r.Type)
	req.Header.Set("X-IQ-Event-Id", strconv.FormatInt(r.ID, 10))
	if p.Secret != "" {
		req.Header.Set("X-IQ-Signature", Sign(p.Secret, time.Now(), r.Payload))
	}
	res, err := p.Client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("webhook returned %d", res.StatusCode)
	}
	return nil
}

// ErrPrivateAddress is returned when a webhook target resolves to a non-public address.
var ErrPrivateAddress = errors.New("webhook target resolves to a private or loopback address")

// SafeClient returns an http.Client that refuses to connect to loopback,
// private, link-local and unspecified addresses unless allowPrivate is set.
// The check runs on the resolved IP at dial time, which also covers DNS
// rebinding and redirects.
func SafeClient(timeout time.Duration, allowPrivate bool) *http.Client {
	d := &net.Dialer{Timeout: 5 * time.Second}
	if !allowPrivate {
		d.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
				ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
				return ErrPrivateAddress
			}
			return nil
		}
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: &http.Transport{DialContext: d.DialContext, MaxIdleConns: 10, IdleConnTimeout: 30 * time.Second},
	}
}
