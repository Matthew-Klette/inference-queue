// Package obs holds logging, metrics and tracing setup.
package obs

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// QueueDepthFunc reports ready, delayed, leased and dead item counts.
type QueueDepthFunc func(ctx context.Context) (ready, delayed, leased, dead int, err error)

// Metrics is the set of Prometheus instruments used across the service.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests    *prometheus.CounterVec
	HTTPDuration    *prometheus.HistogramVec
	RateLimited     prometheus.Counter
	LeaseDuration   prometheus.Histogram
	QueueWait       prometheus.Histogram
	ModelDuration   *prometheus.HistogramVec
	ItemsProcessed  *prometheus.CounterVec
	CacheLookups    *prometheus.CounterVec
	DependencyError *prometheus.CounterVec
	BreakerState    *prometheus.GaugeVec
	WebhookAttempts *prometheus.CounterVec
	LeasesLost      prometheus.Counter
}

// NewMetrics registers all instruments on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	m := &Metrics{Registry: reg}
	m.HTTPRequests = f.NewCounterVec(prometheus.CounterOpts{
		Name: "iq_http_requests_total", Help: "HTTP requests by route pattern and status."}, []string{"route", "status"})
	m.HTTPDuration = f.NewHistogramVec(prometheus.HistogramOpts{
		Name: "iq_http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets}, []string{"route"})
	m.RateLimited = f.NewCounter(prometheus.CounterOpts{
		Name: "iq_rate_limited_total", Help: "Requests rejected by the per-key rate limiter."})
	m.LeaseDuration = f.NewHistogram(prometheus.HistogramOpts{
		Name: "iq_lease_duration_seconds", Help: "Time spent in the lease query.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1}})
	m.QueueWait = f.NewHistogram(prometheus.HistogramOpts{
		Name: "iq_queue_wait_seconds", Help: "Time between an item becoming runnable and being leased.",
		Buckets: []float64{.01, .05, .1, .5, 1, 5, 15, 60, 300, 1800}})
	m.ModelDuration = f.NewHistogramVec(prometheus.HistogramOpts{
		Name: "iq_model_duration_seconds", Help: "Model backend call latency.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}}, []string{"model", "outcome"})
	m.ItemsProcessed = f.NewCounterVec(prometheus.CounterOpts{
		Name: "iq_items_processed_total", Help: "Items by outcome: succeeded, retried, dead, requeued, lease_lost."}, []string{"outcome"})
	m.CacheLookups = f.NewCounterVec(prometheus.CounterOpts{
		Name: "iq_cache_lookups_total", Help: "Result cache lookups. hit ratio = hit / (hit + miss)."}, []string{"result"})
	m.DependencyError = f.NewCounterVec(prometheus.CounterOpts{
		Name: "iq_dependency_errors_total", Help: "Errors talking to optional dependencies (redis)."}, []string{"dependency"})
	m.BreakerState = f.NewGaugeVec(prometheus.GaugeOpts{
		Name: "iq_model_breaker_open", Help: "1 while the model circuit breaker is open."}, []string{"model_backend"})
	m.WebhookAttempts = f.NewCounterVec(prometheus.CounterOpts{
		Name: "iq_webhook_attempts_total", Help: "Webhook delivery attempts by outcome."}, []string{"outcome"})
	m.LeasesLost = f.NewCounter(prometheus.CounterOpts{
		Name: "iq_leases_lost_total", Help: "Completions rejected because the lease had expired."})
	return m
}

// RegisterQueueDepth adds a gauge collector that queries depth on each scrape.
func (m *Metrics) RegisterQueueDepth(fn QueueDepthFunc) {
	m.Registry.MustRegister(&depthCollector{fn: fn})
}

type depthCollector struct{ fn QueueDepthFunc }

var depthDesc = prometheus.NewDesc("iq_queue_items", "Items in the queue by state.", []string{"state"}, nil)

func (c *depthCollector) Describe(ch chan<- *prometheus.Desc) { ch <- depthDesc }

func (c *depthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ready, delayed, leased, dead, err := c.fn(ctx)
	if err != nil {
		return // omit the series rather than report a wrong value
	}
	for state, v := range map[string]int{"ready": ready, "delayed": delayed, "leased": leased, "dead": dead} {
		ch <- prometheus.MustNewConstMetric(depthDesc, prometheus.GaugeValue, float64(v), state)
	}
}
