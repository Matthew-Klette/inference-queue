// Package ratelimit provides a per-key token bucket with an in-memory and a
// Redis implementation.
package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter decides whether a request for key may proceed.
type Limiter interface {
	// Allow takes one token. When denied, retryAfter says how long until a
	// token is available.
	Allow(ctx context.Context, key string, rate float64, burst int) (ok bool, retryAfter time.Duration, err error)
}

// Memory is a per-process token bucket.
type Memory struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewMemory returns an in-memory limiter.
func NewMemory() *Memory { return &Memory{buckets: map[string]*bucket{}, now: time.Now} }

func (m *Memory) Allow(_ context.Context, key string, rate float64, burst int) (bool, time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	b, ok := m.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(burst), last: now}
		m.buckets[key] = b
	}
	b.tokens = math.Min(float64(burst), b.tokens+now.Sub(b.last).Seconds()*rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0, nil
	}
	return false, waitFor(1-b.tokens, rate), nil
}

func waitFor(missing, rate float64) time.Duration {
	if rate <= 0 {
		return time.Hour
	}
	return time.Duration(math.Ceil(missing/rate*1000)) * time.Millisecond
}

// redisScript is an atomic token bucket. State lives in a hash of
// {tokens, ts}; the clock is Redis TIME so that API replicas with skewed
// clocks agree. It returns {allowed, retry_after_ms}.
var redisScript = redis.NewScript(`
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = burst
  ts = now
end
tokens = math.min(burst, tokens + math.max(0, now - ts) / 1000 * rate)
local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) / rate * 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, retry}
`)

// Redis is a token bucket shared across API replicas.
type Redis struct {
	Client redis.UniversalClient
	Prefix string
}

func (r Redis) Allow(ctx context.Context, key string, rate float64, burst int) (bool, time.Duration, error) {
	res, err := redisScript.Run(ctx, r.Client, []string{r.Prefix + key}, rate, burst).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}

// FailOpen wraps a primary limiter; if it errors (for example Redis is down)
// the request is checked against the fallback instead of being rejected.
type FailOpen struct {
	Primary, Fallback Limiter
	OnError           func(error)
}

func (f FailOpen) Allow(ctx context.Context, key string, rate float64, burst int) (bool, time.Duration, error) {
	ok, wait, err := f.Primary.Allow(ctx, key, rate, burst)
	if err == nil {
		return ok, wait, nil
	}
	if f.OnError != nil {
		f.OnError(err)
	}
	return f.Fallback.Allow(ctx, key, rate, burst)
}
