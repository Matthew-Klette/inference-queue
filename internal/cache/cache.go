// Package cache stores model results keyed by a hash of the request so that
// duplicate prompts do not reach the model twice.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Entry is a cached model response.
type Entry struct {
	Text             string `json:"text"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
}

// Cache is a key-value store for Entries. Get returns ok=false on a miss.
type Cache interface {
	Get(ctx context.Context, key string) (Entry, bool, error)
	Set(ctx context.Context, key string, e Entry, ttl time.Duration) error
}

// Key derives the cache key from the model, the sampling parameters and the
// prompt. Params must already be canonical JSON (the API canonicalises it on
// submit) so that {"a":1,"b":2} and {"b":2,"a":1} share a key. Each field is
// length-prefixed so that ("ab","c") and ("a","bc") cannot collide.
func Key(model string, params []byte, prompt string) string {
	h := sha256.New()
	for _, part := range []string{model, string(params), prompt} {
		h.Write([]byte(strconv.Itoa(len(part))))
		h.Write([]byte{':'})
		h.Write([]byte(part))
	}
	return "iq:v1:" + hex.EncodeToString(h.Sum(nil))
}

// Memory is an in-process cache with TTL and a soft size cap. It is the
// fallback when Redis is not configured.
type Memory struct {
	mu      sync.Mutex
	max     int
	entries map[string]memEntry
	now     func() time.Time
}

type memEntry struct {
	e       Entry
	expires time.Time
}

// NewMemory returns an in-memory cache holding at most max entries.
func NewMemory(max int) *Memory {
	return &Memory{max: max, entries: make(map[string]memEntry), now: time.Now}
}

func (m *Memory) Get(_ context.Context, key string) (Entry, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.entries[key]
	if !ok {
		return Entry{}, false, nil
	}
	if !m.now().Before(v.expires) {
		delete(m.entries, key)
		return Entry{}, false, nil
	}
	return v.e, true, nil
}

func (m *Memory) Set(_ context.Context, key string, e Entry, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) >= m.max {
		// Drop expired entries first; if still full, drop an arbitrary one.
		now := m.now()
		for k, v := range m.entries {
			if !now.Before(v.expires) {
				delete(m.entries, k)
			}
		}
		for k := range m.entries {
			if len(m.entries) < m.max {
				break
			}
			delete(m.entries, k)
		}
	}
	m.entries[key] = memEntry{e: e, expires: m.now().Add(ttl)}
	return nil
}

// Redis is a Redis-backed cache shared by all workers.
type Redis struct{ Client redis.UniversalClient }

func (r Redis) Get(ctx context.Context, key string) (Entry, bool, error) {
	b, err := r.Client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, err
	}
	var e Entry
	if err := json.Unmarshal(b, &e); err != nil {
		return Entry{}, false, nil // corrupt entry: treat as a miss
	}
	return e, true, nil
}

func (r Redis) Set(ctx context.Context, key string, e Entry, ttl time.Duration) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return r.Client.Set(ctx, key, b, ttl).Err()
}
