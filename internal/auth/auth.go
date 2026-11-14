// Package auth implements API key generation, hashing and scope checks.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/Matthew-Klette/inference-queue/internal/store"
)

// Scopes.
const (
	ScopeSubmit = "submit"
	ScopeRead   = "read"
	ScopeAdmin  = "admin"
)

// KeyPrefix makes keys recognisable in logs and secret scanners.
const KeyPrefix = "iq_"

// ValidScope reports whether s is a known scope.
func ValidScope(s string) bool {
	return s == ScopeSubmit || s == ScopeRead || s == ScopeAdmin
}

// Generate returns a new random key (shown to the caller once) and its hash.
func Generate() (plaintext string, hash []byte, err error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", nil, err
	}
	plaintext = KeyPrefix + hex.EncodeToString(b[:])
	return plaintext, Hash(plaintext), nil
}

// Hash returns sha256(plaintext). API keys are long random strings, so a
// fast hash is appropriate; a password KDF would only add latency per request.
func Hash(plaintext string) []byte {
	h := sha256.Sum256([]byte(plaintext))
	return h[:]
}

// HasScope reports whether the key grants scope. admin implies every scope.
func HasScope(scopes []string, want string) bool {
	for _, s := range scopes {
		if s == want || s == ScopeAdmin {
			return true
		}
	}
	return false
}

// KeyLookup finds an active key by hash.
type KeyLookup interface {
	ByHash(ctx context.Context, hash []byte) (store.APIKey, error)
}

// ErrUnauthorized is returned for missing, malformed, unknown or revoked keys.
var ErrUnauthorized = errors.New("invalid api key")

// Authenticate resolves a bearer token to a key record. The stored hash is
// compared in constant time even though the lookup is by hash, so a future
// change to a different lookup strategy cannot introduce a timing leak.
func Authenticate(ctx context.Context, keys KeyLookup, header string) (store.APIKey, error) {
	token, ok := strings.CutPrefix(header, "Bearer ")
	token = strings.TrimSpace(token)
	if !ok || token == "" {
		return store.APIKey{}, ErrUnauthorized
	}
	hash := Hash(token)
	rec, err := keys.ByHash(ctx, hash)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.APIKey{}, ErrUnauthorized
		}
		return store.APIKey{}, err
	}
	if subtle.ConstantTimeCompare(rec.KeyHash, hash) != 1 {
		return store.APIKey{}, ErrUnauthorized
	}
	return rec, nil
}
