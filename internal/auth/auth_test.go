package auth

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/Matthew-Klette/inference-queue/internal/store"
)

type fakeKeys map[string]store.APIKey

func (f fakeKeys) ByHash(_ context.Context, h []byte) (store.APIKey, error) {
	if k, ok := f[string(h)]; ok {
		return k, nil
	}
	return store.APIKey{}, store.ErrNotFound
}

func TestGenerateAndHash(t *testing.T) {
	plain, hash, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(plain, KeyPrefix) || len(plain) != len(KeyPrefix)+48 {
		t.Fatalf("unexpected key shape %q", plain)
	}
	if string(Hash(plain)) != string(hash) || len(hash) != 32 {
		t.Fatal("hash mismatch")
	}
	other, _, _ := Generate()
	if other == plain {
		t.Fatal("keys must be random")
	}
}

func TestAuthenticate(t *testing.T) {
	plain, hash, _ := Generate()
	rec := store.APIKey{ID: uuid.New(), KeyHash: hash, Scopes: []string{ScopeRead}}
	keys := fakeKeys{string(hash): rec}

	got, err := Authenticate(context.Background(), keys, "Bearer "+plain)
	if err != nil || got.ID != rec.ID {
		t.Fatalf("valid key rejected: %v", err)
	}
	for _, h := range []string{"", "Bearer ", "Bearer nope", plain, "Basic " + plain, "bearer " + plain} {
		if _, err := Authenticate(context.Background(), keys, h); err != ErrUnauthorized {
			t.Errorf("header %q: got %v, want ErrUnauthorized", h, err)
		}
	}
}

func TestAuthenticateRejectsHashMismatchFromStore(t *testing.T) {
	plain, _, _ := Generate()
	// A store that returns a record whose hash does not match must not authenticate.
	bad := fakeKeys{string(Hash(plain)): {ID: uuid.New(), KeyHash: Hash("something else")}}
	if _, err := Authenticate(context.Background(), bad, "Bearer "+plain); err != ErrUnauthorized {
		t.Fatalf("got %v", err)
	}
}

func TestScopes(t *testing.T) {
	if !HasScope([]string{ScopeSubmit}, ScopeSubmit) || HasScope([]string{ScopeSubmit}, ScopeRead) {
		t.Fatal("exact scope matching broken")
	}
	if !HasScope([]string{ScopeAdmin}, ScopeRead) || !HasScope([]string{ScopeAdmin}, ScopeSubmit) {
		t.Fatal("admin must imply all scopes")
	}
	if HasScope(nil, ScopeRead) || ValidScope("root") || !ValidScope("admin") {
		t.Fatal("scope validation broken")
	}
}
