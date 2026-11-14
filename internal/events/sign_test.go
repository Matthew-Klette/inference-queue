package events

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"batch_id":"b1"}`)
	sig := Sign("s3cret", now, body)
	if !strings.HasPrefix(sig, "t=1700000000,v1=") {
		t.Fatalf("unexpected format %q", sig)
	}
	if err := Verify("s3cret", sig, body, now.Add(time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestVerifyRejectsTampering(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"a":1}`)
	sig := Sign("k", now, body)

	cases := map[string]struct {
		secret, header string
		body           []byte
		at             time.Time
		want           error
	}{
		"wrong secret":  {"other", sig, body, now, ErrBadSignature},
		"modified body": {"k", sig, []byte(`{"a":2}`), now, ErrBadSignature},
		"modified ts":   {"k", strings.Replace(sig, "t=1700000000", "t=1700000001", 1), body, now, ErrBadSignature},
		"garbage":       {"k", "nonsense", body, now, ErrBadSignature},
		"non-hex":       {"k", "t=1,v1=zz", body, now, ErrBadSignature},
		"stale":         {"k", sig, body, now.Add(time.Hour), ErrStale},
		"future":        {"k", sig, body, now.Add(-time.Hour), ErrStale},
	}
	for name, c := range cases {
		if err := Verify(c.secret, c.header, c.body, c.at, 5*time.Minute); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", name, err, c.want)
		}
	}
	if err := Verify("k", sig, body, now.Add(24*time.Hour), 0); err != nil {
		t.Errorf("tolerance 0 should disable the age check: %v", err)
	}
}
