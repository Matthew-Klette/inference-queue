// Package events delivers outbox events to webhooks.
package events

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Sign returns the value of the X-IQ-Signature header: "t=<unix>,v1=<hex>".
// The MAC covers "<unix>.<body>" so a captured body cannot be replayed with a
// fresh timestamp.
func Sign(secret string, ts time.Time, body []byte) string {
	t := strconv.FormatInt(ts.Unix(), 10)
	return "t=" + t + ",v1=" + mac(secret, t, body)
}

func mac(secret, t string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(t))
	m.Write([]byte{'.'})
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// Verification errors.
var (
	ErrBadSignature = errors.New("signature mismatch")
	ErrStale        = errors.New("signature timestamp outside tolerance")
)

// Verify checks a signature header against body. tolerance bounds the age of
// the timestamp; zero disables the age check.
func Verify(secret, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var t, v1 string
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch k {
		case "t":
			t = v
		case "v1":
			v1 = v
		}
	}
	if t == "" || v1 == "" {
		return ErrBadSignature
	}
	got, err := hex.DecodeString(v1)
	if err != nil {
		return ErrBadSignature
	}
	want, _ := hex.DecodeString(mac(secret, t, body))
	if !hmac.Equal(got, want) {
		return ErrBadSignature
	}
	if tolerance > 0 {
		sec, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return ErrBadSignature
		}
		d := now.Sub(time.Unix(sec, 0))
		if d < 0 {
			d = -d
		}
		if d > tolerance {
			return ErrStale
		}
	}
	return nil
}
