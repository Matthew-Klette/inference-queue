package queue

import (
	"testing"
	"time"
)

func TestBackoffCeilingDoublesAndCaps(t *testing.T) {
	base, max := time.Second, 10*time.Second
	// rnd=1 (just under) yields the ceiling; rnd=0 yields half of it.
	cases := []struct {
		attempt int
		ceiling time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {4, 8 * time.Second}, {5, 10 * time.Second}, {30, 10 * time.Second}}
	for _, c := range cases {
		lo := Backoff(c.attempt, base, max, func() float64 { return 0 })
		hi := Backoff(c.attempt, base, max, func() float64 { return 0.999999 })
		if lo != c.ceiling/2 {
			t.Errorf("attempt %d: low = %v, want %v", c.attempt, lo, c.ceiling/2)
		}
		if hi < lo || hi > c.ceiling {
			t.Errorf("attempt %d: high = %v outside [%v, %v]", c.attempt, hi, lo, c.ceiling)
		}
	}
}

func TestBackoffTreatsNonPositiveAttemptAsFirst(t *testing.T) {
	got := Backoff(0, time.Second, time.Minute, func() float64 { return 0 })
	if got != 500*time.Millisecond {
		t.Fatalf("got %v", got)
	}
}

func TestBackoffJitterSpreadsRetries(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[Backoff(3, time.Second, time.Minute, nil)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("expected jittered delays, saw only %d distinct values", len(seen))
	}
}
