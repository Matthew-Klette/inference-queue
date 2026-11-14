package queue

import (
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before retry number attempt (1 for the first
// retry). The ceiling doubles from base up to max, and the result is "equal
// jitter": half the ceiling plus a random share of the other half. That keeps
// a floor on the delay while still spreading retries from a failed burst.
//
// rnd must return a value in [0, 1). Pass nil to use the global source.
func Backoff(attempt int, base, max time.Duration, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if rnd == nil {
		rnd = rand.Float64
	}
	ceiling := base
	for i := 1; i < attempt && ceiling < max; i++ {
		ceiling *= 2
	}
	if ceiling > max {
		ceiling = max
	}
	half := ceiling / 2
	return half + time.Duration(rnd()*float64(ceiling-half))
}
