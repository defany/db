package cluster

import (
	"time"
)

// JitterRandom returns a random time to jitter with max cap specified
func JitterRandom(d time.Duration) time.Duration {
	var rng Rand
	v := rng.Float64() * float64(d.Nanoseconds())
	return time.Duration(v)
}

func RandomInterval(min, max time.Duration) time.Duration {
	var rng Rand
	return time.Duration(rng.Int63n(max.Nanoseconds()-min.Nanoseconds())+min.Nanoseconds()) * time.Nanosecond
}
