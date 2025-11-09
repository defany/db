package postgres

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

type replicaSelector interface {
	Next(ctx context.Context) *pgxpool.Pool
	Close()
}

func newReplicaSelector(strategy ReplicaStrategy, pools []*pgxpool.Pool, names []string, log *slog.Logger) replicaSelector {
	if len(pools) == 0 {
		return nil
	}

	switch strategy {
	case ReplicaStrategyRandom:
		return newRandomSelector(pools, names)
	case ReplicaStrategyRoundRobin, "":
		return newRoundRobinSelector(pools, names)
	default:
		if log != nil {
			log.Warn("unknown replica strategy, falling back to round_robin",
				slog.String("strategy", string(strategy)),
			)
		}
		return newRoundRobinSelector(pools, names)
	}
}

type roundRobinSelector struct {
	pools   []*pgxpool.Pool
	names   []string
	counter atomic.Uint64
}

func newRoundRobinSelector(pools []*pgxpool.Pool, names []string) *roundRobinSelector {
	poolsCopy := make([]*pgxpool.Pool, len(pools))
	copy(poolsCopy, pools)

	namesCopy := make([]string, len(names))
	copy(namesCopy, names)

	return &roundRobinSelector{
		pools: poolsCopy,
		names: namesCopy,
	}
}

func (s *roundRobinSelector) Next(_ context.Context) *pgxpool.Pool {
	if len(s.pools) == 0 {
		return nil
	}

	idx := s.counter.Add(1) - 1
	return s.pools[idx%uint64(len(s.pools))]
}

func (s *roundRobinSelector) Close() {
	for _, pool := range s.pools {
		pool.Close()
	}
}

type randomSelector struct {
	pools []*pgxpool.Pool
	names []string
}

func newRandomSelector(pools []*pgxpool.Pool, names []string) *randomSelector {
	poolsCopy := make([]*pgxpool.Pool, len(pools))
	copy(poolsCopy, pools)

	namesCopy := make([]string, len(names))
	copy(namesCopy, names)

	return &randomSelector{
		pools: poolsCopy,
		names: namesCopy,
	}
}

func (s *randomSelector) Next(_ context.Context) *pgxpool.Pool {
	if len(s.pools) == 0 {
		return nil
	}

	idx := rand.IntN(len(s.pools))
	return s.pools[idx]
}

func (s *randomSelector) Close() {
	for _, pool := range s.pools {
		pool.Close()
	}
}
