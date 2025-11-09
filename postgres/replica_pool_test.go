package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReplicaPool_RoundRobin(t *testing.T) {
	t.Parallel()

	pool1 := &pgxpool.Pool{}
	pool2 := &pgxpool.Pool{}
	pool3 := &pgxpool.Pool{}

	rp := &ReplicaPool{
		pools:    []*pgxpool.Pool{pool1, pool2, pool3},
		strategy: ReplicaStrategyRoundRobin,
	}

	ctx := context.Background()

	expected := []*pgxpool.Pool{pool1, pool2, pool3, pool1, pool2, pool3}
	for i, want := range expected {
		got := rp.pickPool(ctx)
		if got != want {
			t.Errorf("iteration %d: expected pool %p, got %p", i, want, got)
		}
	}
}

func TestReplicaPool_Random(t *testing.T) {
	t.Parallel()

	pool1 := &pgxpool.Pool{}
	pool2 := &pgxpool.Pool{}

	rp := &ReplicaPool{
		pools:    []*pgxpool.Pool{pool1, pool2},
		strategy: ReplicaStrategyRandom,
	}

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		got := rp.pickPool(ctx)
		if got != pool1 && got != pool2 {
			t.Errorf("iteration %d: unexpected pool %p", i, got)
		}
	}
}

func TestReplicaPool_EmptyPools(t *testing.T) {
	t.Parallel()

	rp := &ReplicaPool{
		pools:    []*pgxpool.Pool{},
		strategy: ReplicaStrategyRoundRobin,
	}

	ctx := context.Background()
	got := rp.pickPool(ctx)

	if got != nil {
		t.Errorf("expected nil for empty pools, got %p", got)
	}
}

