package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRoundRobinSelector_Next(t *testing.T) {
	t.Parallel()

	// Create mock pools.
	pool1 := &pgxpool.Pool{}
	pool2 := &pgxpool.Pool{}
	pool3 := &pgxpool.Pool{}

	pools := []*pgxpool.Pool{pool1, pool2, pool3}
	names := []string{"replica-1", "replica-2", "replica-3"}

	selector := newRoundRobinSelector(pools, names)

	ctx := context.Background()

	// Test round-robin behavior over multiple iterations.
	expected := []*pgxpool.Pool{pool1, pool2, pool3, pool1, pool2, pool3}
	for i, want := range expected {
		got := selector.Next(ctx)
		if got != want {
			t.Errorf("iteration %d: expected pool %p, got %p", i, want, got)
		}
	}
}

func TestRoundRobinSelector_EmptyPools(t *testing.T) {
	t.Parallel()

	selector := newRoundRobinSelector(nil, nil)
	ctx := context.Background()

	got := selector.Next(ctx)
	if got != nil {
		t.Errorf("expected nil for empty pools, got %p", got)
	}
}

func TestRandomSelector_Next(t *testing.T) {
	t.Parallel()

	pool1 := &pgxpool.Pool{}
	pool2 := &pgxpool.Pool{}

	pools := []*pgxpool.Pool{pool1, pool2}
	names := []string{"replica-1", "replica-2"}

	selector := newRandomSelector(pools, names)
	ctx := context.Background()

	// Test that Next returns one of the pools.
	for i := 0; i < 10; i++ {
		got := selector.Next(ctx)
		if got != pool1 && got != pool2 {
			t.Errorf("iteration %d: unexpected pool %p", i, got)
		}
	}
}

func TestRandomSelector_EmptyPools(t *testing.T) {
	t.Parallel()

	selector := newRandomSelector(nil, nil)
	ctx := context.Background()

	got := selector.Next(ctx)
	if got != nil {
		t.Errorf("expected nil for empty pools, got %p", got)
	}
}

func TestNewReplicaSelector_UnknownStrategy(t *testing.T) {
	t.Parallel()

	pool := &pgxpool.Pool{}
	pools := []*pgxpool.Pool{pool}
	names := []string{"replica-1"}

	// Use a mock logger that doesn't panic.
	selector := newReplicaSelector("unknown_strategy", pools, names, nil)

	// Should fall back to round-robin.
	if selector == nil {
		t.Fatal("expected non-nil selector for unknown strategy")
	}

	// Verify it behaves like round-robin.
	ctx := context.Background()
	got := selector.Next(ctx)
	if got != pool {
		t.Errorf("expected pool %p, got %p", pool, got)
	}
}

func TestNewReplicaSelector_NilPools(t *testing.T) {
	t.Parallel()

	selector := newReplicaSelector(ReplicaStrategyRoundRobin, nil, nil, nil)
	if selector != nil {
		t.Errorf("expected nil selector for empty pools, got %v", selector)
	}
}
