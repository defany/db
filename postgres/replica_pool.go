package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ReplicaPool struct {
	log      *slog.Logger
	pools    []*pgxpool.Pool
	strategy ReplicaStrategy
	counter  atomic.Uint64
}

func NewReplicaPool(ctx context.Context, log *slog.Logger, configs []*ReplicaConfig, strategy ReplicaStrategy) (*ReplicaPool, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("at least one replica config required")
	}

	pools := make([]*pgxpool.Pool, 0, len(configs))

	for i, cfg := range configs {
		if cfg == nil || cfg.DSN == "" {
			log.Warn("skipping invalid replica config", slog.Int("index", i))
			continue
		}

		pgxCfg, err := pgxpool.ParseConfig(cfg.DSN)
		if err != nil {
			log.Warn("failed to parse replica DSN, skipping",
				slog.String("name", cfg.Name),
				slog.Any("error", err),
			)
			continue
		}

		if cfg.ConnAmount != nil {
			pgxCfg.MaxConns = *cfg.ConnAmount
		}
		if cfg.MinConnAmount != nil {
			pgxCfg.MinConns = *cfg.MinConnAmount
		}
		if cfg.MaxConnIdleTime != nil {
			pgxCfg.MaxConnIdleTime = *cfg.MaxConnIdleTime
		}
		if cfg.MaxConnLifetime != nil {
			pgxCfg.MaxConnLifetime = *cfg.MaxConnLifetime
		}
		if cfg.HealthCheckPeriod != nil {
			pgxCfg.HealthCheckPeriod = *cfg.HealthCheckPeriod
		}

		pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
		if err != nil {
			log.Warn("failed to connect replica, skipping",
				slog.String("name", cfg.Name),
				slog.Any("error", err),
			)
			continue
		}

		if err = pool.Ping(ctx); err != nil {
			log.Warn("replica ping failed, skipping",
				slog.String("name", cfg.Name),
				slog.Any("error", err),
			)
			pool.Close()
			continue
		}

		pools = append(pools, pool)
		log.Info("replica connected", slog.String("name", cfg.Name))
	}

	if len(pools) == 0 {
		return nil, fmt.Errorf("failed to connect to any replica")
	}

	if strategy == "" {
		strategy = ReplicaStrategyRoundRobin
	}

	return &ReplicaPool{
		log:      log,
		pools:    pools,
		strategy: strategy,
	}, nil
}

func (r *ReplicaPool) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	pool := r.pickPool(ctx)
	return pool.Query(ctx, query, args...)
}

func (r *ReplicaPool) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	pool := r.pickPool(ctx)
	return pool.QueryRow(ctx, query, args...)
}

func (r *ReplicaPool) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	pool := r.pickPool(ctx)
	return pool.Exec(ctx, query, args...)
}

func (r *ReplicaPool) pickPool(_ context.Context) *pgxpool.Pool {
	switch r.strategy {
	case ReplicaStrategyRandom:
		idx := rand.IntN(len(r.pools))
		return r.pools[idx]
	case ReplicaStrategyRoundRobin, "":
		idx := r.counter.Add(1) - 1
		return r.pools[idx%uint64(len(r.pools))]
	default:
		idx := r.counter.Add(1) - 1
		return r.pools[idx%uint64(len(r.pools))]
	}
}

func (r *ReplicaPool) Close() {
	for _, pool := range r.pools {
		pool.Close()
	}
}

