package postgres

import (
	"context"
	"fmt"
	"log/slog"

	txman "github.com/defany/db/v2/tx_manager"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres interface {
	Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row
	Exec(ctx context.Context, query string, args ...interface{}) (commandTag pgconn.CommandTag, err error)

	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (txman.Tx, error)

	Pool() *pgxpool.Pool

	Close()
}

type postgres struct {
	log *slog.Logger

	primary         *pgxpool.Pool
	selector        replicaSelector
	fallbackEnabled bool
}

func NewPostgres(ctx context.Context, log *slog.Logger, cfg *Config) (Postgres, error) {
	primary, err := NewClient(ctx, log, cfg)
	if err != nil {
		return nil, err
	}

	p := &postgres{
		log:             log,
		primary:         primary,
		fallbackEnabled: cfg.ReplicaFallbackEnabled,
	}

	if len(cfg.ReplicaConfigs) > 0 {
		pools, names := p.connectReplicas(ctx, log, cfg)
		if len(pools) > 0 {
			p.selector = newReplicaSelector(cfg.effectiveReplicaStrategy(), pools, names, log)
			log.Info("replica routing enabled",
				slog.Int("replica_count", len(pools)),
				slog.String("strategy", string(cfg.effectiveReplicaStrategy())),
				slog.Bool("fallback_enabled", cfg.ReplicaFallbackEnabled),
			)
		}
	}

	return p, nil
}

func (p *postgres) connectReplicas(ctx context.Context, log *slog.Logger, cfg *Config) ([]*pgxpool.Pool, []string) {
	pools := make([]*pgxpool.Pool, 0, len(cfg.ReplicaConfigs))
	names := make([]string, 0, len(cfg.ReplicaConfigs))

	for i, replicaCfg := range cfg.ReplicaConfigs {
		pool, err := connectReplica(ctx, log, cfg, replicaCfg)
		if err != nil {
			log.Warn("failed to connect replica, skipping",
				slog.String("name", replicaCfg.Name),
				slog.Int("index", i),
				slog.Any("error", err),
			)
			continue
		}

		name := replicaCfg.Name
		if name == "" {
			name = fmt.Sprintf("replica-%d", i)
		}

		pools = append(pools, pool)
		names = append(names, name)
	}

	return pools, names
}

func (p *postgres) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	tx, ok := txman.ExtractTX(ctx)
	if ok {
		return tx.Query(ctx, query, args...)
	}

	pool := p.selectReadPool(ctx)
	rows, err := pool.Query(ctx, query, args...)

	if err != nil && p.fallbackEnabled && pool != p.primary && p.primary != nil {
		p.log.Warn("replica query failed, retrying on primary", slog.String("error", err.Error()))
		return p.primary.Query(ctx, query, args...)
	}

	return rows, err
}

func (p *postgres) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	tx, ok := txman.ExtractTX(ctx)
	if ok {
		return tx.QueryRow(ctx, query, args...)
	}

	pool := p.selectReadPool(ctx)
	return pool.QueryRow(ctx, query, args...)
}

func (p *postgres) Exec(ctx context.Context, query string, args ...interface{}) (commandTag pgconn.CommandTag, err error) {
	tx, ok := txman.ExtractTX(ctx)
	if ok {
		return tx.Exec(ctx, query, args...)
	}

	return p.primary.Exec(ctx, query, args...)
}

func (p *postgres) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (txman.Tx, error) {
	return p.primary.BeginTx(ctx, txOptions)
}

func (p *postgres) Pool() *pgxpool.Pool {
	return p.primary
}

func (p *postgres) Close() {
	p.primary.Close()

	if p.selector != nil {
		p.selector.Close()
	}
}

func (p *postgres) selectReadPool(ctx context.Context) *pgxpool.Pool {
	if p.selector == nil {
		return p.primary
	}

	replica := p.selector.Next(ctx)
	if replica == nil {
		return p.primary
	}

	return replica
}
