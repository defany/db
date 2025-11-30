package postgres

import (
	"context"
	"log/slog"

	txman "github.com/defany/db/v2/tx_manager"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DBRequest struct {
	Ctx   context.Context
	Query string
	Args  []interface{}
}

type Middleware func(ctx context.Context, req DBRequest) (context.Context, DBRequest, error)

type Postgres interface {
	Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row
	Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (txman.Tx, error)
	Pool() *pgxpool.Pool
	ReplicaPools() []*pgxpool.Pool
	WithReplicaPool(replicaPool *ReplicaPool) Postgres
	Close()
}

type postgres struct {
	log             *slog.Logger
	primary         *pgxpool.Pool
	replicaPool     *ReplicaPool
	fallbackEnabled bool
	middlewares     []Middleware
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
		middlewares:     cfg.Middlewares,
	}

	if len(cfg.ReplicaConfigs) > 0 {
		replicaPool, err := NewReplicaPool(ctx, log, cfg.ReplicaConfigs, cfg.effectiveReplicaStrategy())
		if err == nil {
			p.replicaPool = replicaPool
			log.Info("replica pool initialized",
				slog.Int("replica_count", len(replicaPool.pools)),
				slog.String("strategy", string(cfg.effectiveReplicaStrategy())),
				slog.Bool("fallback_enabled", cfg.ReplicaFallbackEnabled),
			)
		} else {
			log.Warn("failed to initialize replica pool, using primary only", slog.Any("error", err))
		}
	}

	return p, nil
}

func (p *postgres) WithReplicaPool(replicaPool *ReplicaPool) Postgres {
	p.replicaPool = replicaPool
	return p
}

func (p *postgres) apply(ctx context.Context, query string, args []interface{}) (context.Context, string, []interface{}, error) {
	if len(p.middlewares) == 0 {
		return ctx, query, args, nil
	}

	req := DBRequest{
		Ctx:   ctx,
		Query: query,
		Args:  args,
	}

	var err error
	for _, mw := range p.middlewares {
		ctx, req, err = mw(ctx, req)
		if err != nil {
			return ctx, "", nil, err
		}
	}

	return ctx, req.Query, req.Args, nil
}

func (p *postgres) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	if len(p.middlewares) > 0 {
		var err error
		ctx, query, args, err = p.apply(ctx, query, args)
		if err != nil {
			return nil, err
		}
	}

	tx, ok := txman.ExtractTX(ctx)
	if ok {
		return tx.Query(ctx, query, args...)
	}

	if p.replicaPool == nil {
		return p.primary.Query(ctx, query, args...)
	}

	rows, err := p.replicaPool.Query(ctx, query, args...)
	if err == nil {
		return rows, nil
	}

	if !p.fallbackEnabled {
		return nil, err
	}

	p.log.Warn("replica query failed, falling back to primary", slog.String("error", err.Error()))
	return p.primary.Query(ctx, query, args...)
}

func (p *postgres) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	if len(p.middlewares) > 0 {
		var err error
		ctx, query, args, err = p.apply(ctx, query, args)
		if err != nil {
			return errorRow{err: err}
		}
	}

	if tx, ok := txman.ExtractTX(ctx); ok {
		return tx.QueryRow(ctx, query, args...)
	}

	if p.replicaPool == nil {
		return p.primary.QueryRow(ctx, query, args...)
	}

	return p.replicaPool.QueryRow(ctx, query, args...)
}

func (p *postgres) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	if len(p.middlewares) > 0 {
		var err error
		ctx, query, args, err = p.apply(ctx, query, args)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
	}

	if tx, ok := txman.ExtractTX(ctx); ok {
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

func (p *postgres) ReplicaPools() []*pgxpool.Pool {
	out := make([]*pgxpool.Pool, len(p.replicaPool.pools))
	copy(out, p.replicaPool.pools)

	return out
}

func (p *postgres) Close() {
	p.primary.Close()

	if p.replicaPool != nil {
		p.replicaPool.Close()
	}
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(_ ...interface{}) error {
	return r.err
}
