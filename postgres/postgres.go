package postgres

import (
	"context"
	"database/sql"
	"log/slog"

	txman "github.com/defany/db/v2/tx_manager"
	"github.com/defany/db/v3/postgres/cluster"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"golang.yandex/hasql/v2"
)

type DBRequest struct {
	Ctx   context.Context
	Query string
	Args  []interface{}
}

type Middleware func(ctx context.Context, req DBRequest) (context.Context, DBRequest, error)

type Querier interface {
	Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row
}

type Postgres interface {
	Querier
	Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (txman.Tx, error)
	Pool() *pgxpool.Pool
	PoolContext(ctx context.Context) *pgxpool.Pool
	Close()
}

type postgres struct {
	log         *slog.Logger
	cluster     cluster.ClusterQuerier
	primary     *pgxpool.Pool
	middlewares []Middleware
}

func NewPostgres(ctx context.Context, log *slog.Logger, cfg *Config) (Postgres, error) {
	primary, err := NewClient(ctx, log, cfg)
	if err != nil {
		return nil, err
	}

	// Создаем обертку для пула, которая реализует WrappedPool
	var wrapper cluster.WrappedPool = &wrappedPool{pool: primary}
	nodeDiscoverer := hasql.NewStaticNodeDiscoverer(
		hasql.NewNode("primary", wrapper),
	)

	clusterInstance, err := cluster.NewCluster[cluster.WrappedPool](
		cluster.WithClusterNodeChecker(hasql.PostgreSQLChecker),
		cluster.WithClusterNodePicker(cluster.NewCustomPicker[cluster.WrappedPool]()),
		cluster.WithClusterNodeDiscoverer(nodeDiscoverer),
		cluster.WithClusterContext(ctx),
	)
	if err != nil {
		return nil, err
	}

	err = clusterInstance.Init()
	if err != nil {
		return nil, err
	}

	p := &postgres{
		log:         log,
		cluster:     clusterInstance,
		primary:     primary,
		middlewares: cfg.Middlewares,
	}

	return p, nil
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

	return p.cluster.Query(ctx, query, args...)
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

	return p.cluster.QueryRow(ctx, query, args...)
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

	return p.cluster.Exec(ctx, query, args...)
}

func (p *postgres) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (txman.Tx, error) {
	return p.cluster.BeginTx(ctx, txOptions)
}

func (p *postgres) Pool() *pgxpool.Pool {
	// Для совместимости с интерфейсом возвращаем оригинальный primary пул
	// Так как кластер может использовать разные узлы, возвращаем основной пул
	return p.primary
}

func (p *postgres) PoolContext(ctx context.Context) *pgxpool.Pool {
	return p.primary
}

func (p *postgres) Close() {
	p.primary.Close()
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(_ ...interface{}) error {
	return r.err
}

type wrappedPool struct {
	pool *pgxpool.Pool
}

func (w *wrappedPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return w.pool.Acquire(ctx)
}

func (w *wrappedPool) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
	return w.pool.AcquireAllIdle(ctx)
}

func (w *wrappedPool) AcquireFunc(ctx context.Context, f func(*pgxpool.Conn) error) error {
	return w.pool.AcquireFunc(ctx, f)
}

func (w *wrappedPool) Begin(ctx context.Context) (pgx.Tx, error) {
	return w.pool.Begin(ctx)
}

func (w *wrappedPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return w.pool.BeginTx(ctx, txOptions)
}

func (w *wrappedPool) Close() {
	w.pool.Close()
}

func (w *wrappedPool) Config() *pgxpool.Config {
	return w.pool.Config()
}

func (w *wrappedPool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return w.pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (w *wrappedPool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return w.pool.Exec(ctx, sql, arguments...)
}

func (w *wrappedPool) Ping(ctx context.Context) error {
	return w.pool.Ping(ctx)
}

func (w *wrappedPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return w.pool.Query(ctx, sql, args...)
}

func (w *wrappedPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return w.pool.QueryRow(ctx, sql, args...)
}

func (w *wrappedPool) Reset() {
	w.pool.Reset()
}

func (w *wrappedPool) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return w.pool.SendBatch(ctx, b)
}

func (w *wrappedPool) Stat() *pgxpool.Stat {
	return w.pool.Stat()
}

func (w *wrappedPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db := sql.OpenDB(stdlib.GetPoolConnector(w.pool))
	db.SetMaxIdleConns(0)
	return db.QueryContext(ctx, query, args...)
}

func (w *wrappedPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	db := sql.OpenDB(stdlib.GetPoolConnector(w.pool))
	db.SetMaxIdleConns(0)
	return db.QueryRowContext(ctx, query, args...)
}
