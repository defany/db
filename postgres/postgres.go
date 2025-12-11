package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"sync"

	txman "git.portals-mem.com/portals/backend/db.git/v3/tx_manager"
	"git.portals-mem.com/portals/backend/db.git/v3/postgres/cluster"
	"git.portals-mem.com/portals/backend/slogger.git/pkg/logger/sl"
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
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

type Postgres interface {
	Querier
	Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (txman.Tx, error)
	Pool(ctx context.Context) *pgxpool.Pool
	Pools() []*pgxpool.Pool
	Close()
}

type postgres struct {
	log         *slog.Logger
	cluster     cluster.ClusterQuerier
	pools       []*pgxpool.Pool // Сохраняем все пулы для совместимости с интерфейсом
	middlewares []Middleware
}

func NewPostgres(ctx context.Context, log *slog.Logger, cfg *Config) (Postgres, error) {
	var clusterInstance cluster.ClusterQuerier
	var pools []*pgxpool.Pool

	if len(cfg.DSN) > 0 {
		var nodes []*hasql.Node[cluster.WrappedPool]

		for i, dsn := range cfg.DSN {
			// Парсим DSN для получения hostname
			parsedURL, err := url.Parse(dsn)
			if err != nil {
				log.Error("Unable to parse DSN URL", sl.ErrAttr(err))
				return nil, err
			}

			hostname := parsedURL.Host
			if hostname == "" {
				hostname = fmt.Sprintf("node-%d", i) // fallback имя
			}

			// Создаем конфигурацию пула из DSN
			pgxCfg, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				log.Error("Unable to parse DSN config", sl.ErrAttr(err))
				return nil, err
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

			pgxCfg.ConnConfig.Tracer = cfg.tracer

			pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
			if err != nil {
				log.Error("failed to create pool from DSN", sl.ErrAttr(err))
				return nil, err
			}

			pools = append(pools, pool)

			// для интерфейса
			var wrapper cluster.WrappedPool = &wrappedPool{pool: pool}

			// новая нода
			node := hasql.NewNode(hostname, wrapper)
			nodes = append(nodes, node)
		}

		// Создаем статический Discoverer узлов
		nodeDiscoverer := hasql.NewStaticNodeDiscoverer(nodes...)

		var err error
		clusterInstance, err = cluster.NewCluster[cluster.WrappedPool](
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
			pools:       pools,
			middlewares: cfg.Middlewares,
		}
		return p, nil
	}

	// Старая логика - один клиент
	primary, err := NewClient(ctx, log, cfg)
	if err != nil {
		return nil, err
	}

	pools = []*pgxpool.Pool{primary}

	// Создаем обертку для пула, которая реализует WrappedPool
	var wrapper cluster.WrappedPool = &wrappedPool{pool: primary}
	nodeDiscoverer := hasql.NewStaticNodeDiscoverer(
		hasql.NewNode(cfg.Host, wrapper),
	)

	clusterInstance, err = cluster.NewCluster[cluster.WrappedPool](
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
		pools:       pools,
		middlewares: cfg.Middlewares,
	}
	return p, nil
}

func (p *postgres) apply(ctx context.Context, query string, args []any) (context.Context, string, []any, error) {
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

func (p *postgres) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
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

func (p *postgres) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
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

func (p *postgres) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
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

func (p *postgres) Pool(ctx context.Context) *pgxpool.Pool {
	wp := p.cluster.Pool(ctx)
	if wp == nil {
		return nil
	}
	return wp.Pool()
}

func (p *postgres) Pools() []*pgxpool.Pool {
	return p.pools
}

func (p *postgres) Close() {
	p.cluster.Close() // Сначала закрываем кластер
	for _, pool := range p.pools {
		if pool != nil {
			pool.Close()
		}
	}
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(_ ...any) error {
	return r.err
}

type wrappedPool struct {
	pool *pgxpool.Pool
	db   *sql.DB
	once sync.Once
}

func (w *wrappedPool) Pool() *pgxpool.Pool {
	return w.pool
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
	w.db.Close()
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

func (w *wrappedPool) OpenDB() {
	w.once.Do(func() {
		w.db = sql.OpenDB(stdlib.GetPoolConnector(w.pool))
		w.db.SetMaxIdleConns(0)
	})
}

func (w *wrappedPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	w.OpenDB()
	return w.db.QueryContext(ctx, query, args...)
}

func (w *wrappedPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	w.OpenDB()
	return w.db.QueryRowContext(ctx, query, args...)
}
