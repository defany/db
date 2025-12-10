package cluster

import (
	"context"
	"database/sql"
	"reflect"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"golang.yandex/hasql/v2"
)

func newSQLRowError() *sql.Row {
	row := &sql.Row{}
	t := reflect.TypeOf(row).Elem()
	field, _ := t.FieldByName("err")
	rowPtr := unsafe.Pointer(row)
	errFieldPtr := unsafe.Pointer(uintptr(rowPtr) + field.Offset)
	errPtr := (*error)(errFieldPtr)
	*errPtr = ErrorNoAliveNodes
	return row
}

type Querier interface {
	Acquire(ctx context.Context) (c *pgxpool.Conn, err error)
	AcquireAllIdle(ctx context.Context) []*pgxpool.Conn
	AcquireFunc(ctx context.Context, f func(*pgxpool.Conn) error) error
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Close()
	Config() *pgxpool.Config
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Reset()
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Stat() *pgxpool.Stat
}

type WrappedPool interface {
	Querier
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type Wrapper struct {
	pool *pgxpool.Pool
}

func (w *Wrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db := sql.OpenDB(stdlib.GetPoolConnector(w.pool))
	db.SetMaxIdleConns(0)
	return db.QueryContext(ctx, query, args...)
}

func (w *Wrapper) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	db := sql.OpenDB(stdlib.GetPoolConnector(w.pool))
	db.SetMaxIdleConns(0)
	return db.QueryRowContext(ctx, query, args...)
}

type ClusterQuerier interface {
	Querier

	WaitForNodes(ctx context.Context, criterion ...hasql.NodeStateCriterion) error
	Init(opts ...ClusterOption) error
}

type Cluster struct {
	hasql   *hasql.Cluster[WrappedPool]
	options ClusterOptions
}

func (c *Cluster) Init(opts ...ClusterOption) error {
	options := NewClusterOptions(opts...)

	if options.NodePicker != nil {
		c.options.Options = append(c.options.Options, hasql.WithNodePicker(options.NodePicker))
		if p, ok := options.NodePicker.(*CustomPicker[WrappedPool]); ok {
			p.opts.Priority = options.NodePriority
			p.opts.NodeRolePriority = options.NodeRolePriority
		}
	}

	if options.NodeStateCriterion != c.options.NodeStateCriterion {
		c.options.NodeStateCriterion = options.NodeStateCriterion
	}

	if options.NodeChecker != nil ||
		options.NodeDiscoverer != nil {
		nc, err := hasql.NewCluster(
			options.NodeDiscoverer,
			options.NodeChecker,
			options.Options...,
		)
		if err != nil {
			return err
		}
		c.hasql = nc
	}

	return nil
}

// NewCluster returns [Querier] that provides cluster of nodes.
func NewCluster[T Querier](opts ...ClusterOption) (ClusterQuerier, error) {
	options := NewClusterOptions(opts...)

	if options.NodeChecker == nil {
		return nil, ErrClusterChecker
	}
	if options.NodeDiscoverer == nil {
		return nil, ErrClusterDiscoverer
	}
	if options.NodePicker == nil {
		return nil, ErrClusterPicker
	}

	options.Options = append(options.Options, hasql.WithNodePicker(options.NodePicker))
	if p, ok := options.NodePicker.(*CustomPicker[WrappedPool]); ok {
		p.opts.Priority = options.NodePriority
		p.opts.NodeRolePriority = options.NodeRolePriority
	}

	/*
		hasqlTracer := hasql.Tracer[Querier]{
			NodeDead: func(err error) {
				Querier.SetMaxOpenConns(0),
			},
			NodeAlive: func(node hasql.CheckedNode[Querier]) {},
		}

		options.Options = append(options.Options, hasql.WithTracer(hasqlTracer))
	*/

	c, err := hasql.NewCluster(
		options.NodeDiscoverer,
		options.NodeChecker,
		options.Options...,
	)
	if err != nil {
		return nil, err
	}

	return &Cluster{hasql: c, options: options}, nil
}

func (c *Cluster) Close() {
	c.hasql.Close()
}

func (c *Cluster) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	var conn *pgxpool.Conn
	var ok bool
	var err error

	retries := -1
	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		ok = true
		conn, err = n.DB().Acquire(ctx)
		retries++
		return c.options.RetryFunc(ctx, retries, err)
	})

	if !ok {
		err = ErrorNoAliveNodes
	}

	return conn, err
}

func (c *Cluster) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
	var conns []*pgxpool.Conn

	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		conns = n.DB().AcquireAllIdle(ctx)
		return false
	})

	return conns
}

func (c *Cluster) AcquireFunc(ctx context.Context, f func(*pgxpool.Conn) error) error {
	var err error

	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		err = n.DB().AcquireFunc(ctx, f)
		return false
	})

	return err
}

func (c *Cluster) Begin(ctx context.Context) (pgx.Tx, error) {
	var tx pgx.Tx
	var ok bool
	var err error

	retries := -1
	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		ok = true
		tx, err = n.DB().Begin(ctx)
		retries++
		return c.options.RetryFunc(ctx, retries, err)
	})

	if !ok {
		err = ErrorNoAliveNodes
	}

	return tx, err
}

func (c *Cluster) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	var tx pgx.Tx
	var ok bool
	var err error

	retries := -1
	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		ok = true
		tx, err = n.DB().BeginTx(ctx, txOptions)
		retries++
		return c.options.RetryFunc(ctx, retries, err)
	})

	if !ok {
		err = ErrorNoAliveNodes
	}

	return tx, err
}

func (c *Cluster) Config() *pgxpool.Config {
	var cfg *pgxpool.Config

	c.hasql.NodesIter(c.options.NodeStateCriterion)(func(n *hasql.Node[WrappedPool]) bool {
		cfg = n.DB().Config()
		return false
	})

	return cfg
}

func (c *Cluster) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	var num int64
	var err error
	var ok bool

	retries := -1
	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		ok = true
		num, err = n.DB().CopyFrom(ctx, tableName, columnNames, rowSrc)
		retries++
		return c.options.RetryFunc(ctx, retries, err)
	})

	if !ok {
		err = ErrorNoAliveNodes
	}

	return num, err
}

func (c *Cluster) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	var cmd pgconn.CommandTag
	var err error
	var ok bool

	retries := -1
	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		ok = true
		cmd, err = n.DB().Exec(ctx, sql, arguments...)
		retries++
		return c.options.RetryFunc(ctx, retries, err)
	})

	if !ok {
		err = ErrorNoAliveNodes
	}

	return cmd, err
}

func (c *Cluster) Ping(ctx context.Context) error {
	var err error
	var ok bool

	retries := -1
	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		ok = true
		err = n.DB().Ping(ctx)
		retries++
		return c.options.RetryFunc(ctx, retries, err)
	})

	if !ok {
		err = ErrorNoAliveNodes
	}

	return err
}

func (c *Cluster) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	var err error
	var ok bool
	var rows pgx.Rows

	retries := -1
	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		ok = true
		rows, err = n.DB().Query(ctx, sql, args...)
		retries++

		return c.options.RetryFunc(ctx, retries, err)
	})

	if !ok {
		err = ErrorNoAliveNodes
	}

	return rows, err
}

func (c *Cluster) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	var row pgx.Row

	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		row = n.DB().QueryRow(ctx, sql, args...)

		return false
	})

	return row
}

func (c *Cluster) Reset() {
	c.hasql.NodesIter(c.options.NodeStateCriterion)(func(n *hasql.Node[WrappedPool]) bool {
		n.DB().Reset()

		return false
	})
}

func (c *Cluster) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	var res pgx.BatchResults

	c.hasql.NodesIter(c.getNodeStateCriterion(ctx))(func(n *hasql.Node[WrappedPool]) bool {
		res = n.DB().SendBatch(ctx, b)

		return false
	})

	return res
}

func (c *Cluster) Stat() *pgxpool.Stat {
	var st *pgxpool.Stat

	c.hasql.NodesIter(c.options.NodeStateCriterion)(func(n *hasql.Node[WrappedPool]) bool {
		st = n.DB().Stat()

		return false
	})

	return st
}

func (c *Cluster) WaitForNodes(ctx context.Context, criterions ...hasql.NodeStateCriterion) error {
	var err error
	for _, criterion := range criterions {
		if _, err = c.hasql.WaitForNode(ctx, criterion); err != nil {
			return err
		}
	}
	return nil
}
