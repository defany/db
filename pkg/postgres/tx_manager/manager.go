package txman

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	errs "errors"
	"fmt"

	slerr "github.com/defany/slogger/pkg/err"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	pkgerrors "github.com/pkg/errors"
)

type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Conn() *pgx.Conn
	CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	LargeObjects() pgx.LargeObjects
	Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Postgres interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (Tx, error)
}

type txKey struct{}
type txQueryKey struct{}

type TxConfig struct {
	IsoLevel pgx.TxIsoLevel
	Retry    uint
	ReadOnly bool
}

type TxOption func(*TxConfig)

func WithRetry(n uint) TxOption           { return func(c *TxConfig) { c.Retry = n } }
func WithIso(lvl pgx.TxIsoLevel) TxOption { return func(c *TxConfig) { c.IsoLevel = lvl } }
func ReadOnly(on bool) TxOption           { return func(c *TxConfig) { c.ReadOnly = on } }

type Handler = func(context.Context) error
type GenericHandler[T any] func(context.Context) (T, error)

type TxManager struct{ db Postgres }

func New(db Postgres) *TxManager { return &TxManager{db: db} }

func ReadCommitted[T any](ctx context.Context, tm *TxManager, h GenericHandler[T], opts ...TxOption) (T, error) {
	opts = append(opts, WithIso(pgx.ReadCommitted))
	return execTx(ctx, tm, h, opts...)
}

func RepeatableRead[T any](ctx context.Context, tm *TxManager, h GenericHandler[T], opts ...TxOption) (T, error) {
	opts = append(opts, WithIso(pgx.RepeatableRead))
	return execTx(ctx, tm, h, opts...)
}

func Serializable[T any](ctx context.Context, tm *TxManager, h GenericHandler[T], opts ...TxOption) (T, error) {
	opts = append(opts, WithIso(pgx.Serializable))
	return execTx(ctx, tm, h, opts...)
}

func (tm *TxManager) ReadCommitted(ctx context.Context, h Handler, opts ...TxOption) error {
	opts = append(opts, WithIso(pgx.ReadCommitted))

	cfg := TxConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	return tm.run(ctx, cfg, h)
}

func (tm *TxManager) RepeatableRead(ctx context.Context, h Handler, opts ...TxOption) error {
	opts = append(opts, WithIso(pgx.RepeatableRead))

	cfg := TxConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	return tm.run(ctx, cfg, h)
}

func (tm *TxManager) Serializable(ctx context.Context, h Handler, opts ...TxOption) error {
	opts = append(opts, WithIso(pgx.Serializable))

	cfg := TxConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	return tm.run(ctx, cfg, h)
}

func execTx[T any](ctx context.Context, tm *TxManager, handler GenericHandler[T], opts ...TxOption) (out T, err error) {
	cfg := TxConfig{IsoLevel: pgx.ReadCommitted}
	for _, opt := range opts {
		opt(&cfg)
	}

	err = tm.run(ctx, cfg, func(txCtx context.Context) error {
		out, err = handler(txCtx)
		return err
	})

	return
}

func (tm *TxManager) run(ctx context.Context, cfg TxConfig, h Handler) error {
	for attempt := uint(0); attempt <= cfg.Retry; attempt++ {
		err := tm.execTx(ctx, cfg, h)
		if err == nil {
			return nil
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
			continue
		}

		return slerr.WithSource(err)
	}

	return fmt.Errorf("retries exceeded")
}

func (tm *TxManager) execTx(ctx context.Context, cfg TxConfig, h Handler) (err error) {
	if _, ok := ExtractTX(ctx); ok {
		return h(ctx)
	}

	opts := pgx.TxOptions{IsoLevel: cfg.IsoLevel, AccessMode: chooseAccessMode(cfg.ReadOnly)}

	tx, err := tm.db.BeginTx(ctx, opts)
	if err != nil {
		return pkgerrors.Wrap(err, "begin tx")

	}

	ctx = InjectTX(ctx, tx)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered: %w -> %v", err, r)
		}
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				err = errs.Join(rbErr, err)
			}
			return
		}
		err = tx.Commit(ctx)
	}()

	return h(ctx)
}

func InjectTX(ctx context.Context, tx Tx) context.Context {
	ctx = context.WithValue(ctx, txKey{}, tx)
	return context.WithValue(ctx, txQueryKey{}, generateShortID())
}

func ExtractTX(ctx context.Context) (Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(Tx)
	return tx, ok
}

func ExtractTxQueryKey(ctx context.Context) (string, bool) {
	k, ok := ctx.Value(txQueryKey{}).(string)
	return k, ok
}

func chooseAccessMode(ro bool) pgx.TxAccessMode {
	if ro {
		return pgx.ReadOnly
	}
	return pgx.ReadWrite
}

func generateShortID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "errid"
	}
	return hex.EncodeToString(b[:])
}
