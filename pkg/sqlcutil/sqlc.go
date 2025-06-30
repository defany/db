// Package postgres предоставляет обёртку над pgx с поддержкой
// транзакций, ретраев и дженериков-хендлеров без методов-дженериков.
//
// Требует Go ≥ 1.18.
package postgres

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

/*
   ─────────────────────────────────────────────────────────────────────────────
   1.  БАЗОВЫЕ ИНТЕРФЕЙСЫ
   ─────────────────────────────────────────────────────────────────────────────
*/

// Tx совпадает с тем, что возвращает `pgx.BeginTx`.
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Conn() *pgx.Conn
	// Остальные вспомогательные методы ↓
	CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	LargeObjects() pgx.LargeObjects
	Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Postgres — минимальный интерфейс от вашего драйвера/пула.
type Postgres interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (Tx, error)
}

/*
   ─────────────────────────────────────────────────────────────────────────────
   2.  СВЯЗКА CONTEXT <-> ТРАНЗАКЦИЯ
   ─────────────────────────────────────────────────────────────────────────────
*/

type txKey struct{}
type txQueryKey struct{}

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

func generateShortID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "errid"
	}
	return hex.EncodeToString(b[:])
}

/*
   ─────────────────────────────────────────────────────────────────────────────
   3.  КОНФИГ И ОПЦИИ (functional options)
   ─────────────────────────────────────────────────────────────────────────────
*/

type TxConfig struct {
	IsoLevel pgx.TxIsoLevel
	Retry    uint
	ReadOnly bool
}

type TxOption func(*TxConfig)

func WithRetry(n uint) TxOption           { return func(c *TxConfig) { c.Retry = n } }
func WithIso(lvl pgx.TxIsoLevel) TxOption { return func(c *TxConfig) { c.IsoLevel = lvl } }
func ReadOnly(on bool) TxOption           { return func(c *TxConfig) { c.ReadOnly = on } }

/*
   ─────────────────────────────────────────────────────────────────────────────
   4.  ОБЩИЕ ТИПЫ ХЕНДЛЕРОВ
   ─────────────────────────────────────────────────────────────────────────────
*/

type Handler = func(context.Context) error
type GenericHandler[T any] = func(context.Context) (T, error)

/*
   ─────────────────────────────────────────────────────────────────────────────
   5.  МЕНЕДЖЕР ТРАНЗАКЦИЙ
   ─────────────────────────────────────────────────────────────────────────────
*/

type txManager struct{ db Postgres }

func NewTxManager(db Postgres) *txManager { return &txManager{db: db} }

/*
   ─────────────────────────────────────────────────────────────────────────────
   6.  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ С ДЖЕНЕРИКАМИ
   ─────────────────────────────────────────────────────────────────────────────
*/

// ExecT — общий движок: оборачивает handler, возвращая T + error.
func ExecT[T any](
	ctx context.Context,
	tm *txManager,
	handler GenericHandler[T],
	opts ...TxOption,
) (out T, err error) {

	cfg := TxConfig{
		IsoLevel: pgx.ReadCommitted,
		Retry:    0,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	err = tm.run(ctx, cfg, func(txCtx context.Context) error {
		out, err = handler(txCtx)
		return err
	})
	return
}

// Шорткаты с предустановленным уровнем изоляции.
func ReadCommittedT[T any](ctx context.Context, tm *txManager,
	h GenericHandler[T], opts ...TxOption) (T, error) {
	return ExecT(ctx, tm, h, append([]TxOption{WithIso(pgx.ReadCommitted)}, opts...)...)
}

func RepeatableReadT[T any](ctx context.Context, tm *txManager,
	h GenericHandler[T], opts ...TxOption) (T, error) {
	return ExecT(ctx, tm, h, append([]TxOption{WithIso(pgx.RepeatableRead)}, opts...)...)
}

func SerializableT[T any](ctx context.Context, tm *txManager,
	h GenericHandler[T], opts ...TxOption) (T, error) {
	return ExecT(ctx, tm, h, append([]TxOption{WithIso(pgx.Serializable)}, opts...)...)
}

/*
   ─────────────────────────────────────────────────────────────────────────────
   7.  ВНУТРЕННЯЯ ЛОГИКА tm.run  + doTx
   ─────────────────────────────────────────────────────────────────────────────
*/

func (t *txManager) run(ctx context.Context, cfg TxConfig, h Handler) error {
	for attempt := uint(0); attempt <= cfg.Retry; attempt++ {
		err := t.doTx(ctx, cfg, h)
		if err == nil {
			return nil
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
			continue // serialization failure / deadlock → повтор
		}
		return slerr.WithSource(err)
	}
	return fmt.Errorf("retries exceeded")
}

func (t *txManager) doTx(ctx context.Context, cfg TxConfig, h Handler) (err error) {
	// вложенные транзакции
	if _, ok := ExtractTX(ctx); ok {
		return h(ctx)
	}

	opts := pgx.TxOptions{
		IsoLevel:   cfg.IsoLevel,
		AccessMode: chooseAccessMode(cfg.ReadOnly),
	}

	tx, err := t.db.BeginTx(ctx, opts)
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

func chooseAccessMode(ro bool) pgx.TxAccessMode {
	if ro {
		return pgx.ReadOnly
	}
	return pgx.ReadWrite
}
