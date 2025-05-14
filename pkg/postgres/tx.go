package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	errs "errors"
	"fmt"

	slerr "github.com/defany/slogger/pkg/err"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
)

type Tx interface {
	Begin(ctx context.Context) (pgx.Tx, error)

	Commit(ctx context.Context) error

	Rollback(ctx context.Context) error

	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	LargeObjects() pgx.LargeObjects

	Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error)

	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row

	Conn() *pgx.Conn
}

type txKey struct{}
type txQueryKey struct{}

type TxManager interface {
	ReadCommitted(ctx context.Context, handler Handler) error
	RepeatableRead(ctx context.Context, retryAmount uint, handler Handler) error
	Serializable(ctx context.Context, retryAmount uint, handler Handler) error
}

type Handler func(ctx context.Context) error

type txManager struct {
	db Postgres
}

func NewTxManager(db Postgres) TxManager {
	return &txManager{
		db: db,
	}
}

func (t *txManager) tx(ctx context.Context, opts pgx.TxOptions, handler Handler) (err error) {
	tx, ok := ExtractTX(ctx)
	if ok {
		return handler(ctx)
	}

	tx, err = t.db.BeginTx(ctx, opts)
	if err != nil {
		return errors.Wrap(err, "failed to start tx")
	}

	ctx = InjectTX(ctx, tx)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered: %w -> %v", err, r)
		}

		if err != nil {
			if txErr := tx.Rollback(ctx); txErr != nil {
				err = errs.Join(txErr, err)
			}

			return
		}

		if txErr := tx.Commit(ctx); txErr != nil {
			err = txErr
		}
	}()

	if err := handler(ctx); err != nil {
		return err
	}

	return nil
}

func (t *txManager) ReadCommitted(ctx context.Context, handler Handler) error {
	opts := pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	}

	return t.tx(ctx, opts, handler)
}

// RepeatableRead
// retry amount is an amount of times when handler will be executed again due to concurrent access error
func (t *txManager) RepeatableRead(ctx context.Context, retryAmount uint, handler Handler) error {
	opts := pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	}

	var err error
	for attempt := uint(0); attempt <= retryAmount; attempt++ {
		err = t.tx(ctx, opts, handler)
		if err == nil {
			return nil
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
			continue
		}

		return slerr.WithSource(err)
	}

	return slerr.WithSource(err)
}

func (t *txManager) Serializable(ctx context.Context, retryAmount uint, handler Handler) error {
	opts := pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	}

	var err error
	for attempt := uint(0); attempt <= retryAmount; attempt++ {
		err = t.tx(ctx, opts, handler)
		if err == nil {
			return nil
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
			continue
		}

		return slerr.WithSource(err)
	}

	return slerr.WithSource(err)
}

func InjectTX(ctx context.Context, tx Tx) context.Context {
	ctx = context.WithValue(ctx, txKey{}, tx)

	ctx = context.WithValue(ctx, txQueryKey{}, generateShortID())

	return ctx
}

func ExtractTX(ctx context.Context) (Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(Tx)

	return tx, ok
}

func ExtractTxQueryKey(ctx context.Context) (string, bool) {
	key, ok := ctx.Value(txQueryKey{}).(string)

	return key, ok
}

func generateShortID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "errid"
	}
	return hex.EncodeToString(b[:])
}
