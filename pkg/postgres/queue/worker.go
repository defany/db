package queue

import (
	"context"

	"github.com/defany/db/pkg/postgres"
	"github.com/defany/slogger/pkg/logger/sl"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
)

type Worker[T river.JobArgs] interface {
	Put(ctx context.Context, args T) error
	PutBatch(ctx context.Context, args ...T) error
}

type Repository[T river.JobArgs] struct {
	river *river.Client[pgx.Tx]
}

func New[T river.JobArgs](river *river.Client[pgx.Tx]) *Repository[T] {
	return &Repository[T]{river: river}
}

func (r *Repository[T]) Put(ctx context.Context, args T) error {
	tx, ok := postgres.ExtractTX(ctx)
	if ok {
		_, err := r.river.InsertTx(ctx, tx, args, nil)
		if err != nil {
			return err
		}

		return nil
	}

	_, err := r.river.Insert(ctx, args, nil)
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository[T]) PutBatch(ctx context.Context, args ...T) error {
	op := sl.FnName()

	insertParams := make([]river.InsertManyParams, 0, len(args))
	for _, arg := range args {
		insertParams = append(insertParams, river.InsertManyParams{
			Args: arg,
		})
	}

	tx, ok := postgres.ExtractTX(ctx)
	if ok {
		_, err := r.river.InsertManyFastTx(ctx, tx, insertParams)
		if err != nil {
			return sl.Err(op, err)
		}

		return nil
	}

	_, err := r.river.InsertManyFast(ctx, insertParams)
	if err != nil {
		return sl.Err(op, err)
	}

	return nil
}
