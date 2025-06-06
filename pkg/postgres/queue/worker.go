package queue

import (
	"context"

	"github.com/defany/db/pkg/postgres"
	"github.com/defany/slogger/pkg/logger/sl"
	"github.com/gookit/goutil/arrutil"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

type Worker[T river.JobArgs] interface {
	Put(ctx context.Context, args T) (int64, error)
	PutBatch(ctx context.Context, args ...T) ([]int64, error)
}

type Repository[T river.JobArgs] struct {
	river *river.Client[pgx.Tx]
}

func New[T river.JobArgs](river *river.Client[pgx.Tx]) *Repository[T] {
	return &Repository[T]{river: river}
}

func (r *Repository[T]) Put(ctx context.Context, args T) (int64, error) {
	tx, ok := postgres.ExtractTX(ctx)
	if ok {
		out, err := r.river.InsertTx(ctx, tx, args, nil)
		if err != nil {
			return 0, err
		}

		return out.Job.ID, nil
	}

	out, err := r.river.Insert(ctx, args, nil)
	if err != nil {
		return 0, err
	}

	return out.Job.ID, nil
}

func (r *Repository[T]) PutBatch(ctx context.Context, args ...T) ([]int64, error) {
	op := sl.FnName()

	insertParams := make([]river.InsertManyParams, 0, len(args))
	for _, arg := range args {
		insertParams = append(insertParams, river.InsertManyParams{
			Args: arg,
		})
	}

	tx, ok := postgres.ExtractTX(ctx)
	if ok {
		out, err := r.river.InsertManyTx(ctx, tx, insertParams)
		if err != nil {
			return nil, sl.Err(op, err)
		}

		jobIds := arrutil.Map(out, func(input *rivertype.JobInsertResult) (target int64, find bool) {
			return input.Job.ID, true
		})

		return jobIds, nil
	}

	out, err := r.river.InsertMany(ctx, insertParams)
	if err != nil {
		return nil, sl.Err(op, err)
	}

	jobIds := arrutil.Map(out, func(input *rivertype.JobInsertResult) (target int64, find bool) {
		return input.Job.ID, true
	})

	return jobIds, nil
}
