package queue

import (
	"context"

	"github.com/defany/db/pkg/postgres"
	slerr "github.com/defany/slogger/pkg/err"
	"github.com/defany/slogger/pkg/logger/sl"
	"github.com/gookit/goutil/arrutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

type Worker[T river.JobArgs] interface {
	Put(ctx context.Context, args T) (int64, error)
	PutBatch(ctx context.Context, args ...T) ([]int64, error)
	JobStatuses(ctx context.Context, ids ...int64) ([]JobStatus, error)
}

type Repository[T river.JobArgs] struct {
	river *river.Client[pgx.Tx]
	db    *pgxpool.Pool
}

func New[T river.JobArgs](river *river.Client[pgx.Tx], db *pgxpool.Pool) *Repository[T] {
	return &Repository[T]{river: river, db: db}
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

func (r *Repository[T]) JobStatuses(ctx context.Context, ids ...int64) ([]JobStatus, error) {
	q := `select id, state from river_job where id = any($1)`

	rows, err := r.db.Query(ctx, q, ids)
	if err != nil {
		return nil, slerr.WithSource(err)
	}

	type job struct {
		ID    int64              `db:"id"`
		State rivertype.JobState `db:"state"`
	}

	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[job])
	if err != nil {
		return nil, slerr.WithSource(err)
	}

	statuses := arrutil.Map(jobs, func(input job) (target JobStatus, find bool) {
		return JobStatus{
			JobID:  input.ID,
			Status: input.State,
		}, true
	})

	return statuses, nil
}
