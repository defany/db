package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/defany/db/pkg/postgres"
	slerr "github.com/defany/slogger/pkg/err"
	"github.com/defany/slogger/pkg/logger/sl"
	"github.com/gookit/goutil/arrutil"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

var (
	ErrJobIdsNotProvided = fmt.Errorf("job ids not provided")
	ErrJobNotFound       = fmt.Errorf("job not found")
)

type Options struct {
	ScheduledAt time.Time
	MaxAttempts int
	Metadata    []byte
	Pending     bool
	Priority    int
	Queue       string
	Tags        []string
	UniqueOpts  river.UniqueOpts
}

type Worker[T river.JobArgs] interface {
	Put(ctx context.Context, args T) (int64, error)
	PutBatch(ctx context.Context, args ...T) ([]int64, error)
	PutWithOpts(ctx context.Context, options Options, args T) (int64, error)
	PutBatchWithOpts(ctx context.Context, options Options, args ...T) ([]int64, error)
	JobStatuses(ctx context.Context, ids ...int64) ([]JobStatus, error)
	FetchJobs(ctx context.Context, ids ...int64) ([]*rivertype.JobRow, error)
	FetchJob(ctx context.Context, id int64) (*rivertype.JobRow, error)
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

func (r *Repository[T]) PutWithOpts(ctx context.Context, options Options, args T) (int64, error) {
	tx, ok := postgres.ExtractTX(ctx)
	if ok {
		out, err := r.river.InsertTx(ctx, tx, args, &river.InsertOpts{
			ScheduledAt: options.ScheduledAt,
		})
		if err != nil {
			return 0, err
		}

		return out.Job.ID, nil
	}

	out, err := r.river.Insert(ctx, args, &river.InsertOpts{
		ScheduledAt: options.ScheduledAt,
	})
	if err != nil {
		return 0, err
	}

	return out.Job.ID, nil
}

func (r *Repository[T]) PutBatchWithOpts(ctx context.Context, options Options, args ...T) ([]int64, error) {
	op := sl.FnName()

	insertParams := make([]river.InsertManyParams, 0, len(args))
	for _, arg := range args {
		insertParams = append(insertParams, river.InsertManyParams{
			Args: arg,
			InsertOpts: &river.InsertOpts{
				ScheduledAt: options.ScheduledAt,
			},
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
	if len(ids) == 0 {
		return nil, ErrJobIdsNotProvided
	}

	params := river.NewJobListParams().IDs(ids...)

	jobs, err := r.river.JobList(ctx, params)
	if err != nil {
		return nil, slerr.WithSource(err)
	}

	statuses := arrutil.Map(jobs.Jobs, func(input *rivertype.JobRow) (target JobStatus, find bool) {
		return JobStatus{
			JobID:  input.ID,
			Status: input.State,
		}, true
	})

	return statuses, nil
}

func (r *Repository[T]) FetchJobs(ctx context.Context, ids ...int64) ([]*rivertype.JobRow, error) {
	if len(ids) == 0 {
		return nil, ErrJobIdsNotProvided
	}

	params := river.NewJobListParams().IDs(ids...)

	jobs, err := r.river.JobList(ctx, params)
	if err != nil {
		return nil, slerr.WithSource(err)
	}

	if jobs == nil {
		return nil, nil
	}

	return jobs.Jobs, nil
}

func (r *Repository[T]) FetchJob(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	jobs, err := r.FetchJobs(ctx, id)
	if err != nil {
		return nil, slerr.WithSource(err)
	}

	if len(jobs) == 0 {
		return nil, ErrJobNotFound
	}

	return jobs[0], nil
}
