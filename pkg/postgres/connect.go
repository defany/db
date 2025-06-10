package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/defany/db/pkg/retry"
	"github.com/defany/slogger/pkg/logger/sl"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultMaxConnAttempts = 3
	defaultRetryConnDelay  = time.Second
	defaultAcquireTimeout  = 5 * time.Second
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     string
	Database string

	ConnAmount        *int32
	MinConnAmount     *int32
	MaxConnIdleTime   *time.Duration
	MaxConnLifetime   *time.Duration
	HealthCheckPeriod *time.Duration
	AcquireTimeout    time.Duration

	maxConnAttempts int
	retryConnDelay  time.Duration

	tracer pgx.QueryTracer
}

func NewConfig(username, password, host, port, database string) *Config {
	return &Config{
		Username: username,
		Password: password,
		Host:     host,
		Port:     port,
		Database: database,

		maxConnAttempts: defaultMaxConnAttempts,
		retryConnDelay:  defaultRetryConnDelay,

		AcquireTimeout: defaultAcquireTimeout,
	}
}

func (c *Config) WithConnAmount(amount int32) *Config {
	c.ConnAmount = &amount
	return c
}

func (c *Config) WithMinConnAmount(amount int32) *Config {
	c.MinConnAmount = &amount
	return c
}

func (c *Config) WithMaxConnIdleTime(d time.Duration) *Config {
	c.MaxConnIdleTime = &d
	return c
}

func (c *Config) WithMaxConnLifetime(d time.Duration) *Config {
	c.MaxConnLifetime = &d
	return c
}

func (c *Config) WithHealthCheckPeriod(d time.Duration) *Config {
	c.HealthCheckPeriod = &d
	return c
}

func (c *Config) WithAcquireTimeout(d time.Duration) *Config {
	c.AcquireTimeout = &d
	return c
}

func (c *Config) WithMaxConnAttempts(attempts int) *Config {
	c.maxConnAttempts = attempts
	return c
}

func (c *Config) WithRetryConnDelay(delay time.Duration) *Config {
	c.retryConnDelay = delay
	return c
}

func (c *Config) WithTracer(tracer pgx.QueryTracer) *Config {
	c.tracer = tracer
	return c
}

func (c *Config) dsn() string {
	return fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		c.Username, c.Password,
		c.Host, c.Port, c.Database,
	)
}

// NewClient создает и конфигурирует пул соединений.
func NewClient(ctx context.Context, log *slog.Logger, cfg *Config) (pool *pgxpool.Pool, err error) {
	dsn := cfg.dsn()

	err = retry.WithAttempts(cfg.maxConnAttempts, cfg.retryConnDelay, func() error {
		log.Info("connecting to postgresql database...")

		connectCtx, cancel := context.WithTimeout(ctx, cfg.AcquireTimeout)
		defer cancel()

		pgxCfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			log.Error("Unable to parse configs", sl.ErrAttr(err))
			return err
		}

		if cfg.ConnAmount != nil {
			pgxCfg.MaxConns = *cfg.ConnAmount
		}

		if pgxCfg.MinConns != nil {
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

		pool, err = pgxpool.NewWithConfig(connectCtx, pgxCfg)
		if err != nil {
			log.Error("failed to connect to postgres...", sl.ErrAttr(err))
			return err
		}

		err = pool.Ping(ctx)
		if err != nil {
			log.Error("ping to postgres failed...", sl.ErrAttr(err))
		}

		return err
	})

	if err != nil {
		log.Error("all attempts are exceeded. unable to connect to postgres database")
		return nil, err
	}

	log.Info("connected to postgresql")
	return pool, nil
}
