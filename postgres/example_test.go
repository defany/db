package postgres_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/defany/db/v2/postgres"
	txman "github.com/defany/db/v2/tx_manager"
)

// ExampleNewPostgres_withReplicas demonstrates how to configure read replicas.
func ExampleNewPostgres_withReplicas() {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := postgres.NewConfig("user", "password", "primary.db.example.com", "5432", "mydb").
		WithConnAmount(20).
		WithHealthCheckPeriod(30).
		WithReplicas(
			postgres.NewReplicaConfig("postgresql://user:password@replica1.db.example.com:5432/mydb").
				WithName("replica-1"),
			postgres.NewReplicaConfig("postgresql://user:password@replica2.db.example.com:5432/mydb").
				WithName("replica-2"),
		).
		WithReplicaStrategy(postgres.ReplicaStrategyRoundRobin)

	db, err := postgres.NewPostgres(ctx, log, cfg)
	if err != nil {
		log.Error("failed to connect", slog.Any("error", err))
		return
	}
	defer db.Close()

	rows, err := db.Query(ctx, "SELECT id, name FROM users WHERE active = $1", true)
	if err != nil {
		log.Error("query failed", slog.Any("error", err))
		return
	}
	defer rows.Close()

	_, err = db.Exec(ctx, "INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")
	if err != nil {
		log.Error("exec failed", slog.Any("error", err))
		return
	}
}

// ExampleNewPostgres_withReplicaPool demonstrates manual ReplicaPool creation.
func ExampleNewPostgres_withReplicaPool() {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	primaryCfg := postgres.NewConfig("user", "pass", "primary.db", "5432", "mydb")
	db, _ := postgres.NewPostgres(ctx, log, primaryCfg)
	defer db.Close()

	replicaPool, err := postgres.NewReplicaPool(ctx, log, []*postgres.ReplicaConfig{
		postgres.NewReplicaConfig("postgresql://user:pass@replica1:5432/mydb").
			WithName("replica-1").
			WithConnAmount(50),
		postgres.NewReplicaConfig("postgresql://user:pass@replica2:5432/mydb").
			WithName("replica-2").
			WithConnAmount(30),
	}, postgres.ReplicaStrategyRoundRobin)

	if err != nil {
		log.Error("failed to create replica pool", slog.Any("error", err))
		return
	}

	db.WithReplicaPool(replicaPool)

	rows, _ := db.Query(ctx, "SELECT * FROM users")
	defer rows.Close()
}

// ExampleNewPostgres_transactions demonstrates transaction behavior with replicas.
func ExampleNewPostgres_transactions() {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := postgres.NewConfig("user", "password", "primary.db.example.com", "5432", "mydb").
		WithReplicas(
			postgres.NewReplicaConfig("postgresql://user:password@replica1.db.example.com:5432/mydb"),
		)

	db, err := postgres.NewPostgres(ctx, log, cfg)
	if err != nil {
		log.Error("failed to connect", slog.Any("error", err))
		return
	}
	defer db.Close()

	tm := txman.New(db)

	// Inside a transaction, all operations go to primary.
	err = tm.ReadCommitted(ctx, func(txCtx context.Context) error {
		// This query goes to PRIMARY (not replica) because we're in a transaction.
		var count int
		row := db.QueryRow(txCtx, "SELECT COUNT(*) FROM users")
		if err := row.Scan(&count); err != nil {
			return err
		}

		// Write operation also goes to primary.
		_, err := db.Exec(txCtx, "UPDATE users SET last_seen = NOW() WHERE id = $1", 123)
		return err
	})

	if err != nil {
		log.Error("transaction failed", slog.Any("error", err))
	}
}

// ExampleNewPostgres_withoutReplicas demonstrates backward compatibility.
func ExampleNewPostgres_withoutReplicas() {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Without replicas, all operations go to primary (backward compatible).
	cfg := postgres.NewConfig("user", "password", "primary.db.example.com", "5432", "mydb")

	db, err := postgres.NewPostgres(ctx, log, cfg)
	if err != nil {
		log.Error("failed to connect", slog.Any("error", err))
		return
	}
	defer db.Close()

	// All queries go to primary.
	rows, err := db.Query(ctx, "SELECT * FROM users")
	if err != nil {
		log.Error("query failed", slog.Any("error", err))
		return
	}
	defer rows.Close()
}

// ExampleReplicaStrategy demonstrates different load balancing strategies.
func ExampleReplicaStrategy() {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Round-robin strategy (default): distributes load evenly.
	cfgRR := postgres.NewConfig("user", "pass", "primary", "5432", "db").
		WithReplicas(
			postgres.NewReplicaConfig("postgresql://replica1:5432/db"),
			postgres.NewReplicaConfig("postgresql://replica2:5432/db"),
		).
		WithReplicaStrategy(postgres.ReplicaStrategyRoundRobin)

	dbRR, _ := postgres.NewPostgres(ctx, log, cfgRR)
	defer dbRR.Close()

	// Random strategy: selects replica randomly for each query.
	cfgRandom := postgres.NewConfig("user", "pass", "primary", "5432", "db").
		WithReplicas(
			postgres.NewReplicaConfig("postgresql://replica1:5432/db"),
			postgres.NewReplicaConfig("postgresql://replica2:5432/db"),
		).
		WithReplicaStrategy(postgres.ReplicaStrategyRandom)

	dbRandom, _ := postgres.NewPostgres(ctx, log, cfgRandom)
	defer dbRandom.Close()
}

// ExampleNewPostgres_withFallbackDisabled demonstrates disabling automatic fallback to primary.
func ExampleNewPostgres_withFallbackDisabled() {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Disable automatic fallback - if replica fails, query will fail immediately.
	cfg := postgres.NewConfig("user", "pass", "primary", "5432", "db").
		WithReplicas(
			postgres.NewReplicaConfig("postgresql://replica1:5432/db"),
		).
		WithReplicaFallback(false) // Disable fallback

	db, _ := postgres.NewPostgres(ctx, log, cfg)
	defer db.Close()

	// If replica fails, this query will return error (no fallback to primary).
	_, err := db.Query(ctx, "SELECT * FROM users")
	if err != nil {
		log.Error("query failed, no fallback", slog.Any("error", err))
	}
}
