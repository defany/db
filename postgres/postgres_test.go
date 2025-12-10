package postgres_test

import (
	"context"
	"testing"

	txman "github.com/defany/db/v3/tx_manager"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// mockTx is a minimal mock implementation of txman.Tx for testing.
type mockTx struct{}

func (m *mockTx) Commit(ctx context.Context) error   { return nil }
func (m *mockTx) Rollback(ctx context.Context) error { return nil }
func (m *mockTx) Conn() *pgx.Conn                    { return nil }
func (m *mockTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (m *mockTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (m *mockTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (m *mockTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (m *mockTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (m *mockTx) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (m *mockTx) QueryRow(context.Context, string, ...any) pgx.Row        { return nil }
func (m *mockTx) Begin(context.Context) (pgx.Tx, error)                   { return nil, nil }

func TestTransactionContext_ExtractTX(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Without transaction.
	_, ok := txman.ExtractTX(ctx)
	if ok {
		t.Error("expected no transaction in empty context")
	}

	// With transaction.
	tx := &mockTx{}
	ctxWithTx := txman.InjectTX(ctx, tx)

	_, ok = txman.ExtractTX(ctxWithTx)
	if !ok {
		t.Fatal("expected transaction to be present")
	}
}
