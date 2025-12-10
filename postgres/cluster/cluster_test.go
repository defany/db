package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.yandex/hasql/v2"
)

type TestSQLDBWrapper struct {
	db *sql.DB
}

func (w *TestSQLDBWrapper) Pool() *pgxpool.Pool {
	return nil
}

func (w *TestSQLDBWrapper) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return nil, fmt.Errorf("Acquire not supported for test *sql.DB wrapper")
}

func (w *TestSQLDBWrapper) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
	return nil
}

func (w *TestSQLDBWrapper) AcquireFunc(ctx context.Context, f func(*pgxpool.Conn) error) error {
	return fmt.Errorf("AcquireFunc not supported for test *sql.DB wrapper")
}

func (w *TestSQLDBWrapper) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &TestSQLTx{tx: tx}, nil
}

func (w *TestSQLDBWrapper) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &TestSQLTx{tx: tx}, nil
}

func (w *TestSQLDBWrapper) Close() {
	w.db.Close()
}

func (w *TestSQLDBWrapper) Config() *pgxpool.Config {
	return nil
}

func (w *TestSQLDBWrapper) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("CopyFrom not supported for test *sql.DB wrapper")
}

func (w *TestSQLDBWrapper) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	_, err := w.db.ExecContext(ctx, sql, arguments...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return pgconn.CommandTag{}, nil
}

func (w *TestSQLDBWrapper) Ping(ctx context.Context) error {
	return w.db.PingContext(ctx)
}

func (w *TestSQLDBWrapper) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := w.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &TestSQLRows{rows: rows}, nil
}

func (w *TestSQLDBWrapper) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	row := w.db.QueryRowContext(ctx, sql, args...)
	return &TestSQLRow{row: row}
}

func (w *TestSQLDBWrapper) Reset() {
	// No reset operation for *sql.DB
}

func (w *TestSQLDBWrapper) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}

func (w *TestSQLDBWrapper) Stat() *pgxpool.Stat {
	return nil
}

func (w *TestSQLDBWrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return w.db.QueryContext(ctx, query, args...)
}

func (w *TestSQLDBWrapper) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return w.db.QueryRowContext(ctx, query, args...)
}

type TestSQLTx struct {
	tx *sql.Tx
}

func (tx *TestSQLTx) Conn() *pgx.Conn {
	return nil // Return nil for test purposes
}

func (tx *TestSQLTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{} // Return empty struct for test purposes
}

func (tx *TestSQLTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return tx, nil // Return self for test purposes
}

func (tx *TestSQLTx) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return tx, nil // Return self for test purposes
}

func (tx *TestSQLTx) Commit(ctx context.Context) error {
	return tx.tx.Commit()
}

func (tx *TestSQLTx) Rollback(ctx context.Context) error {
	return tx.tx.Rollback()
}

func (tx *TestSQLTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	_, err := tx.tx.ExecContext(ctx, sql, arguments...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return pgconn.CommandTag{}, nil
}

func (tx *TestSQLTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := tx.tx.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &TestSQLRows{rows: rows}, nil
}

func (tx *TestSQLTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	row := tx.tx.QueryRowContext(ctx, sql, args...)
	return &TestSQLRow{row: row}
}

func (tx *TestSQLTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("CopyFrom not supported for test SQL transaction")
}

func (tx *TestSQLTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, fmt.Errorf("Prepare not supported for test SQL transaction")
}

func (tx *TestSQLTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *TestSQLTx) CopyFromRows(ctx context.Context, tableName pgx.Identifier, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("CopyFromRows not supported for test SQL transaction")
}

type TestSQLRows struct {
	rows *sql.Rows
}

func (r *TestSQLRows) Conn() *pgx.Conn {
	return nil // Return nil for test purposes
}

func (r *TestSQLRows) Close() {
	r.rows.Close()
}

func (r *TestSQLRows) Err() error {
	return r.rows.Err()
}

func (r *TestSQLRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *TestSQLRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *TestSQLRows) Next() bool {
	return r.rows.Next()
}

func (r *TestSQLRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *TestSQLRows) RawValues() [][]byte {
	return nil // Return nil for test purposes
}

func (r *TestSQLRows) Values() ([]any, error) {
	return nil, fmt.Errorf("Values not supported for test SQL rows")
}

type TestSQLRow struct {
	row *sql.Row
}

func (r *TestSQLRow) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

func wrapDB(db *sql.DB) WrappedPool {
	return &TestSQLDBWrapper{db: db}
}

func TestNewCluster(t *testing.T) {
	dbMaster, dbMasterMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer dbMaster.Close()
	dbMasterMock.MatchExpectationsInOrder(false)

	dbMasterMock.ExpectQuery(`.*pg_is_in_recovery.*`).WillReturnRows(
		sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("role").OfType("int8", 0),
			sqlmock.NewColumn("replication_lag").OfType("int8", 0)).
			AddRow(hasql.NodeStateCriterion(hasql.Primary)-1, 0)).
		RowsWillBeClosed().
		WithoutArgs()

	dbMasterMock.ExpectQuery(`SELECT node_name as name`).WillReturnRows(
		sqlmock.NewRows([]string{"name"}).
			AddRow("master-dc1"))

	dbDRMaster, dbDRMasterMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer dbDRMaster.Close()
	dbDRMasterMock.MatchExpectationsInOrder(false)

	dbDRMasterMock.ExpectQuery(`.*pg_is_in_recovery.*`).WillReturnRows(
		sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("role").OfType("int8", 0),
			sqlmock.NewColumn("replication_lag").OfType("int8", 0)).
			AddRow(hasql.NodeStateCriterion(hasql.Standby)-1, 40)).
		RowsWillBeClosed().
		WithoutArgs()

	dbDRMasterMock.ExpectQuery(`SELECT node_name as name`).WillReturnRows(
		sqlmock.NewRows([]string{"name"}).
			AddRow("drmaster1-dc2"))

	dbSlaveDC1, dbSlaveDC1Mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer dbSlaveDC1.Close()
	dbSlaveDC1Mock.MatchExpectationsInOrder(false)

	dbSlaveDC1Mock.ExpectQuery(`.*pg_is_in_recovery.*`).WillReturnRows(
		sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("role").OfType("int8", 0),
			sqlmock.NewColumn("replication_lag").OfType("int8", 0)).
			AddRow(hasql.NodeStateCriterion(hasql.Standby)-1, 50)).
		RowsWillBeClosed().
		WithoutArgs()

	dbSlaveDC1Mock.ExpectQuery(`SELECT node_name as name`).WillReturnRows(
		sqlmock.NewRows([]string{"name"}).
			AddRow("slave-dc1"))

	dbSlaveDC2, dbSlaveDC2Mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer dbSlaveDC2.Close()
	dbSlaveDC1Mock.MatchExpectationsInOrder(false)

	dbSlaveDC2Mock.ExpectQuery(`.*pg_is_in_recovery.*`).WillReturnRows(
		sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("role").OfType("int8", 0),
			sqlmock.NewColumn("replication_lag").OfType("int8", 0)).
			AddRow(hasql.NodeStateCriterion(hasql.Standby)-1, 50)).
		RowsWillBeClosed().
		WithoutArgs()

	dbSlaveDC2Mock.ExpectQuery(`SELECT node_name as name`).WillReturnRows(
		sqlmock.NewRows([]string{"name"}).
			AddRow("slave-dc2"))

	tctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	c, err := NewCluster[WrappedPool](
		WithClusterRetries(RetryError),
		WithClusterContext(tctx),
		WithClusterNodeChecker(hasql.PostgreSQLChecker),
		WithClusterNodePicker(NewCustomPicker[WrappedPool](
			CustomPickerMaxLag(100),
		)),
		WithClusterNodes(
			ClusterNode{"slave-dc1", wrapDB(dbSlaveDC1), 1},
			ClusterNode{"master-dc1", wrapDB(dbMaster), 1},
			ClusterNode{"slave-dc2", wrapDB(dbSlaveDC2), 2},
			ClusterNode{"drmaster1-dc2", wrapDB(dbDRMaster), 0},
		),
		WithClusterOptions(
			hasql.WithUpdateInterval[WrappedPool](2*time.Second),
			hasql.WithUpdateTimeout[WrappedPool](1*time.Second),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Init(); err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if err = c.WaitForNodes(tctx, hasql.Primary, hasql.Standby); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	node1Name := ""
	//	fmt.Printf("check for Standby\n")
	if row := c.QueryRow(NodeStateCriterion(tctx, hasql.Standby), "SELECT node_name as name"); row == nil {
		t.Fatal("nil row")
	} else if err = row.Scan(&node1Name); err != nil {
		t.Fatal(err)
	} else if "slave-dc1" != node1Name {
		t.Logf("ZZZ %#+v\n", row)
		t.Fatalf("invalid node name %s != %s", "slave-dc1", node1Name)
	}

	dbSlaveDC1Mock.ExpectQuery(`SELECT node_name as name`).WillReturnRows(
		sqlmock.NewRows([]string{"name"}).
			AddRow("slave-dc1"))

	node2Name := ""
	// fmt.Printf("check for PreferStandby\n")
	if row := c.QueryRow(NodeStateCriterion(tctx, hasql.PreferStandby), "SELECT node_name as name"); row == nil {
		t.Fatal("nil row")
	} else if err = row.Scan(&node2Name); err != nil {
		t.Fatal(err)
	} else if "slave-dc1" != node2Name {
		t.Fatalf("invalid node name %s != %s", "slave-dc1", node2Name)
	}

	node3Name := ""
	// fmt.Printf("check for PreferPrimary\n")
	if row := c.QueryRow(NodeStateCriterion(tctx, hasql.PreferPrimary), "SELECT node_name as name"); row == nil {
		t.Fatal("nil row")
	} else if err = row.Scan(&node3Name); err != nil {
		t.Fatal(err)
	} else if "master-dc1" != node3Name {
		t.Fatalf("invalid node name %s != %s", "master-dc1", node3Name)
	}

	dbSlaveDC1Mock.ExpectQuery(`.*`).WillReturnRows(sqlmock.NewRows([]string{"role"}).RowError(1, fmt.Errorf("row error")))

	time.Sleep(2 * time.Second)

	// fmt.Printf("check for PreferStandby\n")
	if row := c.QueryRow(NodeStateCriterion(tctx, hasql.PreferStandby), "SELECT node_name as name"); row != nil {
		t.Fatal("must return error")
	}

	if dbMasterErr := dbMasterMock.ExpectationsWereMet(); dbMasterErr != nil {
		t.Error(dbMasterErr)
	}
}
