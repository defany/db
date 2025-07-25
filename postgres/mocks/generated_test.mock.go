// Code generated by mockery; DO NOT EDIT.
// github.com/vektra/mockery
// template: testify

package mockpostgres

import (
	"context"

	"github.com/defany/db/v2/tx_manager"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	mock "github.com/stretchr/testify/mock"
)

// NewMockPostgres creates a new instance of MockPostgres. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockPostgres(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockPostgres {
	mock := &MockPostgres{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// MockPostgres is an autogenerated mock type for the Postgres type
type MockPostgres struct {
	mock.Mock
}

type MockPostgres_Expecter struct {
	mock *mock.Mock
}

func (_m *MockPostgres) EXPECT() *MockPostgres_Expecter {
	return &MockPostgres_Expecter{mock: &_m.Mock}
}

// BeginTx provides a mock function for the type MockPostgres
func (_mock *MockPostgres) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (txman.Tx, error) {
	ret := _mock.Called(ctx, txOptions)

	if len(ret) == 0 {
		panic("no return value specified for BeginTx")
	}

	var r0 txman.Tx
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, pgx.TxOptions) (txman.Tx, error)); ok {
		return returnFunc(ctx, txOptions)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, pgx.TxOptions) txman.Tx); ok {
		r0 = returnFunc(ctx, txOptions)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(txman.Tx)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, pgx.TxOptions) error); ok {
		r1 = returnFunc(ctx, txOptions)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockPostgres_BeginTx_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BeginTx'
type MockPostgres_BeginTx_Call struct {
	*mock.Call
}

// BeginTx is a helper method to define mock.On call
//   - ctx context.Context
//   - txOptions pgx.TxOptions
func (_e *MockPostgres_Expecter) BeginTx(ctx interface{}, txOptions interface{}) *MockPostgres_BeginTx_Call {
	return &MockPostgres_BeginTx_Call{Call: _e.mock.On("BeginTx", ctx, txOptions)}
}

func (_c *MockPostgres_BeginTx_Call) Run(run func(ctx context.Context, txOptions pgx.TxOptions)) *MockPostgres_BeginTx_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 pgx.TxOptions
		if args[1] != nil {
			arg1 = args[1].(pgx.TxOptions)
		}
		run(
			arg0,
			arg1,
		)
	})
	return _c
}

func (_c *MockPostgres_BeginTx_Call) Return(tx txman.Tx, err error) *MockPostgres_BeginTx_Call {
	_c.Call.Return(tx, err)
	return _c
}

func (_c *MockPostgres_BeginTx_Call) RunAndReturn(run func(ctx context.Context, txOptions pgx.TxOptions) (txman.Tx, error)) *MockPostgres_BeginTx_Call {
	_c.Call.Return(run)
	return _c
}

// Close provides a mock function for the type MockPostgres
func (_mock *MockPostgres) Close() {
	_mock.Called()
	return
}

// MockPostgres_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type MockPostgres_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
func (_e *MockPostgres_Expecter) Close() *MockPostgres_Close_Call {
	return &MockPostgres_Close_Call{Call: _e.mock.On("Close")}
}

func (_c *MockPostgres_Close_Call) Run(run func()) *MockPostgres_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockPostgres_Close_Call) Return() *MockPostgres_Close_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockPostgres_Close_Call) RunAndReturn(run func()) *MockPostgres_Close_Call {
	_c.Run(run)
	return _c
}

// Exec provides a mock function for the type MockPostgres
func (_mock *MockPostgres) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	var tmpRet mock.Arguments
	if len(args) > 0 {
		tmpRet = _mock.Called(ctx, query, args)
	} else {
		tmpRet = _mock.Called(ctx, query)
	}
	ret := tmpRet

	if len(ret) == 0 {
		panic("no return value specified for Exec")
	}

	var r0 pgconn.CommandTag
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, string, ...interface{}) (pgconn.CommandTag, error)); ok {
		return returnFunc(ctx, query, args...)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, string, ...interface{}) pgconn.CommandTag); ok {
		r0 = returnFunc(ctx, query, args...)
	} else {
		r0 = ret.Get(0).(pgconn.CommandTag)
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, string, ...interface{}) error); ok {
		r1 = returnFunc(ctx, query, args...)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockPostgres_Exec_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Exec'
type MockPostgres_Exec_Call struct {
	*mock.Call
}

// Exec is a helper method to define mock.On call
//   - ctx context.Context
//   - query string
//   - args ...interface{}
func (_e *MockPostgres_Expecter) Exec(ctx interface{}, query interface{}, args ...interface{}) *MockPostgres_Exec_Call {
	return &MockPostgres_Exec_Call{Call: _e.mock.On("Exec",
		append([]interface{}{ctx, query}, args...)...)}
}

func (_c *MockPostgres_Exec_Call) Run(run func(ctx context.Context, query string, args ...interface{})) *MockPostgres_Exec_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 string
		if args[1] != nil {
			arg1 = args[1].(string)
		}
		var arg2 []interface{}
		var variadicArgs []interface{}
		if len(args) > 2 {
			variadicArgs = args[2].([]interface{})
		}
		arg2 = variadicArgs
		run(
			arg0,
			arg1,
			arg2...,
		)
	})
	return _c
}

func (_c *MockPostgres_Exec_Call) Return(commandTag pgconn.CommandTag, err error) *MockPostgres_Exec_Call {
	_c.Call.Return(commandTag, err)
	return _c
}

func (_c *MockPostgres_Exec_Call) RunAndReturn(run func(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)) *MockPostgres_Exec_Call {
	_c.Call.Return(run)
	return _c
}

// Pool provides a mock function for the type MockPostgres
func (_mock *MockPostgres) Pool() *pgxpool.Pool {
	ret := _mock.Called()

	if len(ret) == 0 {
		panic("no return value specified for Pool")
	}

	var r0 *pgxpool.Pool
	if returnFunc, ok := ret.Get(0).(func() *pgxpool.Pool); ok {
		r0 = returnFunc()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pgxpool.Pool)
		}
	}
	return r0
}

// MockPostgres_Pool_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Pool'
type MockPostgres_Pool_Call struct {
	*mock.Call
}

// Pool is a helper method to define mock.On call
func (_e *MockPostgres_Expecter) Pool() *MockPostgres_Pool_Call {
	return &MockPostgres_Pool_Call{Call: _e.mock.On("Pool")}
}

func (_c *MockPostgres_Pool_Call) Run(run func()) *MockPostgres_Pool_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockPostgres_Pool_Call) Return(pool *pgxpool.Pool) *MockPostgres_Pool_Call {
	_c.Call.Return(pool)
	return _c
}

func (_c *MockPostgres_Pool_Call) RunAndReturn(run func() *pgxpool.Pool) *MockPostgres_Pool_Call {
	_c.Call.Return(run)
	return _c
}

// Query provides a mock function for the type MockPostgres
func (_mock *MockPostgres) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	var tmpRet mock.Arguments
	if len(args) > 0 {
		tmpRet = _mock.Called(ctx, query, args)
	} else {
		tmpRet = _mock.Called(ctx, query)
	}
	ret := tmpRet

	if len(ret) == 0 {
		panic("no return value specified for Query")
	}

	var r0 pgx.Rows
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, string, ...interface{}) (pgx.Rows, error)); ok {
		return returnFunc(ctx, query, args...)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, string, ...interface{}) pgx.Rows); ok {
		r0 = returnFunc(ctx, query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pgx.Rows)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, string, ...interface{}) error); ok {
		r1 = returnFunc(ctx, query, args...)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockPostgres_Query_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Query'
type MockPostgres_Query_Call struct {
	*mock.Call
}

// Query is a helper method to define mock.On call
//   - ctx context.Context
//   - query string
//   - args ...interface{}
func (_e *MockPostgres_Expecter) Query(ctx interface{}, query interface{}, args ...interface{}) *MockPostgres_Query_Call {
	return &MockPostgres_Query_Call{Call: _e.mock.On("Query",
		append([]interface{}{ctx, query}, args...)...)}
}

func (_c *MockPostgres_Query_Call) Run(run func(ctx context.Context, query string, args ...interface{})) *MockPostgres_Query_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 string
		if args[1] != nil {
			arg1 = args[1].(string)
		}
		var arg2 []interface{}
		var variadicArgs []interface{}
		if len(args) > 2 {
			variadicArgs = args[2].([]interface{})
		}
		arg2 = variadicArgs
		run(
			arg0,
			arg1,
			arg2...,
		)
	})
	return _c
}

func (_c *MockPostgres_Query_Call) Return(rows pgx.Rows, err error) *MockPostgres_Query_Call {
	_c.Call.Return(rows, err)
	return _c
}

func (_c *MockPostgres_Query_Call) RunAndReturn(run func(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)) *MockPostgres_Query_Call {
	_c.Call.Return(run)
	return _c
}

// QueryRow provides a mock function for the type MockPostgres
func (_mock *MockPostgres) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	var tmpRet mock.Arguments
	if len(args) > 0 {
		tmpRet = _mock.Called(ctx, query, args)
	} else {
		tmpRet = _mock.Called(ctx, query)
	}
	ret := tmpRet

	if len(ret) == 0 {
		panic("no return value specified for QueryRow")
	}

	var r0 pgx.Row
	if returnFunc, ok := ret.Get(0).(func(context.Context, string, ...interface{}) pgx.Row); ok {
		r0 = returnFunc(ctx, query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pgx.Row)
		}
	}
	return r0
}

// MockPostgres_QueryRow_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'QueryRow'
type MockPostgres_QueryRow_Call struct {
	*mock.Call
}

// QueryRow is a helper method to define mock.On call
//   - ctx context.Context
//   - query string
//   - args ...interface{}
func (_e *MockPostgres_Expecter) QueryRow(ctx interface{}, query interface{}, args ...interface{}) *MockPostgres_QueryRow_Call {
	return &MockPostgres_QueryRow_Call{Call: _e.mock.On("QueryRow",
		append([]interface{}{ctx, query}, args...)...)}
}

func (_c *MockPostgres_QueryRow_Call) Run(run func(ctx context.Context, query string, args ...interface{})) *MockPostgres_QueryRow_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 string
		if args[1] != nil {
			arg1 = args[1].(string)
		}
		var arg2 []interface{}
		var variadicArgs []interface{}
		if len(args) > 2 {
			variadicArgs = args[2].([]interface{})
		}
		arg2 = variadicArgs
		run(
			arg0,
			arg1,
			arg2...,
		)
	})
	return _c
}

func (_c *MockPostgres_QueryRow_Call) Return(row pgx.Row) *MockPostgres_QueryRow_Call {
	_c.Call.Return(row)
	return _c
}

func (_c *MockPostgres_QueryRow_Call) RunAndReturn(run func(ctx context.Context, query string, args ...interface{}) pgx.Row) *MockPostgres_QueryRow_Call {
	_c.Call.Return(run)
	return _c
}
