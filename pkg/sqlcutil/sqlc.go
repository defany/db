package sqlcutil

import (
	"context"
	"reflect"

	"github.com/defany/db/pkg/postgres"
)

func MakeTx[T any](ctx context.Context, querier T) T {
	val := reflect.ValueOf(querier)
	method := val.MethodByName("WithTx")

	if !method.IsValid() {
		panic("there is no WithTx method?")
	}

	tx, ok := postgres.ExtractTX(ctx)
	if !ok {
		panic("lol, man, there are no tx in make tx method?")
	}

	args := []reflect.Value{reflect.ValueOf(tx)}

	result := method.Call(args)

	txQuerier, ok := result[0].Interface().(T)
	if !ok {
		panic("result is not of the expected type")
	}

	return txQuerier
}
