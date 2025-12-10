package cluster

import (
	"context"
	"testing"
)

func TestRetryError(t *testing.T) {
	var err error

	if RetryError(context.TODO(), 0, err) != false {
		t.Fatal("RetryError works unexpected")
	}
}
