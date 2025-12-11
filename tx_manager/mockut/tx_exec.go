package mockut

import (
	"context"

	txman "git.portals-mem.com/portals/backend/db.git/v3/tx_manager"
	mocktxman "git.portals-mem.com/portals/backend/db.git/v3/tx_manager/mocks"
	"github.com/stretchr/testify/mock"
)

func MockOnTxExecution(tx *mocktxman.MockTxManager) {
	tx.
		On("RunWithOpts",
			mock.IsType(context.Background()),
			mock.MatchedBy(func(h txman.Handler) bool { return h != nil }),
			mock.IsType([]txman.TxOption{}),
		).
		Return(func(ctx context.Context, h txman.Handler, _ []txman.TxOption) error {
			return h(ctx)
		}).
		Once()
}
