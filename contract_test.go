package idempotency_test

import (
	"testing"
	"time"

	idempotency "github.com/yedoma-labs/yaarys-idempotency-go"
	"github.com/yedoma-labs/yaarys-idempotency-go/storetest"
)

func TestMemoryStoreContract(t *testing.T) {
	storetest.RunStoreContract(t, func(now func() time.Time) idempotency.Store {
		return idempotency.NewMemoryStore(now)
	})
}
