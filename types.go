// Package idempotency makes HTTP POST (and PUT/PATCH) requests idempotent.
//
// A client sends an Idempotency-Key with each mutating request. The first request with a key
// runs the handler and stores its response. Later requests with the same key and the same
// request fingerprint receive the stored response without running the handler again. Requests
// with the same key but a different fingerprint are rejected. Concurrent requests with the same
// key do not run in parallel.
//
// The core is standard library only. Persistence is pluggable through the Store interface.
// MemoryStore is provided for tests and single-process use.
package idempotency

import (
	"bytes"
	"context"
	"net/http"
	"time"
)

// State is the lifecycle state of a stored idempotency record.
type State uint8

const (
	// StateProcessing means a request holds the key and its handler is running.
	StateProcessing State = iota + 1
	// StateDone means the handler finished and its response is stored for replay.
	StateDone
)

// StoredResponse is the HTTP response captured for a key and replayed later.
type StoredResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// Record is the persisted state of one idempotency key.
type Record struct {
	Fingerprint string
	State       State
	Response    StoredResponse
	ExpiresAt   time.Time
}

// Store persists idempotency records. Implementations must satisfy the contract checked by
// storetest.RunStoreContract.
//
// Implementations decide expiry with their own clock. TTLs are relative durations.
//
//   - Get returns (nil, nil) for a missing or expired key. It returns a copy of the record.
//   - TryLock atomically creates a Processing record if the key has no live record. It returns
//     false, and no error, if a live record exists. Atomicity is what prevents two concurrent
//     requests from both running the handler.
//   - Complete replaces the record with a Done record that expires after ttl. The ExpiresAt
//     field of the passed Record is informational and may be ignored.
//   - Release removes a Processing record. It must never remove a Done record. Releasing a
//     missing key is not an error.
type Store interface {
	Get(ctx context.Context, key string) (*Record, error)
	TryLock(ctx context.Context, key, fingerprint string, ttl time.Duration) (bool, error)
	Complete(ctx context.Context, key string, rec Record, ttl time.Duration) error
	Release(ctx context.Context, key string) error
}

// cloneResponse returns a deep copy so that stored data and caller data never share memory.
func cloneResponse(resp StoredResponse) StoredResponse {
	return StoredResponse{
		StatusCode: resp.StatusCode,
		Header:     resp.Header.Clone(),
		Body:       bytes.Clone(resp.Body),
	}
}
