package idempotency_test

import (
	"context"
	"sync"
	"time"

	idempotency "github.com/yedoma-labs/yaarys-idempotency-go"
)

// fakeClock is a manually advanced clock that is safe for concurrent use.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)}
}

// Now returns the current fake time.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake time forward by d.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// wrappedStore embeds a Store and can inject errors into selected operations.
type wrappedStore struct {
	idempotency.Store
	getErr      error
	lockErr     error
	completeErr error
}

func (s *wrappedStore) Get(ctx context.Context, key string) (*idempotency.Record, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.Store.Get(ctx, key)
}

func (s *wrappedStore) TryLock(ctx context.Context, key, fingerprint string, ttl time.Duration) (bool, error) {
	if s.lockErr != nil {
		return false, s.lockErr
	}
	return s.Store.TryLock(ctx, key, fingerprint, ttl)
}

func (s *wrappedStore) Complete(ctx context.Context, key string, rec idempotency.Record, ttl time.Duration) error {
	if s.completeErr != nil {
		return s.completeErr
	}
	return s.Store.Complete(ctx, key, rec, ttl)
}
