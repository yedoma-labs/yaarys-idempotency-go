package idempotency

import (
	"context"
	"sync"
	"time"
)

// MemoryStore is an in-process Store backed by a map guarded by a mutex.
//
// WARNING: state lives in this process only. It is not shared across instances, restarts or
// workers. Two replicas of a service each have their own store, so a retry that lands on the
// other replica runs the handler again. Use MemoryStore for tests and single-process services.
// For multiple instances, implement Store on a shared backend such as Redis or SQL.
type MemoryStore struct {
	mu      sync.Mutex
	now     func() time.Time
	records map[string]Record
}

// NewMemoryStore creates an empty MemoryStore. now is the clock used for expiry. If now is nil,
// time.Now is used.
func NewMemoryStore(now func() time.Time) *MemoryStore {
	if now == nil {
		now = time.Now
	}
	return &MemoryStore{now: now, records: make(map[string]Record)}
}

// Get returns a copy of the live record for key, or (nil, nil) if there is none.
func (m *MemoryStore) Get(_ context.Context, key string) (*Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.liveLocked(key)
	if !ok {
		return nil, nil
	}
	out := rec
	out.Response = cloneResponse(rec.Response)
	return &out, nil
}

// TryLock atomically creates a Processing record if no live record exists for key.
func (m *MemoryStore) TryLock(_ context.Context, key, fingerprint string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.liveLocked(key); ok {
		return false, nil
	}
	m.records[key] = Record{
		Fingerprint: fingerprint,
		State:       StateProcessing,
		ExpiresAt:   m.now().Add(ttl),
	}
	return true, nil
}

// Complete stores the response as a Done record that expires after ttl.
func (m *MemoryStore) Complete(_ context.Context, key string, rec Record, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.records[key] = Record{
		Fingerprint: rec.Fingerprint,
		State:       StateDone,
		Response:    cloneResponse(rec.Response),
		ExpiresAt:   m.now().Add(ttl),
	}
	return nil
}

// Release removes a Processing record for key. Done records are kept.
func (m *MemoryStore) Release(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rec, ok := m.records[key]; ok && rec.State == StateProcessing {
		delete(m.records, key)
	}
	return nil
}

// liveLocked returns the record for key if it has not expired. Expired records are removed.
// The caller must hold m.mu.
func (m *MemoryStore) liveLocked(key string) (Record, bool) {
	rec, ok := m.records[key]
	if !ok {
		return Record{}, false
	}
	if !rec.ExpiresAt.After(m.now()) {
		delete(m.records, key)
		return Record{}, false
	}
	return rec, true
}
