package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultTTL is how long a completed response is kept for replay.
	DefaultTTL = 24 * time.Hour
	// DefaultLockTTL is how long an in-flight lock is valid if the process dies mid-request.
	DefaultLockTTL = 30 * time.Second
)

var errNilHandler = errors.New("idempotency: handler must not be nil")

// Option configures a Service created with New.
type Option func(*config)

type config struct {
	ttl     time.Duration
	lockTTL time.Duration
	now     func() time.Time
}

// WithTTL sets how long a completed response is replayed. It panics if ttl is not positive.
func WithTTL(ttl time.Duration) Option {
	return func(c *config) {
		if ttl <= 0 {
			panic("idempotency: WithTTL requires a positive duration")
		}
		c.ttl = ttl
	}
}

// WithLockTTL sets how long an in-flight lock stays valid. It panics if ttl is not positive.
// The lock should be longer than the slowest expected handler, otherwise a second request can
// start while the first one is still running.
func WithLockTTL(ttl time.Duration) Option {
	return func(c *config) {
		if ttl <= 0 {
			panic("idempotency: WithLockTTL requires a positive duration")
		}
		c.lockTTL = ttl
	}
}

// WithClock sets the clock used by the Service to decide whether records are expired. It panics
// if now is nil. Stores have their own clock. In tests, pass the same fake clock to both.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now == nil {
			panic("idempotency: WithClock requires a non-nil clock")
		}
		c.now = now
	}
}

// Service runs handlers at most once per idempotency key.
type Service struct {
	store   Store
	ttl     time.Duration
	lockTTL time.Duration
	now     func() time.Time
}

// Result is the outcome of Execute.
type Result struct {
	// Replayed is true if the stored response was returned and the handler did not run.
	Replayed bool
	// Response is the response for the key. For an executed request it is the handler's response.
	Response StoredResponse
}

// New creates a Service backed by store. It panics if store is nil or an option is invalid.
func New(store Store, opts ...Option) *Service {
	if store == nil {
		panic("idempotency: New requires a non-nil Store")
	}
	cfg := config{ttl: DefaultTTL, lockTTL: DefaultLockTTL, now: time.Now}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Service{store: store, ttl: cfg.ttl, lockTTL: cfg.lockTTL, now: cfg.now}
}

// Execute runs handler for key, or replays the stored response.
//
// Behavior:
//   - Invalid key: ErrInvalidKey. The handler does not run.
//   - No live record: the key is locked, handler runs, and a successful response is stored.
//     Result.Replayed is false.
//   - Completed record with the same fingerprint: the stored response is returned with
//     Replayed true. The handler does not run.
//   - Record with a different fingerprint: ErrKeyReusedWithDifferentPayload.
//   - In-flight record with the same fingerprint: ErrRequestInProgress.
//   - Handler error or invalid response: the lock is released, nothing is stored, and the error
//     is returned. A later request with the same key runs the handler again.
//   - Store error on Complete: the lock is kept until its TTL expires, so a retry does not run
//     the handler a second time while the first run may already have had side effects. The
//     error is returned.
//
// Expired records and expired locks count as missing.
func (s *Service) Execute(
	ctx context.Context,
	key, fingerprint string,
	handler func(ctx context.Context) (StoredResponse, error),
) (Result, error) {
	if err := validateKey(key); err != nil {
		return Result{}, err
	}
	if fingerprint == "" {
		return Result{}, ErrInvalidFingerprint
	}
	if handler == nil {
		return Result{}, errNilHandler
	}

	rec, err := s.store.Get(ctx, key)
	if err != nil {
		return Result{}, fmt.Errorf("idempotency: get record: %w", err)
	}
	if rec != nil && s.live(rec) {
		return s.resolve(rec, fingerprint)
	}

	locked, err := s.store.TryLock(ctx, key, fingerprint, s.lockTTL)
	if err != nil {
		return Result{}, fmt.Errorf("idempotency: acquire lock: %w", err)
	}
	if !locked {
		// Another request took the key between Get and TryLock.
		raced, err := s.store.Get(ctx, key)
		if err != nil {
			return Result{}, fmt.Errorf("idempotency: get record: %w", err)
		}
		if raced != nil && s.live(raced) {
			return s.resolve(raced, fingerprint)
		}
		return Result{}, ErrRequestInProgress
	}

	return s.run(ctx, key, fingerprint, handler)
}

// run executes the handler while the caller holds the lock.
func (s *Service) run(
	ctx context.Context,
	key, fingerprint string,
	handler func(ctx context.Context) (StoredResponse, error),
) (Result, error) {
	handlerFinished := false
	defer func() {
		if !handlerFinished {
			// Handler error or panic: free the key so the client can retry. The release error is
			// ignored because the lock expires on its own, and the handler error matters more.
			_ = s.store.Release(context.WithoutCancel(ctx), key)
		}
	}()

	resp, err := handler(ctx)
	if err != nil {
		return Result{}, err
	}
	if err := validateResponse(resp); err != nil {
		return Result{}, err
	}

	// From here on the handler has completed. The lock must not be released on error,
	// otherwise a retry would run the side effects a second time.
	handlerFinished = true
	stored := cloneResponse(resp)
	rec := Record{
		Fingerprint: fingerprint,
		State:       StateDone,
		Response:    stored,
		ExpiresAt:   s.now().Add(s.ttl),
	}
	if err := s.store.Complete(ctx, key, rec, s.ttl); err != nil {
		return Result{}, fmt.Errorf("idempotency: store response: %w", err)
	}
	return Result{Replayed: false, Response: cloneResponse(stored)}, nil
}

// resolve decides what an existing live record means for a request with fingerprint.
func (s *Service) resolve(rec *Record, fingerprint string) (Result, error) {
	if rec.Fingerprint != fingerprint {
		return Result{}, ErrKeyReusedWithDifferentPayload
	}
	if rec.State == StateDone {
		return Result{Replayed: true, Response: cloneResponse(rec.Response)}, nil
	}
	return Result{}, ErrRequestInProgress
}

func (s *Service) live(rec *Record) bool {
	return rec.ExpiresAt.After(s.now())
}

func validateResponse(resp StoredResponse) error {
	if resp.StatusCode < 100 || resp.StatusCode > 599 {
		return fmt.Errorf("%w: status code %d is outside 100-599", ErrInvalidResponse, resp.StatusCode)
	}
	return nil
}
