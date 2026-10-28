// Package storetest provides a contract test suite for idempotency.Store implementations.
//
// External stores (Redis, SQL, ...) should run RunStoreContract in their own test suite. The
// suite checks the atomicity and expiry behavior that the idempotency.Service depends on.
package storetest

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	idempotency "github.com/yedoma-labs/yaarys-idempotency-go"
)

// RunStoreContract runs the store contract tests as subtests of t.
//
// factory must return a new, empty store that uses the given clock for expiry. The suite
// advances the clock to simulate TTL expiry, so the store must not use the wall clock.
func RunStoreContract(t *testing.T, factory func(now func() time.Time) idempotency.Store) {
	t.Helper()

	cases := []struct {
		name string
		run  func(t *testing.T, s idempotency.Store, clk *fakeClock)
	}{
		{"get missing key returns nil", testGetMissing},
		{"trylock is exclusive while live", testTryLockExclusive},
		{"release frees a processing lock", testReleaseFreesLock},
		{"release keeps done record", testReleaseKeepsDone},
		{"complete then get returns done record", testCompleteThenGet},
		{"stored data is isolated from callers", testIsolation},
		{"complete replaces processing lock", testCompleteReplacesLock},
		{"expired lock counts as missing", testExpiredLock},
		{"expired done record counts as missing", testExpiredDone},
		{"record is live just before expiry", testLiveBeforeExpiry},
		{"release of missing key is not an error", testReleaseMissing},
		{"keys are independent", testIndependentKeys},
		{"concurrent trylock has exactly one winner", testConcurrentTryLock},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newFakeClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
			tc.run(t, factory(clk.Now), clk)
		})
	}
}

const (
	testFingerprint = "fp-1"
	testTTL         = time.Hour
)

func testGetMissing(t *testing.T, s idempotency.Store, _ *fakeClock) {
	rec, err := s.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec != nil {
		t.Fatalf("Get on missing key returned %+v, want nil", rec)
	}
}

func testTryLockExclusive(t *testing.T, s idempotency.Store, _ *fakeClock) {
	ctx := context.Background()
	if got := mustTryLock(t, s, "k", testFingerprint, testTTL); !got {
		t.Fatal("first TryLock returned false, want true")
	}
	if got := mustTryLock(t, s, "k", testFingerprint, testTTL); got {
		t.Fatal("second TryLock on a live lock returned true, want false")
	}
	rec, err := s.Get(ctx, "k")
	if err != nil || rec == nil {
		t.Fatalf("Get after lock: rec=%v err=%v", rec, err)
	}
	if rec.State != idempotency.StateProcessing || rec.Fingerprint != testFingerprint {
		t.Fatalf("Get after lock: got state=%v fingerprint=%q", rec.State, rec.Fingerprint)
	}
}

func testReleaseFreesLock(t *testing.T, s idempotency.Store, _ *fakeClock) {
	ctx := context.Background()
	mustTryLock(t, s, "k", testFingerprint, testTTL)
	if err := s.Release(ctx, "k"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := mustTryLock(t, s, "k", testFingerprint, testTTL); !got {
		t.Fatal("TryLock after Release returned false, want true")
	}
}

func testReleaseKeepsDone(t *testing.T, s idempotency.Store, clk *fakeClock) {
	ctx := context.Background()
	mustTryLock(t, s, "k", testFingerprint, testTTL)
	mustComplete(t, s, "k", testFingerprint, sampleResponse(), testTTL, clk)
	if err := s.Release(ctx, "k"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	rec, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec == nil || rec.State != idempotency.StateDone {
		t.Fatalf("Release removed or changed a done record: %+v", rec)
	}
	if got := mustTryLock(t, s, "k", testFingerprint, testTTL); got {
		t.Fatal("TryLock on a done record returned true, want false")
	}
}

func testCompleteThenGet(t *testing.T, s idempotency.Store, clk *fakeClock) {
	ctx := context.Background()
	mustTryLock(t, s, "k", testFingerprint, testTTL)
	mustComplete(t, s, "k", testFingerprint, sampleResponse(), testTTL, clk)

	rec, err := s.Get(ctx, "k")
	if err != nil || rec == nil {
		t.Fatalf("Get: rec=%v err=%v", rec, err)
	}
	if rec.Fingerprint != testFingerprint {
		t.Errorf("Fingerprint = %q, want %q", rec.Fingerprint, testFingerprint)
	}
	if rec.State != idempotency.StateDone {
		t.Errorf("State = %v, want StateDone", rec.State)
	}
	if rec.Response.StatusCode != 201 {
		t.Errorf("StatusCode = %d, want 201", rec.Response.StatusCode)
	}
	if got := rec.Response.Header.Get("Location"); got != "/orders/1" {
		t.Errorf("Location header = %q, want /orders/1", got)
	}
	if string(rec.Response.Body) != `{"id":1}` {
		t.Errorf("Body = %q, want {\"id\":1}", rec.Response.Body)
	}
}

func testIsolation(t *testing.T, s idempotency.Store, clk *fakeClock) {
	ctx := context.Background()
	mustTryLock(t, s, "k", testFingerprint, testTTL)

	resp := sampleResponse()
	mustComplete(t, s, "k", testFingerprint, resp, testTTL, clk)

	// Mutating the value passed to Complete must not change what is stored.
	resp.Body[0] = 'X'
	resp.Header.Set("Location", "/mutated")

	rec, err := s.Get(ctx, "k")
	if err != nil || rec == nil {
		t.Fatalf("Get: rec=%v err=%v", rec, err)
	}
	if string(rec.Response.Body) != `{"id":1}` || rec.Response.Header.Get("Location") != "/orders/1" {
		t.Fatalf("stored response changed after caller mutation: body=%q location=%q",
			rec.Response.Body, rec.Response.Header.Get("Location"))
	}

	// Mutating a value returned by Get must not change what is stored.
	rec.Response.Body[0] = 'Y'
	rec.Response.Header.Set("Location", "/mutated-again")

	again, err := s.Get(ctx, "k")
	if err != nil || again == nil {
		t.Fatalf("second Get: rec=%v err=%v", again, err)
	}
	if string(again.Response.Body) != `{"id":1}` || again.Response.Header.Get("Location") != "/orders/1" {
		t.Fatalf("stored response changed after Get mutation: body=%q location=%q",
			again.Response.Body, again.Response.Header.Get("Location"))
	}
}

func testCompleteReplacesLock(t *testing.T, s idempotency.Store, clk *fakeClock) {
	ctx := context.Background()
	mustTryLock(t, s, "k", testFingerprint, testTTL)
	mustComplete(t, s, "k", testFingerprint, sampleResponse(), testTTL, clk)
	rec, err := s.Get(ctx, "k")
	if err != nil || rec == nil || rec.State != idempotency.StateDone {
		t.Fatalf("after Complete: rec=%+v err=%v, want done record", rec, err)
	}
}

func testExpiredLock(t *testing.T, s idempotency.Store, clk *fakeClock) {
	mustTryLock(t, s, "k", testFingerprint, time.Minute)
	clk.Advance(time.Minute + time.Second)
	if got := mustTryLock(t, s, "k", testFingerprint, testTTL); !got {
		t.Fatal("TryLock after lock expiry returned false, want true")
	}
}

func testExpiredDone(t *testing.T, s idempotency.Store, clk *fakeClock) {
	ctx := context.Background()
	mustTryLock(t, s, "k", testFingerprint, testTTL)
	mustComplete(t, s, "k", testFingerprint, sampleResponse(), testTTL, clk)
	clk.Advance(testTTL + time.Second)

	rec, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec != nil {
		t.Fatalf("Get after expiry returned %+v, want nil", rec)
	}
	if got := mustTryLock(t, s, "k", testFingerprint, testTTL); !got {
		t.Fatal("TryLock after done-record expiry returned false, want true")
	}
}

func testLiveBeforeExpiry(t *testing.T, s idempotency.Store, clk *fakeClock) {
	ctx := context.Background()
	mustTryLock(t, s, "k", testFingerprint, testTTL)
	mustComplete(t, s, "k", testFingerprint, sampleResponse(), testTTL, clk)
	clk.Advance(testTTL - time.Second)

	rec, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec == nil {
		t.Fatal("record expired one second early")
	}
}

func testReleaseMissing(t *testing.T, s idempotency.Store, _ *fakeClock) {
	if err := s.Release(context.Background(), "never-locked"); err != nil {
		t.Fatalf("Release on missing key: %v", err)
	}
}

func testIndependentKeys(t *testing.T, s idempotency.Store, _ *fakeClock) {
	if !mustTryLock(t, s, "a", testFingerprint, testTTL) {
		t.Fatal("TryLock(a) returned false")
	}
	if !mustTryLock(t, s, "b", testFingerprint, testTTL) {
		t.Fatal("TryLock(b) returned false while a is locked")
	}
}

func testConcurrentTryLock(t *testing.T, s idempotency.Store, _ *fakeClock) {
	const workers = 32
	var (
		wg       sync.WaitGroup
		winners  atomic.Int32
		failures atomic.Int32
		start    = make(chan struct{})
	)
	ctx := context.Background()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := s.TryLock(ctx, "contended", testFingerprint, testTTL)
			if err != nil {
				failures.Add(1)
				return
			}
			if ok {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if failures.Load() != 0 {
		t.Fatalf("%d TryLock calls returned errors", failures.Load())
	}
	if winners.Load() != 1 {
		t.Fatalf("%d goroutines acquired the lock, want exactly 1", winners.Load())
	}
}

// mustTryLock calls TryLock and fails the test on error.
func mustTryLock(t *testing.T, s idempotency.Store, key, fingerprint string, ttl time.Duration) bool {
	t.Helper()
	ok, err := s.TryLock(context.Background(), key, fingerprint, ttl)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	return ok
}

// mustComplete calls Complete and fails the test on error.
func mustComplete(
	t *testing.T,
	s idempotency.Store,
	key, fingerprint string,
	resp idempotency.StoredResponse,
	ttl time.Duration,
	clk *fakeClock,
) {
	t.Helper()
	rec := idempotency.Record{
		Fingerprint: fingerprint,
		State:       idempotency.StateDone,
		Response:    resp,
		ExpiresAt:   clk.Now().Add(ttl),
	}
	if err := s.Complete(context.Background(), key, rec, ttl); err != nil {
		t.Fatalf("Complete: %v", err)
	}
}

func sampleResponse() idempotency.StoredResponse {
	return idempotency.StoredResponse{
		StatusCode: http.StatusCreated,
		Header:     http.Header{"Location": {"/orders/1"}},
		Body:       []byte(`{"id":1}`),
	}
}

// fakeClock is a manually advanced clock that is safe for concurrent use.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
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
