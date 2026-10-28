package idempotency_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	idempotency "github.com/yedoma-labs/yaarys-idempotency-go"
)

const testFP = "fp-order-1"

var errBoom = errors.New("boom")

// fixture bundles a Service with the fake clock and store it was built on.
type fixture struct {
	svc   *idempotency.Service
	clk   *fakeClock
	store *idempotency.MemoryStore
}

func newFixture(t *testing.T, opts ...idempotency.Option) fixture {
	t.Helper()
	clk := newFakeClock()
	store := idempotency.NewMemoryStore(clk.Now)
	all := append([]idempotency.Option{idempotency.WithClock(clk.Now)}, opts...)
	return fixture{svc: idempotency.New(store, all...), clk: clk, store: store}
}

// countingHandler returns a handler that counts its calls and answers 201 with body.
func countingHandler(calls *atomic.Int32, body string) func(context.Context) (idempotency.StoredResponse, error) {
	return func(context.Context) (idempotency.StoredResponse, error) {
		calls.Add(1)
		return idempotency.StoredResponse{
			StatusCode: http.StatusCreated,
			Header:     http.Header{"Location": {"/orders/1"}},
			Body:       []byte(body),
		}, nil
	}
}

func TestExecuteFirstRequestRunsHandler(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32

	res, err := f.svc.Execute(context.Background(), "key-1", testFP, countingHandler(&calls, `{"id":1}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", calls.Load())
	}
	if res.Replayed {
		t.Error("Replayed = true on first request, want false")
	}
	if res.Response.StatusCode != http.StatusCreated || string(res.Response.Body) != `{"id":1}` {
		t.Errorf("unexpected response: %+v", res.Response)
	}
}

func TestExecuteReplaysStoredResponseWithoutHandler(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	ctx := context.Background()

	first, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, `{"id":1}`))
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	// Changing the returned value must not change what is replayed later.
	first.Response.Body[0] = 'X'
	first.Response.Header.Set("Location", "/mutated")

	second, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "other"))
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1 (replay must not run the handler)", calls.Load())
	}
	if !second.Replayed {
		t.Error("Replayed = false on second request, want true")
	}
	if string(second.Response.Body) != `{"id":1}` || second.Response.Header.Get("Location") != "/orders/1" {
		t.Errorf("replayed response differs: body=%q location=%q",
			second.Response.Body, second.Response.Header.Get("Location"))
	}
}

func TestExecuteRejectsPayloadMismatch(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	ctx := context.Background()

	if _, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "a")); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	_, err := f.svc.Execute(ctx, "key-1", "fp-different", countingHandler(&calls, "b"))
	if !errors.Is(err, idempotency.ErrKeyReusedWithDifferentPayload) {
		t.Fatalf("err = %v, want ErrKeyReusedWithDifferentPayload", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", calls.Load())
	}
}

func TestExecuteRejectsSameKeyWhileInFlight(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	started := make(chan struct{})
	unblock := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := f.svc.Execute(ctx, "key-1", testFP, func(context.Context) (idempotency.StoredResponse, error) {
			close(started)
			<-unblock
			return idempotency.StoredResponse{StatusCode: http.StatusOK, Body: []byte("ok")}, nil
		})
		done <- err
	}()
	<-started

	_, err := f.svc.Execute(ctx, "key-1", testFP, func(context.Context) (idempotency.StoredResponse, error) {
		t.Error("handler must not run while the key is in flight")
		return idempotency.StoredResponse{}, nil
	})
	if !errors.Is(err, idempotency.ErrRequestInProgress) {
		t.Fatalf("err = %v, want ErrRequestInProgress", err)
	}

	_, err = f.svc.Execute(ctx, "key-1", "fp-different", func(context.Context) (idempotency.StoredResponse, error) {
		return idempotency.StoredResponse{}, nil
	})
	if !errors.Is(err, idempotency.ErrKeyReusedWithDifferentPayload) {
		t.Fatalf("in-flight with other payload: err = %v, want ErrKeyReusedWithDifferentPayload", err)
	}

	close(unblock)
	if err := <-done; err != nil {
		t.Fatalf("first Execute: %v", err)
	}
}

func TestExecuteConcurrentSameKeyRunsHandlerOnce(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	const workers = 50

	type outcome struct {
		res idempotency.Result
		err error
	}
	outcomes := make([]outcome, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := f.svc.Execute(context.Background(), "shared", testFP, func(context.Context) (idempotency.StoredResponse, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return idempotency.StoredResponse{StatusCode: http.StatusCreated, Body: []byte("once")}, nil
			})
			outcomes[i] = outcome{res: res, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want exactly 1", calls.Load())
	}
	executed := 0
	for i, o := range outcomes {
		switch {
		case o.err == nil && !o.res.Replayed:
			executed++
		case o.err == nil && o.res.Replayed:
			if string(o.res.Response.Body) != "once" {
				t.Errorf("worker %d replayed body %q, want once", i, o.res.Response.Body)
			}
		case errors.Is(o.err, idempotency.ErrRequestInProgress):
		default:
			t.Errorf("worker %d: unexpected error %v", i, o.err)
		}
	}
	if executed != 1 {
		t.Fatalf("executed results = %d, want 1", executed)
	}
}

func TestExecuteHandlerErrorIsNotCached(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	ctx := context.Background()

	_, err := f.svc.Execute(ctx, "key-1", testFP, func(context.Context) (idempotency.StoredResponse, error) {
		calls.Add(1)
		return idempotency.StoredResponse{}, errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}

	res, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if err != nil {
		t.Fatalf("retry Execute: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want 2 (failure must not be cached)", calls.Load())
	}
	if res.Replayed {
		t.Error("retry after failure reported Replayed=true")
	}
}

func TestExecuteHandlerPanicReleasesKey(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	func() {
		defer func() { _ = recover() }()
		_, _ = f.svc.Execute(ctx, "key-1", testFP, func(context.Context) (idempotency.StoredResponse, error) {
			panic("handler exploded")
		})
	}()

	var calls atomic.Int32
	if _, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok")); err != nil {
		t.Fatalf("Execute after panic: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls after panic = %d, want 1", calls.Load())
	}
}

func TestExecuteInvalidResponseIsNotCached(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	ctx := context.Background()

	for _, status := range []int{0, 99, 600} {
		_, err := f.svc.Execute(ctx, "key-1", testFP, func(context.Context) (idempotency.StoredResponse, error) {
			calls.Add(1)
			return idempotency.StoredResponse{StatusCode: status}, nil
		})
		if !errors.Is(err, idempotency.ErrInvalidResponse) {
			t.Fatalf("status %d: err = %v, want ErrInvalidResponse", status, err)
		}
	}

	res, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if err != nil {
		t.Fatalf("Execute with valid response: %v", err)
	}
	if calls.Load() != 4 || res.Replayed {
		t.Fatalf("calls=%d replayed=%v, want calls=4 replayed=false", calls.Load(), res.Replayed)
	}
}

func TestExecuteLockExpiresWithFakeClock(t *testing.T) {
	f := newFixture(t, idempotency.WithLockTTL(5*time.Second))
	ctx := context.Background()

	// Simulate a process that died while holding the key.
	if locked, err := f.store.TryLock(ctx, "key-1", testFP, 5*time.Second); err != nil || !locked {
		t.Fatalf("simulated lock: locked=%v err=%v", locked, err)
	}

	var calls atomic.Int32
	_, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if !errors.Is(err, idempotency.ErrRequestInProgress) {
		t.Fatalf("while lock is live: err = %v, want ErrRequestInProgress", err)
	}

	f.clk.Advance(4 * time.Second)
	_, err = f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if !errors.Is(err, idempotency.ErrRequestInProgress) {
		t.Fatalf("4s into 5s lock: err = %v, want ErrRequestInProgress", err)
	}

	f.clk.Advance(2 * time.Second)
	res, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if err != nil {
		t.Fatalf("after lock expiry: %v", err)
	}
	if calls.Load() != 1 || res.Replayed {
		t.Fatalf("calls=%d replayed=%v, want calls=1 replayed=false", calls.Load(), res.Replayed)
	}
}

func TestExecuteResponseExpiresAfterTTL(t *testing.T) {
	f := newFixture(t, idempotency.WithTTL(time.Hour))
	var calls atomic.Int32
	ctx := context.Background()

	if _, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok")); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	f.clk.Advance(time.Hour - time.Second)
	res, err := f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if err != nil || !res.Replayed {
		t.Fatalf("one second before TTL: replayed=%v err=%v, want replay", res.Replayed, err)
	}

	f.clk.Advance(2 * time.Second)
	res, err = f.svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if err != nil {
		t.Fatalf("after TTL: %v", err)
	}
	if res.Replayed || calls.Load() != 2 {
		t.Fatalf("after TTL: replayed=%v calls=%d, want executed with calls=2", res.Replayed, calls.Load())
	}
}

func TestExecuteKeyValidation(t *testing.T) {
	invalid := map[string]string{
		"empty":           "",
		"space":           " ",
		"inner space":     "a b",
		"tab":             "a\tb",
		"newline":         "a\nb",
		"nul":             "a\x00b",
		"delete":          "a\x7fb",
		"non-ascii":       "héllo",
		"256 characters":  strings.Repeat("a", 256),
		"trailing space":  "key ",
		"leading newline": "\nkey",
	}
	for name, key := range invalid {
		t.Run("invalid "+name, func(t *testing.T) {
			f := newFixture(t)
			var calls atomic.Int32
			_, err := f.svc.Execute(context.Background(), key, testFP, countingHandler(&calls, "ok"))
			if !errors.Is(err, idempotency.ErrInvalidKey) {
				t.Fatalf("err = %v, want ErrInvalidKey", err)
			}
			if calls.Load() != 0 {
				t.Fatal("handler ran for an invalid key")
			}
		})
	}

	valid := map[string]string{
		"single char":    "a",
		"255 characters": strings.Repeat("a", 255),
		"punctuation":    "order-42:abc_DEF.123~!",
	}
	for name, key := range valid {
		t.Run("valid "+name, func(t *testing.T) {
			f := newFixture(t)
			var calls atomic.Int32
			if _, err := f.svc.Execute(context.Background(), key, testFP, countingHandler(&calls, "ok")); err != nil {
				t.Fatalf("Execute: %v", err)
			}
		})
	}
}

func TestExecuteRejectsEmptyFingerprint(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	_, err := f.svc.Execute(context.Background(), "key-1", "", countingHandler(&calls, "ok"))
	if !errors.Is(err, idempotency.ErrInvalidFingerprint) {
		t.Fatalf("err = %v, want ErrInvalidFingerprint", err)
	}
}

func TestExecuteStoreGetErrorIsReturnedAndHandlerSkipped(t *testing.T) {
	clk := newFakeClock()
	errStore := errors.New("store offline")
	store := &wrappedStore{Store: idempotency.NewMemoryStore(clk.Now), getErr: errStore}
	svc := idempotency.New(store, idempotency.WithClock(clk.Now))

	var calls atomic.Int32
	_, err := svc.Execute(context.Background(), "key-1", testFP, countingHandler(&calls, "ok"))
	if !errors.Is(err, errStore) {
		t.Fatalf("err = %v, want wrapped store error", err)
	}
	if calls.Load() != 0 {
		t.Fatal("handler ran although the store failed")
	}
}

func TestExecuteCompleteErrorKeepsLock(t *testing.T) {
	clk := newFakeClock()
	errStore := errors.New("disk full")
	store := &wrappedStore{Store: idempotency.NewMemoryStore(clk.Now), completeErr: errStore}
	svc := idempotency.New(store, idempotency.WithClock(clk.Now), idempotency.WithLockTTL(time.Minute))
	ctx := context.Background()

	var calls atomic.Int32
	_, err := svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if !errors.Is(err, errStore) {
		t.Fatalf("err = %v, want wrapped complete error", err)
	}

	// The handler already ran, so the key must stay locked instead of being freed for a retry.
	_, err = svc.Execute(ctx, "key-1", testFP, countingHandler(&calls, "ok"))
	if !errors.Is(err, idempotency.ErrRequestInProgress) {
		t.Fatalf("retry after failed Complete: err = %v, want ErrRequestInProgress", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", calls.Load())
	}
}

func TestNewPanicsOnInvalidConfiguration(t *testing.T) {
	store := idempotency.NewMemoryStore(nil)
	cases := map[string]func(){
		"nil store":     func() { idempotency.New(nil) },
		"zero ttl":      func() { idempotency.New(store, idempotency.WithTTL(0)) },
		"negative lock": func() { idempotency.New(store, idempotency.WithLockTTL(-time.Second)) },
		"nil clock":     func() { idempotency.New(store, idempotency.WithClock(nil)) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}
}
