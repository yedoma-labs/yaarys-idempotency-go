package idempotency_test

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	idempotency "github.com/yedoma-labs/yaarys-idempotency-go"
)

// newTestHandler wraps h with the idempotency middleware on a memory store.
func newTestHandler(t *testing.T, h http.Handler, opts ...idempotency.MiddlewareOption) http.Handler {
	t.Helper()
	clk := newFakeClock()
	svc := idempotency.New(idempotency.NewMemoryStore(clk.Now), idempotency.WithClock(clk.Now))
	return idempotency.Middleware(svc, opts...)(h)
}

func serve(h http.Handler, method, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/orders", strings.NewReader(body))
	if key != "" {
		req.Header.Set(idempotency.DefaultHeader, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// createOrder answers 201 with a Location header and a body that includes the call number.
func createOrder(calls *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Location", "/orders/1")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, "created #%d", n)
	})
}

func TestMiddlewareExecutesThenReplays(t *testing.T) {
	var calls atomic.Int32
	h := newTestHandler(t, createOrder(&calls))

	first := serve(h, http.MethodPost, "key-1", `{"sku":"x"}`)
	if first.Code != http.StatusCreated || first.Body.String() != "created #1" {
		t.Fatalf("first: status=%d body=%q", first.Code, first.Body.String())
	}
	if first.Header().Get(idempotency.ReplayHeader) != "" {
		t.Error("first response must not carry the replay header")
	}

	second := serve(h, http.MethodPost, "key-1", `{"sku":"x"}`)
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", calls.Load())
	}
	if second.Code != http.StatusCreated {
		t.Fatalf("replay status = %d, want 201", second.Code)
	}
	if second.Body.String() != "created #1" {
		t.Fatalf("replay body = %q, want stored body", second.Body.String())
	}
	if second.Header().Get("Location") != "/orders/1" {
		t.Errorf("replay Location = %q, want /orders/1", second.Header().Get("Location"))
	}
	if second.Header().Get(idempotency.ReplayHeader) != "true" {
		t.Errorf("%s = %q, want true", idempotency.ReplayHeader, second.Header().Get(idempotency.ReplayHeader))
	}
}

func TestMiddlewareReplaysForPutAndPatch(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			h := newTestHandler(t, createOrder(&calls))

			serve(h, method, "key-1", "body")
			second := serve(h, method, "key-1", "body")
			if calls.Load() != 1 || second.Header().Get(idempotency.ReplayHeader) != "true" {
				t.Fatalf("calls=%d replay=%q, want calls=1 and replay header",
					calls.Load(), second.Header().Get(idempotency.ReplayHeader))
			}
		})
	}
}

func TestMiddlewarePassesThroughSafeMethods(t *testing.T) {
	var calls atomic.Int32
	h := newTestHandler(t, createOrder(&calls))

	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		rec := serve(h, method, "", "")
		if rec.Code != http.StatusCreated {
			t.Fatalf("%s: status = %d, want handler status", method, rec.Code)
		}
		if rec.Header().Get(idempotency.ReplayHeader) != "" {
			t.Fatalf("%s: replay header set on pass-through", method)
		}
	}
	serve(h, http.MethodGet, "same-key", "")
	serve(h, http.MethodGet, "same-key", "")
	if calls.Load() != 4 {
		t.Fatalf("handler calls = %d, want 4 (pass-through must not be deduplicated)", calls.Load())
	}
}

func TestMiddlewareStatusMapping(t *testing.T) {
	t.Run("missing key is 400", func(t *testing.T) {
		var calls atomic.Int32
		h := newTestHandler(t, createOrder(&calls))
		rec := serve(h, http.MethodPost, "", "body")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
		if calls.Load() != 0 {
			t.Fatal("handler ran without a valid key")
		}
	})

	t.Run("key with whitespace is 400", func(t *testing.T) {
		var calls atomic.Int32
		h := newTestHandler(t, createOrder(&calls))
		if rec := serve(h, http.MethodPost, "bad key", "body"); rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("reused key with different payload is 422", func(t *testing.T) {
		var calls atomic.Int32
		h := newTestHandler(t, createOrder(&calls))
		serve(h, http.MethodPost, "key-1", "payload-a")
		rec := serve(h, http.MethodPost, "key-1", "payload-b")
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", rec.Code)
		}
		if calls.Load() != 1 {
			t.Fatalf("handler calls = %d, want 1", calls.Load())
		}
	})

	t.Run("concurrent request with same key is 409", func(t *testing.T) {
		started := make(chan struct{})
		unblock := make(chan struct{})
		done := make(chan struct{})
		h := newTestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-unblock
			w.WriteHeader(http.StatusCreated)
		}))

		go func() {
			defer close(done)
			serve(h, http.MethodPost, "key-1", "body")
		}()
		<-started

		rec := serve(h, http.MethodPost, "key-1", "body")
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409", rec.Code)
		}
		close(unblock)
		<-done
	})

	t.Run("body over limit is 413", func(t *testing.T) {
		var calls atomic.Int32
		h := newTestHandler(t, createOrder(&calls), idempotency.WithMaxBodyBytes(8))
		rec := serve(h, http.MethodPost, "key-1", "123456789")
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", rec.Code)
		}
		if calls.Load() != 0 {
			t.Fatal("handler ran for an oversized body")
		}
	})

	t.Run("store failure is 500 without internal detail", func(t *testing.T) {
		const secret = "10.0.0.5:6379 connection refused"
		clk := newFakeClock()
		store := &wrappedStore{Store: idempotency.NewMemoryStore(clk.Now), lockErr: errors.New(secret)}
		svc := idempotency.New(store, idempotency.WithClock(clk.Now))
		var calls atomic.Int32
		h := idempotency.Middleware(svc)(createOrder(&calls))

		rec := serve(h, http.MethodPost, "key-1", "body")
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", rec.Code)
		}
		if strings.Contains(rec.Body.String(), "10.0.0.5") || strings.Contains(rec.Body.String(), "refused") {
			t.Fatalf("500 body leaks internal detail: %q", rec.Body.String())
		}
		if calls.Load() != 0 {
			t.Fatal("handler ran although the lock failed")
		}
	})
}

func TestMiddlewareInvalidHandlerStatusIsNotCached(t *testing.T) {
	var calls atomic.Int32
	h := newTestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(99)
	}))

	if rec := serve(h, http.MethodPost, "key-1", "body"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	serve(h, http.MethodPost, "key-1", "body")
	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want 2 (invalid response must not be cached)", calls.Load())
	}
}

func TestMiddlewareHandlerReadsFullBodyOnFirstAndReplayedCall(t *testing.T) {
	var calls atomic.Int32
	h := newTestHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		_, _ = w.Write(data)
	}))

	const payload = `{"sku":"x","qty":2}`
	first := serve(h, http.MethodPost, "key-1", payload)
	if first.Body.String() != payload {
		t.Fatalf("first echo = %q, want %q", first.Body.String(), payload)
	}
	second := serve(h, http.MethodPost, "key-1", payload)
	if second.Body.String() != payload || calls.Load() != 1 {
		t.Fatalf("replay echo = %q calls=%d, want stored payload and 1 call", second.Body.String(), calls.Load())
	}
}

func TestMiddlewareCustomHeader(t *testing.T) {
	var calls atomic.Int32
	h := newTestHandler(t, createOrder(&calls), idempotency.WithHeader("X-Request-Key"))

	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader("body"))
	req.Header.Set(idempotency.DefaultHeader, "ignored")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("default header only: status = %d, want 400", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader("body"))
	req.Header.Set("X-Request-Key", "key-1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("custom header: status = %d, want 201", rec.Code)
	}
}
