# yaarys-idempotency-go

Idempotency keys for HTTP `POST`, `PUT` and `PATCH` requests in Go. Standard library only.

## The problem

Clients retry requests after timeouts, and users double-click buttons. Without protection, a
retried `POST /payments` charges the customer twice and a retried `POST /orders` creates two
orders. An idempotency key fixes this: the client sends the same key with each attempt, and the
server runs the operation once and returns the stored result for every further attempt.

This library provides:

- **Replay.** The first request with a key runs the handler and stores the response. Later
  requests with the same key and the same payload get the stored response. The handler does not
  run again.
- **Payload check.** The same key with a different payload is rejected, because it is a client bug.
- **In-flight lock.** Concurrent requests with the same key do not run in parallel. One runs, the
  others get `ErrRequestInProgress` (HTTP 409) or the stored response once the first one finishes.
- **Pluggable storage.** The core depends only on a small `Store` interface. `MemoryStore` is
  included. Redis and SQL stores are yours to write, and a contract suite checks them.
- **net/http middleware.** Wrap any `http.Handler`. Errors are mapped to status codes.

## Installation

```sh
go get github.com/yedoma-labs/yaarys-idempotency-go
```

Requires Go 1.23 or newer.

## Quickstart with net/http

```go
package main

import (
	"log"
	"net/http"
	"time"

	idempotency "github.com/yedoma-labs/yaarys-idempotency-go"
)

func main() {
	// In-process store: fine for one instance. See "Memory store" below before using it in production.
	store := idempotency.NewMemoryStore(time.Now)
	svc := idempotency.New(store)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"ord_1"}`))
	})

	handler := idempotency.Middleware(svc)(mux)
	log.Fatal(http.ListenAndServe(":8080", handler))
}
```

The client sends the key on every attempt:

```sh
curl -i -X POST localhost:8080/orders \
  -H 'Idempotency-Key: 7c9e6679-order-42' \
  -H 'Content-Type: application/json' \
  -d '{"sku":"A-1","qty":2}'
```

A retry with the same key and body returns `201` with the same body and the header
`X-Idempotent-Replay: true`, and the handler does not run.

### Middleware behavior

- Only `POST`, `PUT` and `PATCH` are affected. Other methods pass through.
- The key is read from `Idempotency-Key` by default. Use `WithHeader` to change it.
- For mutating requests the key is required. A missing or invalid key returns `400`.
- Request bodies are hashed up to `WithMaxBodyBytes` (default 1 MiB). Larger bodies return `413`.

| Condition                                | Status |
|------------------------------------------|--------|
| `ErrInvalidKey`                          | 400    |
| `ErrKeyReusedWithDifferentPayload`       | 422    |
| `ErrRequestInProgress`                   | 409    |
| `ErrBodyTooLarge`                        | 413    |
| Anything else (including store failures) | 500, generic message only |

### Using the service without the middleware

```go
res, err := svc.Execute(ctx, key, fingerprint, func(ctx context.Context) (idempotency.StoredResponse, error) {
	id, err := createPayment(ctx, req) // your side effect
	if err != nil {
		return idempotency.StoredResponse{}, err // not cached, the client may retry
	}
	return idempotency.StoredResponse{
		StatusCode: http.StatusCreated,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       []byte(`{"id":"` + id + `"}`),
	}, nil
})
switch {
case errors.Is(err, idempotency.ErrRequestInProgress):
	// 409
case errors.Is(err, idempotency.ErrKeyReusedWithDifferentPayload):
	// 422
case err != nil:
	// 400 for ErrInvalidKey, 500 otherwise
default:
	// res.Replayed tells you whether the stored response was returned
}
```

Use `idempotency.FingerprintRequest(r)` to compute the fingerprint for an `*http.Request`. It
covers method, request URI with query, `Content-Type` and body. Other headers are not included.

## Store interface

```go
type Store interface {
	Get(ctx context.Context, key string) (*Record, error)
	TryLock(ctx context.Context, key, fingerprint string, ttl time.Duration) (bool, error)
	Complete(ctx context.Context, key string, rec Record, ttl time.Duration) error
	Release(ctx context.Context, key string) error
}
```

- `Get` returns `(nil, nil)` for a missing or expired key.
- `TryLock` must be **atomic**. It succeeds only if the key has no live record. This is what
  prevents two concurrent requests from both running the handler.
- `Complete` stores the response as a `Done` record that expires after `ttl`.
- `Release` removes a `Processing` lock. It must never remove a `Done` record.

Stores decide expiry with their own clock. `storetest.RunStoreContract` checks the behavior the
service relies on. Run it against every store implementation:

```go
func TestRedisStoreContract(t *testing.T) {
	storetest.RunStoreContract(t, func(now func() time.Time) idempotency.Store {
		return newRedisStore(t, now) // a fresh, empty store that uses the given clock
	})
}
```

## Memory store: single process only

`MemoryStore` keeps all state in one process. It is not shared across instances, restarts or
workers. With two replicas behind a load balancer, a retry that reaches the other replica runs
the handler a second time, which is exactly the bug this library exists to prevent. Use
`MemoryStore` for tests, local development and single-instance services only.

## Redis and SQL stores

A production store must be atomic in two places:

- **Redis:** `TryLock` is `SET key value NX PX <lockTTL>`. `NX` makes it succeed only if the key
  is absent, and `PX` makes the lock expire if the process dies. `Complete` overwrites the key
  with the response and a new `PX <ttl>`. `Release` deletes the key only while it still holds a
  processing marker, for example with a Lua script or `WATCH`/`MULTI`.
- **SQL:** `TryLock` is an `INSERT` into a table with a unique primary key on the key. A duplicate
  key error means the lock is taken. Expired rows must count as absent, so either delete them
  inside the same transaction or treat them as missing in the `WHERE` clause. `Complete` and
  `Release` run in a transaction that checks the current state.

## Comparison with the TypeScript package

This library follows the semantics of `@yedoma-labs/yaarys-idempotency`:

- The same three error conditions, the same decision order (fingerprint is compared before state),
  the same release behavior on handler errors, and the same key rules (1-255 printable ASCII
  characters, no whitespace).
- Expired records and expired locks count as missing in both.

Differences:

- Go errors are sentinel values checked with `errors.Is`. There are no error codes or HTTP
  status fields on the error values. The HTTP mapping lives in the middleware.
- The Go `Execute` result has `Replayed bool` instead of a status string.
- Store methods take a `context.Context`. TTLs are `time.Duration` instead of seconds.
- Response headers are `http.Header` and bodies are `[]byte` instead of `Record<string, string>`
  and `string`.
- The Go fingerprint also includes `Content-Type`, so fingerprints are not comparable with the
  TypeScript implementation.
- The Go package includes a net/http middleware. The TypeScript package does not.

## Limitations

- A completed response is buffered in memory before it is sent. Very large responses use
  proportional memory.
- The lock TTL (default 30 seconds) must be longer than the slowest handler. A longer handler
  lets a second request start after the lock expires.
- If storing the response fails after the handler succeeded, the key stays locked until the lock
  TTL expires. Retries get `ErrRequestInProgress` until then. This avoids running side effects twice.
- Keys are not logged by this library. Keep it that way in your own logging.

## License

MIT. See [LICENSE](LICENSE).
