package idempotency

import (
	"context"
	"errors"
	"net/http"
)

const (
	// DefaultHeader is the request header that carries the idempotency key.
	DefaultHeader = "Idempotency-Key"
	// ReplayHeader is set to "true" on responses that were replayed from the store.
	ReplayHeader = "X-Idempotent-Replay"
)

// MiddlewareOption configures the middleware returned by Middleware.
type MiddlewareOption func(*middlewareConfig)

type middlewareConfig struct {
	header       string
	maxBodyBytes int64
}

// WithHeader sets the request header that carries the key. The default is Idempotency-Key.
// It panics if name is empty.
func WithHeader(name string) MiddlewareOption {
	return func(c *middlewareConfig) {
		if name == "" {
			panic("idempotency: WithHeader requires a non-empty header name")
		}
		c.header = name
	}
}

// WithMaxBodyBytes sets the largest request body that is hashed for the fingerprint. Larger
// bodies are rejected with 413. The default is DefaultMaxBodyBytes (1 MiB). It panics if n is
// not positive.
func WithMaxBodyBytes(n int64) MiddlewareOption {
	return func(c *middlewareConfig) {
		if n <= 0 {
			panic("idempotency: WithMaxBodyBytes requires a positive limit")
		}
		c.maxBodyBytes = n
	}
}

// Middleware returns net/http middleware that makes POST, PUT and PATCH requests idempotent.
// Other methods pass through unchanged.
//
// For mutating requests the key must be present and valid, otherwise the response is 400. The
// middleware fingerprints the request, runs the next handler at most once per key, and replays
// the stored response on later requests. A replayed response has the header X-Idempotent-Replay:
// true.
//
// Errors are mapped to status codes as follows:
//
//	ErrInvalidKey                    400
//	ErrKeyReusedWithDifferentPayload 422
//	ErrRequestInProgress             409
//	ErrBodyTooLarge                  413
//	anything else                    500 with a generic message
//
// The handler's response is buffered in memory before it is sent, so very large responses use
// proportional memory. Hijacking and streaming are not supported for mutating routes.
func Middleware(svc *Service, opts ...MiddlewareOption) func(http.Handler) http.Handler {
	if svc == nil {
		panic("idempotency: Middleware requires a non-nil Service")
	}
	cfg := middlewareConfig{header: DefaultHeader, maxBodyBytes: DefaultMaxBodyBytes}
	for _, opt := range opts {
		opt(&cfg)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !isMutatingMethod(r.Method) {
				next.ServeHTTP(w, r)
				return
			}

			fingerprint, err := FingerprintRequestWithLimit(r, cfg.maxBodyBytes)
			if err != nil {
				writeFailure(w, err)
				return
			}

			key := r.Header.Get(cfg.header)
			result, err := svc.Execute(r.Context(), key, fingerprint, func(ctx context.Context) (StoredResponse, error) {
				rec := newResponseRecorder()
				next.ServeHTTP(rec, r.WithContext(ctx))
				return rec.response(), nil
			})
			if err != nil {
				writeFailure(w, err)
				return
			}
			writeStoredResponse(w, result.Response, result.Replayed)
		})
	}
}

func isMutatingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return true
	default:
		return false
	}
}

// writeStoredResponse sends a captured response, either the one just produced or a replay.
func writeStoredResponse(w http.ResponseWriter, resp StoredResponse, replayed bool) {
	for name, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(name, v)
		}
	}
	if replayed {
		w.Header().Set(ReplayHeader, "true")
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(resp.Body)
}

// writeFailure maps idempotency errors to HTTP responses. Internal error details are never sent
// to the client.
func writeFailure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidKey):
		http.Error(w, "invalid idempotency key", http.StatusBadRequest)
	case errors.Is(err, ErrKeyReusedWithDifferentPayload):
		http.Error(w, "idempotency key was already used with a different request", http.StatusUnprocessableEntity)
	case errors.Is(err, ErrRequestInProgress):
		http.Error(w, "a request with this idempotency key is already in progress", http.StatusConflict)
	case errors.Is(err, ErrBodyTooLarge):
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
	default:
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}
