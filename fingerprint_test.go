package idempotency_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	idempotency "github.com/yedoma-labs/yaarys-idempotency-go"
)

func newPost(t *testing.T, target, contentType, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	return r
}

func mustFingerprint(t *testing.T, r *http.Request) string {
	t.Helper()
	fp, err := idempotency.FingerprintRequest(r)
	if err != nil {
		t.Fatalf("FingerprintRequest: %v", err)
	}
	return fp
}

func TestFingerprintIsStableForSameRequest(t *testing.T) {
	a := mustFingerprint(t, newPost(t, "/orders?draft=1", "application/json", `{"sku":"x"}`))
	b := mustFingerprint(t, newPost(t, "/orders?draft=1", "application/json", `{"sku":"x"}`))
	if a != b {
		t.Fatalf("identical requests produced different fingerprints: %s vs %s", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("fingerprint length = %d, want 64 hex characters", len(a))
	}
}

func TestFingerprintChangesWithAnyCoveredInput(t *testing.T) {
	base := mustFingerprint(t, newPost(t, "/orders?draft=1", "application/json", `{"sku":"x"}`))

	variants := map[string]*http.Request{
		"method":       httptest.NewRequest(http.MethodPut, "/orders?draft=1", strings.NewReader(`{"sku":"x"}`)),
		"path":         newPost(t, "/orders/2?draft=1", "application/json", `{"sku":"x"}`),
		"query":        newPost(t, "/orders?draft=2", "application/json", `{"sku":"x"}`),
		"content-type": newPost(t, "/orders?draft=1", "text/plain", `{"sku":"x"}`),
		"body":         newPost(t, "/orders?draft=1", "application/json", `{"sku":"y"}`),
	}
	for name, r := range variants {
		if got := mustFingerprint(t, r); got == base {
			t.Errorf("changing %s did not change the fingerprint", name)
		}
	}
}

func TestFingerprintIgnoresUncoveredHeaders(t *testing.T) {
	a := newPost(t, "/orders", "application/json", `{}`)
	b := newPost(t, "/orders", "application/json", `{}`)
	b.Header.Set("Idempotency-Key", "something-else")
	b.Header.Set("Authorization", "Bearer token")
	if mustFingerprint(t, a) != mustFingerprint(t, b) {
		t.Fatal("headers other than Content-Type changed the fingerprint")
	}
}

func TestFingerprintRestoresBody(t *testing.T) {
	const payload = `{"sku":"x","qty":2}`
	r := newPost(t, "/orders", "application/json", payload)

	mustFingerprint(t, r)

	got, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read restored body: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("restored body = %q, want %q", got, payload)
	}
}

func TestFingerprintHandlesMissingBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/orders", nil)
	withoutBody := mustFingerprint(t, r)

	nilBody := &http.Request{Method: http.MethodPost, Header: http.Header{}, URL: r.URL}
	if got := mustFingerprint(t, nilBody); got != withoutBody {
		t.Fatal("nil body and http.NoBody produced different fingerprints")
	}
}

func TestFingerprintRejectsNilRequest(t *testing.T) {
	if _, err := idempotency.FingerprintRequest(nil); err == nil {
		t.Fatal("expected error for nil request")
	}
}

func TestFingerprintBodyLimit(t *testing.T) {
	t.Run("body at limit is accepted", func(t *testing.T) {
		r := newPost(t, "/orders", "", strings.Repeat("a", 10))
		if _, err := idempotency.FingerprintRequestWithLimit(r, 10); err != nil {
			t.Fatalf("body at limit: %v", err)
		}
	})

	t.Run("body over limit returns ErrBodyTooLarge and keeps the body", func(t *testing.T) {
		const payload = "01234567890"
		r := newPost(t, "/orders", "", payload)
		_, err := idempotency.FingerprintRequestWithLimit(r, 10)
		if !errors.Is(err, idempotency.ErrBodyTooLarge) {
			t.Fatalf("err = %v, want ErrBodyTooLarge", err)
		}
		got, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Fatalf("read body after limit error: %v", readErr)
		}
		if string(got) != payload {
			t.Fatalf("body after limit error = %q, want %q", got, payload)
		}
	})

	t.Run("default limit is 1 MiB", func(t *testing.T) {
		over := bytes.Repeat([]byte("a"), int(idempotency.DefaultMaxBodyBytes)+1)
		r := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(over))
		if _, err := idempotency.FingerprintRequest(r); !errors.Is(err, idempotency.ErrBodyTooLarge) {
			t.Fatalf("err = %v, want ErrBodyTooLarge", err)
		}
	})

	t.Run("non-positive limit is rejected", func(t *testing.T) {
		r := newPost(t, "/orders", "", "x")
		if _, err := idempotency.FingerprintRequestWithLimit(r, 0); err == nil {
			t.Fatal("expected error for zero limit")
		}
	})
}
