package idempotency

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
)

// DefaultMaxBodyBytes is the largest request body FingerprintRequest reads. Larger bodies
// return ErrBodyTooLarge.
const DefaultMaxBodyBytes int64 = 1 << 20

// FingerprintRequest returns the SHA-256 fingerprint of r as lowercase hex. It uses
// DefaultMaxBodyBytes as the body limit. See FingerprintRequestWithLimit.
func FingerprintRequest(r *http.Request) (string, error) {
	return FingerprintRequestWithLimit(r, DefaultMaxBodyBytes)
}

// FingerprintRequestWithLimit returns the SHA-256 fingerprint of r as lowercase hex.
//
// The fingerprint covers the method, the request URI including the query string, the
// Content-Type header and the body. Each field is length-prefixed, so different requests cannot
// produce the same input. Other headers are not included.
//
// The body is read and then restored on r as an io.NopCloser over the same bytes, so the handler
// can read it again. If the body is larger than maxBodyBytes, ErrBodyTooLarge is returned and
// the body is restored in full, including the part that was already read.
func FingerprintRequestWithLimit(r *http.Request, maxBodyBytes int64) (string, error) {
	if r == nil {
		return "", errors.New("idempotency: request must not be nil")
	}
	if maxBodyBytes <= 0 {
		return "", errors.New("idempotency: maxBodyBytes must be positive")
	}

	body, err := readAndRestoreBody(r, maxBodyBytes)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	writeField(h, []byte(r.Method))
	writeField(h, []byte(requestURI(r)))
	writeField(h, []byte(r.Header.Get("Content-Type")))
	writeField(h, body)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func requestURI(r *http.Request) string {
	if r.URL == nil {
		return ""
	}
	return r.URL.RequestURI()
}

// writeField writes an 8-byte big-endian length followed by the field bytes.
func writeField(h hash.Hash, field []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(field)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(field)
}

// readAndRestoreBody reads at most maxBytes+1 bytes to detect oversize bodies without reading
// an unbounded amount of data. It replaces r.Body so that it can be read again.
func readAndRestoreBody(r *http.Request, maxBytes int64) ([]byte, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}

	original := r.Body
	buf, err := io.ReadAll(io.LimitReader(original, maxBytes+1))
	if err != nil {
		r.Body = restoredBody{Reader: io.MultiReader(bytes.NewReader(buf), original), Closer: original}
		return nil, fmt.Errorf("idempotency: read request body: %w", err)
	}
	if int64(len(buf)) > maxBytes {
		r.Body = restoredBody{Reader: io.MultiReader(bytes.NewReader(buf), original), Closer: original}
		return nil, ErrBodyTooLarge
	}

	r.Body = io.NopCloser(bytes.NewReader(buf))
	_ = original.Close()
	return buf, nil
}

// restoredBody replays the bytes that were already read, then the rest of the original body.
type restoredBody struct {
	io.Reader
	io.Closer
}
