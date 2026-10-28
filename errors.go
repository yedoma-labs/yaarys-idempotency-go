package idempotency

import "errors"

var (
	// ErrInvalidKey means the idempotency key is empty, longer than 255 bytes, or contains
	// characters outside printable ASCII without whitespace.
	ErrInvalidKey = errors.New("idempotency: invalid key")

	// ErrKeyReusedWithDifferentPayload means the key was already used with a different request
	// fingerprint.
	ErrKeyReusedWithDifferentPayload = errors.New("idempotency: key reused with different payload")

	// ErrRequestInProgress means a request with the same key and fingerprint is still running.
	ErrRequestInProgress = errors.New("idempotency: request with this key is in progress")

	// ErrBodyTooLarge means the request body exceeds the configured fingerprint limit.
	ErrBodyTooLarge = errors.New("idempotency: request body too large")

	// ErrInvalidFingerprint means an empty fingerprint was passed to Execute.
	ErrInvalidFingerprint = errors.New("idempotency: fingerprint must not be empty")

	// ErrInvalidResponse means the handler returned a response that cannot be stored, such as a
	// status code outside 100-599. Nothing is stored in this case.
	ErrInvalidResponse = errors.New("idempotency: invalid handler response")
)
