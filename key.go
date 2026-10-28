package idempotency

import "fmt"

const maxKeyLength = 255

// validateKey accepts 1 to 255 printable ASCII bytes (0x21-0x7E). Whitespace and control
// characters are rejected.
func validateKey(key string) error {
	if key == "" || len(key) > maxKeyLength {
		return fmt.Errorf("%w: must be 1-%d characters", ErrInvalidKey, maxKeyLength)
	}
	for i := 0; i < len(key); i++ {
		if c := key[i]; c < 0x21 || c > 0x7e {
			return fmt.Errorf("%w: must be printable ASCII without whitespace", ErrInvalidKey)
		}
	}
	return nil
}
