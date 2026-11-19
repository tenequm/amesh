package relaytest

import (
	"errors"
	"testing"
)

// Inbox returns a socket path and the individual lines received there.
func Inbox(tb testing.TB) (string, <-chan []byte, error) {
	tb.Helper()
	return "", nil, errors.ErrUnsupported
}
