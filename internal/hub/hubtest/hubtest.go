package hubtest

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// New serves the scripted method/path patterns and reports any unscripted request to t.
func New(tb testing.TB, routes map[string]http.HandlerFunc) (*httptest.Server, error) {
	tb.Helper()
	return nil, errors.ErrUnsupported
}
