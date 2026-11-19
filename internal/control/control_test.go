package control

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tenequm/amesh/internal/registry"
)

func request(t *testing.T, query string) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/amesh/mcp?"+query, nil)
	r.Header.Set("Connection", "keep-alive, Upgrade")
	r.Header.Set("Upgrade", "mcp-stdio")
	return r
}

func TestUpgradeArgumentsAndRefusals(t *testing.T) {
	for _, q := range []string{"arg=--parnet", "reconnect=0", "reconnect=1&reconnect=1", "arg=--parent&arg=", "unknown=2"} {
		if _, _, err := mode(request(t, q)); err == nil {
			t.Fatal(q)
		}
	}
	parent, reconnect, err := mode(request(t, "arg=--parent&arg=a%2Bb&reconnect=1"))
	if err != nil || parent != "a+b" || !reconnect {
		t.Fatal(parent, reconnect, err)
	}
	d := daemon{registry: registry.New(nil)}
	w := httptest.NewRecorder()
	d.upgrade(w, request(t, ""))
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	if !excluded("/tmp/amesh-identity/sub", []string{"/tmp/amesh-identity"}) || excluded("/tmp/amesh-identity-other", []string{"/tmp/amesh-identity"}) {
		t.Fatal("directory boundary")
	}
}
