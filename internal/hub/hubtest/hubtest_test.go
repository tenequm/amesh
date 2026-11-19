package hubtest

import (
	"net/http"
	"testing"
)

func TestScript(t *testing.T) {
	const target = "/peer/messages"
	s := Script(t, Step{"POST", target, LostResponse}, Step{"POST", target, Reply(t, "errIdConflict")})
	for i := range 2 {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.URL+target, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Test-Index", string(rune('0'+i)))
		resp, err := s.Client().Do(req)
		if i == 0 {
			if err == nil {
				resp.Body.Close()
				t.Fatal("expected connection loss")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	calls := s.Calls()
	if len(calls) != 2 || calls[0].Header.Get("Test-Index") != "0" || calls[1].Target != target {
		t.Fatal("incorrect recording")
	}
}
