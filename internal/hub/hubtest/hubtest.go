// Package hubtest serves and records scripted peer requests using the contract examples.
package hubtest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"go.yaml.in/yaml/v3"
)

type fixture struct {
	Value  any    `yaml:"dataValue"`
	Stream string `yaml:"serializedValue"`
	Status int    `yaml:"x-amesh-status"`
}

var examples = sync.OnceValues(func() (map[string]fixture, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	for {
		body, readErr := os.ReadFile(filepath.Join(dir, "api", "openapi.yaml"))
		if readErr == nil {
			var doc struct {
				Components struct{ Examples map[string]fixture }
			}
			err = yaml.Unmarshal(body, &doc)
			return doc.Components.Examples, err
		}
		parent := filepath.Dir(dir)
		if !errors.Is(readErr, os.ErrNotExist) || parent == dir {
			return nil, readErr
		}
		dir = parent
	}
})

func load(tb testing.TB, name string) fixture {
	tb.Helper()
	all, err := examples()
	if err != nil {
		tb.Fatal(err)
	}
	f, ok := all[name]
	if !ok {
		tb.Fatalf("unknown contract example %q", name)
	}
	return f
}

// Example returns the exact stream bytes or the JSON value of a named example.
func Example(tb testing.TB, name string) []byte {
	tb.Helper()
	f := load(tb, name)
	if f.Stream != "" {
		return []byte(f.Stream)
	}
	b, err := json.Marshal(f.Value)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

func Reply(tb testing.TB, name string) http.HandlerFunc {
	tb.Helper()
	f := load(tb, name)
	body := Example(tb, name)
	status := f.Status
	if status == 0 {
		status = http.StatusOK
	}
	media := "application/json"
	if f.Stream != "" {
		media = "text/event-stream"
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", media)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

func LostResponse(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) }

// New serves method/path patterns. Unexpected requests fail the owning test.
func New(tb testing.TB, routes map[string]http.HandlerFunc) (*httptest.Server, error) {
	tb.Helper()
	mux := http.NewServeMux()
	for route, handler := range routes {
		mux.HandleFunc(route, handler)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern == "" {
			tb.Errorf("unscripted %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	tb.Cleanup(server.Close)
	return server, nil
}

type Step struct {
	Method, Target string
	Reply          http.HandlerFunc
}

type Call struct {
	Method, Target string
	Header         http.Header
	Body           []byte
}

type Recorder struct {
	*httptest.Server
	mu    sync.Mutex
	calls []Call
}

// Script consumes steps in order and checks for unused steps at cleanup.
func Script(tb testing.TB, steps ...Step) *Recorder {
	tb.Helper()
	r := new(Recorder)
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 256<<10))
		if err != nil {
			tb.Errorf("reading request: %v", err)
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		r.mu.Lock()
		i := len(r.calls)
		r.calls = append(r.calls, Call{req.Method, req.URL.RequestURI(), req.Header.Clone(), body})
		r.mu.Unlock()
		if i >= len(steps) || steps[i].Method != req.Method || steps[i].Target != req.URL.RequestURI() {
			tb.Errorf("unscripted request %d: %s %s", i+1, req.Method, req.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		steps[i].Reply(w, req)
	}))
	tb.Cleanup(func() {
		r.Close()
		if n := len(r.Calls()); n != len(steps) {
			tb.Errorf("recorded %d calls, expected %d", n, len(steps))
		}
	})
	return r
}

func (r *Recorder) Calls() []Call {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := slices.Clone(r.calls)
	for i := range calls {
		calls[i].Header = calls[i].Header.Clone()
		calls[i].Body = bytes.Clone(calls[i].Body)
	}
	return calls
}
