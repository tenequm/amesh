package control

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/mcpserver"

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

func TestReconnectBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var times []time.Duration
		start := time.Now()
		_, err := redial(t.Context(), func(context.Context) (*bufferedConn, error) {
			times = append(times, time.Since(start))
			return nil, os.ErrNotExist
		})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 30*time.Second || len(times) < 5 || times[0] != 250*time.Millisecond || times[1] != 750*time.Millisecond || times[4]-times[3] != 2*time.Second {
			t.Fatal(times, err)
		}
	})
}

func TestPipeForwardsArgvAndBytes(t *testing.T) {
	t.Chdir(t.TempDir())
	socket := "amesh-identity.sock"
	ln, e := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if e != nil {
		t.Fatal(e)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() {
		c, err := ln.AcceptUnix()
		if err != nil {
			done <- err
			return
		}
		defer c.Close()
		reader := bufio.NewReader(c)
		req, err := http.ReadRequest(reader)
		if err != nil {
			done <- err
			return
		}
		if !slices.Equal(req.URL.Query()["arg"], []string{"--parent", "x +&/"}) {
			done <- errors.New("changed argv")
			return
		}
		_, err = c.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: mcp-stdio\r\n\r\nearly\n"))
		if err == nil {
			_, err = io.Copy(c, reader)
		}
		done <- err
	}()
	var out bytes.Buffer
	err := Pipe(t.Context(), socket, []string{"--parent", "x +&/"}, strings.NewReader("one\x00two\n"), &out)
	if err != nil || out.String() != "early\none\x00two\n" {
		t.Fatal(out.String(), err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

type humanBackend struct {
	mcpserver.MeshBackend
	human chan registry.Session
}

func (b humanBackend) HumanTurn(_ context.Context, s registry.Session) error {
	b.human <- s
	return nil
}

func TestHookGuardAndHumanTurns(t *testing.T) {
	key := registry.Key{PID: 123, ProcStart: "start"}
	s := registry.Session{Key: key, SessionID: "resolved"}
	backend := humanBackend{human: make(chan registry.Session, 1)}
	reg := &registration{key: s.Key}
	d := daemon{ctx: t.Context(), mesh: backend, pipes: map[*registration]struct{}{reg: {}}}
	call := func(input string, code int) {
		t.Helper()
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/amesh/hooks", strings.NewReader(input))
		w := httptest.NewRecorder()
		d.applyHook(w, r, s)
		if w.Code != code || (code == 200 && w.Body.String() != denySubagent) || ((code == 204 || code == 202) && w.Body.Len() != 0) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, tool := range []string{"list_peers", "send_message", "send_to_parent"} {
		call(fmt.Sprintf(`{"hook_event_name":"PreToolUse","session_id":"claimed","tool_name":"mcp__amesh__%s","agent_id":""}`, tool), 200)
	}
	call(`{"hook_event_name":"PreToolUse","session_id":"claimed","tool_name":"mcp__amesh__list_peers"}`, 204)
	prompt := func(p string) string {
		return fmt.Sprintf(`{"hook_event_name":"UserPromptSubmit","session_id":"claimed","prompt":%q}`, p)
	}
	for _, p := range []string{"<task-notification>", "<agent-message from", "<cross-session-message from"} {
		call(prompt(p), 204)
	}
	call(prompt("hello"), 202)
	d.work.Wait()
	if got := <-backend.human; got.SessionID != "resolved" {
		t.Fatal(got)
	}
	reg.parent = "parent"
	call(prompt("hello"), 204)
	delete(d.pipes, reg)
	call(prompt("hello"), 204)
	call("{}", 400)
}
