package control

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub/hubtest"

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
	if !(registry.Session{CWD: "/tmp/amesh-identity/sub"}).MeshExcluded(nil, []string{"/tmp/amesh-identity"}) || (registry.Session{CWD: "/tmp/amesh-identity-other"}).MeshExcluded(nil, []string{"/tmp/amesh-identity"}) {
		t.Fatal("directory boundary")
	}
	dir, key := liveEntry(t)
	d.cfg.ClaudeConfigDirs = []string{filepath.Join(dir, "primary"), dir}
	d.registry = registry.New(d.cfg.ClaudeConfigDirs)
	r := request(t, "")
	w = httptest.NewRecorder()
	d.upgrade(w, r.WithContext(context.WithValue(r.Context(), peerPID{}, key.PID)))
	var refusal localError
	if err := json.Unmarshal(w.Body.Bytes(), &refusal); err != nil || w.Code != http.StatusForbidden || refusal.Code != "excluded" {
		t.Fatal(w.Code, refusal, err)
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
	human  chan registry.Session
	member registry.Key
}

func (b humanBackend) Status(s *contract.DaemonStatus) error {
	s.Sessions = append(s.Sessions, contract.DaemonStatusSessionsItem{Key: sessionKey(b.member), Address: "amesh-a2@project:mac", Role: "member"})
	return nil
}

func (b humanBackend) IsMember(key registry.Key) bool { return b.member == key }

func (humanBackend) OwnsRelay(addr string) bool { return addr == "uds:/tmp/amesh-identity-relay.sock" }

func (b humanBackend) HumanTurn(_ context.Context, s registry.Session) error {
	b.human <- s
	return nil
}

func TestHookGuardAndHumanTurns(t *testing.T) {
	dir, key := liveEntry(t)
	sessions := registry.New([]string{dir})
	s, err := sessions.Resolve(t.Context(), key.PID)
	if err != nil {
		t.Fatal(err)
	}
	backend := humanBackend{human: make(chan registry.Session, 1), member: key}
	d := daemon{ctx: t.Context(), mesh: &backend, ownsRelay: backend.OwnsRelay, registry: sessions}
	if s.FromMode != "" {
		t.Fatal("classified before first hook")
	}
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
	if len(backend.human) != 0 {
		t.Fatal("reported without transcript")
	}
	transcript := filepath.Join(dir, "projects", "amesh-identity", "id.jsonl")
	if e := os.MkdirAll(filepath.Dir(transcript), 0o700); e != nil {
		t.Fatal(e)
	}
	row := `{"type":"user","sessionId":"id","promptId":"candidate","message":{"content":"hello"},"origin":{"kind":"human"}}` + "\n"
	if e := os.WriteFile(transcript, []byte(row), 0o600); e != nil {
		t.Fatal(e)
	}
	call(fmt.Sprintf(`{"session_id":"claimed","hook_event_name":"UserPromptSubmit","prompt":"hello","prompt_id":"candidate","transcript_path":%q,"permission_mode":"bypassPermissions"}`, transcript), 202)
	file, err := os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString(row); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	d.work.Wait()
	if got := <-backend.human; got.SessionID != "id" || got.FromMode != "bypass" {
		t.Fatal(got)
	}
	call(`{"hook_event_name":"PreToolUse","session_id":"id","tool_name":"SendMessage","agent_id":"sub","tool_input":{"to":"uds:/tmp/amesh-identity-relay.sock"}}`, 200)
	call(`{"hook_event_name":"PreToolUse","session_id":"id","tool_name":"SendMessage","agent_id":"sub","tool_input":{"to":"uds:/tmp/amesh-identity-other.sock"}}`, 204)
	call(`{"hook_event_name":"PreToolUse","session_id":"id","tool_name":"SendMessage","tool_input":{"to":"uds:/tmp/amesh-identity-relay.sock"}}`, 204)
	if current, err := sessions.Resolve(t.Context(), key.PID); err != nil || current.FromMode != "prompting" {
		t.Fatal("hook without bypass permission retained elevated class", current, err)
	}
	call(`{"hook_event_name":"PreToolUse","session_id":"id","tool_name":"SendMessage","agent_id":"sub","permission_mode":"bypassPermissions"}`, 204)
	if current, err := sessions.Resolve(t.Context(), key.PID); err != nil || current.FromMode != "prompting" {
		t.Fatal(current, err)
	}
	d.cfg.ClaudeConfigDirs = []string{dir + "/other", dir}
	call(prompt("hello"), 204)
	d.cfg.ClaudeConfigDirs = []string{dir}
	backend.member = registry.Key{}
	d.pipes = map[*registration]struct{}{{key: key, parent: "parent"}: {}}
	call(prompt("hello"), 204)
	clear(d.pipes)
	call(prompt("hello"), 204)
	call("{}", 400)
}

func liveEntry(t *testing.T) (string, registry.Key) {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	pid := os.Getpid()
	var start string
	if runtime.GOOS == "darwin" {
		cmd := exec.CommandContext(ctx, "ps", "-o", "lstart=", "-p", strconv.Itoa(pid))
		cmd.Env = append(os.Environ(), "LC_ALL=C", "TZ=UTC")
		out, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		start = strings.TrimSpace(string(out))
	} else {
		b, err := os.ReadFile("/proc/self/stat")
		if err != nil {
			t.Fatal(err)
		}
		start = strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+1:]))[19]
	}

	if err := os.Mkdir(filepath.Join(dir, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	entry := map[string]any{"pid": pid, "procStart": start, "sessionId": "id", "name": "amesh-identity", "cwd": dir, "messagingSocketPath": "/tmp/amesh-identity.sock", "peerProtocol": 1, "status": "idle", "version": "2.1.330"}
	b, _ := json.Marshal(entry)
	if err := os.WriteFile(filepath.Join(dir, "sessions", strconv.Itoa(pid)+".json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, registry.Key{PID: pid, ProcStart: start}
}

func TestStatusShowsSeatsAndConnectedPipes(t *testing.T) {
	dir, key := liveEntry(t)
	dirs := []string{dir, filepath.Join(dir, "empty")}
	d := daemon{mesh: humanBackend{member: key}, registry: registry.New(dirs), cfg: config.Config{ClaudeConfigDirs: dirs, MachineName: "mac"}, started: time.Now(), pipes: map[*registration]struct{}{{key: key}: {}, {key: key, parent: "parent"}: {}}}
	w := httptest.NewRecorder()
	d.status(w, request(t, ""))
	var got struct {
		Dirs     []contract.DaemonStatusConfigDirsItem `json:"configDirs"`
		Pipes    []pipeStatus                          `json:"pipes"`
		Members  []contract.DaemonStatusSessionsItem   `json:"sessions"`
		Warnings []string                              `json:"warnings"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Dirs) != 2 || got.Dirs[0].Sessions != 1 || got.Dirs[1].Sessions != 0 || len(got.Pipes) != 2 || got.Pipes[1].Parent != "parent" || len(got.Members) != 1 {
		t.Fatal(got)
	}
	clear(d.pipes)
	got.Warnings = nil
	got.Members = nil
	w = httptest.NewRecorder()
	d.status(w, request(t, ""))
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || len(got.Pipes) != 0 || len(got.Members) != 1 || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "2.1.330") {
		t.Fatal(got, err)
	}
}

func TestTranscriptRequiresMatchingCompleteRow(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	text := "same text"
	h := hookInput{PromptID: "right", Prompt: &text}
	row := func(id, prompt, kind string) string {
		return fmt.Sprintf(`{"type":"attachment","sessionId":%q,"promptId":%q,"attachment":{"type":"queued_command","prompt":"same text","origin":{"kind":%q}}}`, id, prompt, kind)
	}
	old := row("other", "right", "human") + "\n" + row("id", "wrong", "human") + "\n"
	target := row("id", "right", "peer")
	file := filepath.Join(dir, "amesh-identity.jsonl")
	if err = os.WriteFile(file, []byte(old+target), 0o600); err != nil {
		t.Fatal(err)
	}
	n, kind, err := scanTranscript(root, filepath.Base(file), 0, "id", h, time.Now())
	if err != nil || n != int64(len(old)) || kind != nil {
		t.Fatal(n, kind, err)
	}
	if err = os.WriteFile(file, []byte(old+target+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, kind, err = scanTranscript(root, filepath.Base(file), n, "id", h, time.Now())
	if err != nil || kind == nil || kind.Kind != "peer" {
		t.Fatal(kind, err)
	}
}

func TestCandidateWaitsForRowOrSessionEnd(t *testing.T) {
	dir, key := liveEntry(t)
	r := registry.New([]string{dir})
	s, err := r.Resolve(t.Context(), key.PID)
	if err != nil {
		t.Fatal(err)
	}
	backend := humanBackend{human: make(chan registry.Session, 2), member: key}
	transcript := filepath.Join(dir, "projects", "amesh-identity", "id.jsonl")
	if err = os.MkdirAll(filepath.Dir(transcript), 0o700); err != nil {
		t.Fatal(err)
	}
	text := "queued"
	h := hookInput{Prompt: &text, PromptID: "p", Transcript: transcript}
	synctest.Test(t, func(t *testing.T) {
		d := daemon{ctx: t.Context(), registry: r, mesh: backend}
		received := time.Now()
		d.scheduleTurn(s, h, received)
		synctest.Wait()
		if len(backend.human) != 0 {
			t.Fatal("early report")
		}
		line := `{"type":"user","sessionId":"id","promptId":"p","origin":{"kind":"human"},"message":{"content":[{"type":"text","text":"queued"}]}}` + "\n"
		if err := os.WriteFile(transcript, []byte(line), 0o600); err != nil {
			t.Fatal(err)
		}
		d.work.Wait()
		if len(backend.human) != 1 {
			t.Fatal("missing report")
		}
		// An unattributed matching row ends this candidate, even if the text is repeated later.
		if err := os.WriteFile(transcript, []byte(strings.Replace(line, `"kind":"human"`, `"kind":""`, 1)+line), 0o600); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		d.scheduleTurn(s, h, received)
		d.work.Wait()
		if time.Since(start) != 30*time.Second {
			t.Fatal("old row used or wrong check deadline", time.Since(start))
		}
		if len(backend.human) != 1 {
			t.Fatal("used the origin of a later row")
		}
		h.PromptID = "p"
		d.scheduleTurn(s, h, received)
		synctest.Wait()
		newer := h
		newer.PromptID = "newer"
		d.scheduleTurn(s, newer, received)
		file, e := os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0)
		if e != nil {
			t.Fatal(e)
		}
		_, e = file.WriteString(line)
		if e != nil {
			t.Fatal(e)
		}
		if e = file.Close(); e != nil {
			t.Fatal(e)
		}
		d.work.Wait()
		if len(backend.human) != 1 {
			t.Fatal("superseded transcript check reported a turn")
		}
		h.PromptID = "absent"
		d.scheduleTurn(s, h, received)
		synctest.Wait()
		if err := os.Remove(filepath.Join(dir, "sessions", strconv.Itoa(key.PID)+".json")); err != nil {
			t.Fatal(err)
		}
		d.work.Wait()
		if len(backend.human) != 1 {
			t.Fatal("reported after end")
		}
	})
}

func TestReconnectedPipeReturnsRefusal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		_, err := redial(t.Context(), func(context.Context) (*bufferedConn, error) {
			return nil, &localError{"excluded", "amesh is turned off for this directory."}
		})
		if _, ok := errors.AsType[*localError](err); !ok || time.Since(start) != 250*time.Millisecond {
			t.Fatal(err)
		}
	})
}

func TestHumanHookTranscriptExample(t *testing.T) {
	var h hookInput
	if err := json.Unmarshal(hubtest.Example(t, "hookPromptHuman"), &h); err != nil {
		t.Fatal(err)
	}
	if h.SessionID == nil || h.Transcript != "/Users/tenequm/.claude/projects/-Users-tenequm-pj-pond/"+*h.SessionID+".jsonl" {
		t.Fatal("hook transcript path")
	}
}
