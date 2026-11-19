package registry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func fixture(t *testing.T, dir string, pid int, start, id, name string) {
	t.Helper()
	path := filepath.Join(dir, "sessions")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{"pid": pid, "procStart": start, "sessionId": id, "name": name, "cwd": dir, "status": "idle", "messagingSocketPath": "/tmp/amesh-identity.sock", "peerProtocol": 1})
	if err := os.WriteFile(filepath.Join(path, strconv.Itoa(pid)+".json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSeatOrderAndMutableAttributes(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	r := New([]string{a, b})
	r.lookup = func(p int) (int, string, error) { return p - 1, "start", nil }
	fixture(t, a, 11, "start", "parent", "parent")
	fixture(t, b, 12, "start", "child", "before")
	check := func(id, name string) {
		t.Helper()
		s, err := r.Resolve(t.Context(), 13)
		if err != nil || s.SessionID != id || s.Name != name {
			t.Fatalf("%+v %v", s, err)
		}
	}
	check("child", "before")
	fixture(t, b, 12, "start", "clear", "renamed")
	check("clear", "renamed")
	fixture(t, b, 12, "stale", "clear", "renamed")
	check("parent", "parent")
	live, err := r.List(t.Context())
	if err != nil || len(live) != 1 {
		t.Fatalf("%+v %v", live, err)
	}
	_, err = r.Resolve(t.Context(), 20)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func TestLiveProcessAndFormat(t *testing.T) {
	pid := os.Getpid()
	p, start, err := process(pid)
	if err != nil || p != os.Getppid() || start == "" {
		t.Fatal(p, start, err)
	}
	if runtime.GOOS == "darwin" {
		cmd := exec.CommandContext(t.Context(), "ps", "-o", "lstart=", "-p", strconv.Itoa(pid))
		cmd.Env = append(os.Environ(), "LC_ALL=C", "TZ=UTC")
		out, e := cmd.Output()
		if e != nil || strings.TrimSpace(string(out)) != start {
			t.Fatalf("%q %q %v", out, start, e)
		}
	}
	dir := t.TempDir()
	fixture(t, dir, pid, start, "id", "")
	err = New([]string{dir}).SelfCheck(t.Context())
	if err == nil || !strings.Contains(err.Error(), "missing name") {
		t.Fatal(err)
	}
	if _, _, err = process(99999999); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(fmt.Errorf("missing pid: %w", err))
	}
}

func TestStartupChecksNativeInbox(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "amesh-identity")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	pid := os.Getpid()
	_, start, e := process(pid)
	if e != nil {
		t.Fatal(e)
	}
	fixture(t, dir, pid, start, "id", "name")
	path := filepath.Join(dir, "sessions", strconv.Itoa(pid)+".json")
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	socket := filepath.Join(dir, strconv.Itoa(pid)+".sock")
	b = bytes.Replace(b, []byte("/tmp/amesh-identity.sock"), []byte(socket), 1)
	if e = os.WriteFile(path, b, 0o600); e != nil {
		t.Fatal(e)
	}
	r := New([]string{dir})
	if err := r.SelfCheck(t.Context()); err == nil {
		t.Fatal("accepted absent inbox")
	}
	ln, e := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Base(socket), Net: "unix"})
	if e != nil {
		t.Fatal(e)
	}
	defer ln.Close()
	if err := r.SelfCheck(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture(t, dir, pid, "stale", "id", "name")
	if err := r.SelfCheck(t.Context()); !errors.Is(err, errStaleProcess) {
		t.Fatal(err)
	}
}
