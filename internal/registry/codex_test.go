package registry

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCodexPipeRequiresHeldLockAndNewestSibling(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	dir := filepath.Join(home, "thread-writer-locks")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	r := New(nil)
	lock := func() (*os.File, string) {
		id := uuid.NewString()
		f, err := os.Create(filepath.Join(dir, id+".lock"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		return f, id
	}
	pipe := func() *CodexPipe {
		cmd := exec.CommandContext(t.Context(), "sleep", "300")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		p := r.OpenCodexPipe(t.Context(), cmd.Process.Pid)
		if p == nil {
			t.Fatal("parent lock not recognized")
		}
		t.Cleanup(p.Close)
		return p
	}
	_, first := lock()
	old := pipe()
	orig, err := old.Session(t.Context(), "")
	if err != nil || orig.SessionID != first {
		t.Fatal(orig, err)
	}
	secondLock, second := lock()
	current := pipe()
	active, err := current.Session(t.Context(), "")
	if err != nil || active.SessionID != second {
		t.Fatal(active, err)
	}
	if _, err = old.Session(t.Context(), first); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("hidden thread remained reachable", err)
	}
	if _, err = current.Session(t.Context(), uuid.NewString()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("accepted unrelated metadata", err)
	}
	if err = syscall.Flock(int(secondLock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if _, err = current.Session(t.Context(), second); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unlocked open descriptor was accepted", err)
	}
	current.Close()
	old.Close()
	if _, err = old.Session(t.Context(), first); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("closed pipe remained reachable", err)
	}
	_, third := lock()
	unresolved := pipe()
	if _, err = unresolved.Session(t.Context(), ""); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("guessed among multiple locks", err)
	}
	if got, e := unresolved.Session(t.Context(), third); e != nil || got.SessionID != third {
		t.Fatal(got, e)
	}
}

func TestCodexRolloutMatchesDedicatedPeerTurn(t *testing.T) {
	for _, tc := range []struct {
		trigger         string
		steer, complete bool
	}{{"queue", false, true}, {"amesh", false, true}, {"user", false, false}, {"amesh", true, false}} {
		t.Run(tc.trigger, func(t *testing.T) {
			const thread = "thread"
			const input = "<cross-session-message>\nhello\n</cross-session-message>"
			received := false
			p := CodexPipe{registry: &Registry{CodexComplete: func(id string, hash [32]byte, answer string) {
				received = id == thread && hash == sha256.Sum256([]byte(input)) && answer == "reply"
			}}}
			emit := func(kind string, payload any) {
				line, err := json.Marshal(map[string]any{"timestamp": time.Now(), "type": kind, "payload": payload})
				if err != nil {
					t.Fatal(err)
				}
				p.readEvent(thread, line)
			}
			emit("event_msg", map[string]any{"type": "task_started", "turn_id": "turn", "turn_attribution": map[string]string{"turn_trigger": tc.trigger}})
			if p.status != "busy" || (p.human == "turn") != (tc.trigger == "user") {
				t.Fatal("turn attribution", p.status, p.human)
			}
			user := map[string]any{"type": "message", "role": "user", "content": []map[string]string{{"text": input}}}
			emit("response_item", user)
			if tc.steer {
				emit("response_item", user)
			}
			emit("event_msg", map[string]string{"type": "task_complete", "turn_id": "turn", "last_agent_message": "reply"})
			if p.status != "idle" || received != tc.complete {
				t.Fatal("completion", p.status, received)
			}
		})
	}
}
