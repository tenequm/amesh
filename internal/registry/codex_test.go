package registry

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
	firstHome := t.TempDir()
	t.Setenv("CODEX_HOME", firstHome)
	dir := filepath.Join(home, "thread-writer-locks")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	r := New(nil, firstHome, home)
	r.CodexStatus = func(dir, _ string) string {
		if dir == home {
			return "busy"
		}
		return "idle"
	}
	lock := func(dir, id string) *os.File {
		f, err := os.Create(filepath.Join(dir, id+".lock"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		return f
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
	first := uuid.NewString()
	lock(dir, first)
	sql := fmt.Sprintf("create table threads(id, name, title, cwd, rollout_path, created_at, thread_source); insert into threads values('%s','review','','/work/demo','',0,'user')", first)
	if err := exec.CommandContext(t.Context(), "sqlite3", filepath.Join(home, stateDB), sql).Run(); err != nil {
		t.Fatal(err)
	}
	old := pipe()
	orig, err := old.Session(t.Context(), "")
	if err != nil || orig.SessionID != first || orig.ConfigDir != home || orig.Status != "busy" || orig.Name != "review" || orig.Project != "demo" {
		t.Fatal(orig, err)
	}
	second := uuid.NewString()
	secondLock := lock(dir, second)
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
	otherDir := filepath.Join(firstHome, writerLocks)
	if err = os.Mkdir(otherDir, 0o700); err != nil {
		t.Fatal(err)
	}
	otherID := uuid.NewString()
	otherLock := lock(otherDir, otherID)
	otherPipe := pipe()
	if s, e := otherPipe.Session(t.Context(), ""); e != nil || s.ConfigDir != firstHome || s.SessionID != otherID || s.Status != "idle" {
		t.Fatal(s, e)
	}
	if _, e := current.Session(t.Context(), second); e != nil {
		t.Fatal("new pipe in another home hid a live thread", e)
	}
	otherPipe.Close()
	if err = otherLock.Close(); err != nil {
		t.Fatal(err)
	}
	lock(otherDir, second)
	duplicate := pipe()
	if _, err = duplicate.Session(t.Context(), second); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("claimed another home's active thread UUID", err)
	}
	duplicate.Close()
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
	third := uuid.NewString()
	lock(dir, third)
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
