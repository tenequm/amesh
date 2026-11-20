package registry

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

type CodexPipe struct {
	registry              *Registry
	key                   Key
	parent                int
	order                 uint64
	cwd, thread           string
	rollout, turn, status string
	opened                time.Time
	human, trigger        string
	input                 [32]byte
	offset                int64
	mu                    sync.Mutex
}

func CodexHome() string {
	if dir := os.Getenv("CODEX_HOME"); filepath.IsAbs(dir) {
		return dir
	}
	dir, _ := os.UserHomeDir()
	return filepath.Join(dir, ".codex")
}

type openFiles struct {
	locks map[string]bool
	cwd   string
}

func heldLock(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	return errors.Is(err, syscall.EWOULDBLOCK)
}

func processFiles(ctx context.Context, pid int, home string) openFiles {
	files := openFiles{locks: make(map[string]bool)}
	dir := filepath.Join(home, "thread-writer-locks")
	raw, err := exec.CommandContext(ctx, "lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-F0fn").Output() // #nosec G204 -- pid comes from kernel peer credentials.
	if err != nil {
		return files
	}
	var fd string
	for field := range bytes.SplitSeq(raw, []byte{0}) {
		s := strings.TrimPrefix(string(field), "\n")
		if after, ok := strings.CutPrefix(s, "f"); ok {
			fd = after
			continue
		}
		p, ok := strings.CutPrefix(s, "n")
		if !ok {
			continue
		}
		if fd == "cwd" {
			files.cwd = p
		}
		if filepath.Dir(p) != dir {
			continue
		}
		id, ok := strings.CutSuffix(filepath.Base(p), ".lock")
		u, err := uuid.Parse(id)
		if ok && err == nil && u.String() == id && heldLock(p) {
			files.locks[id] = true
		}
	}
	return files
}

func (r *Registry) OpenCodexPipe(ctx context.Context, pid int) *CodexPipe {
	parent, start, err := r.lookup(pid)
	if err != nil || parent <= 1 {
		return nil
	}
	home := CodexHome()
	if resolved, e := filepath.EvalSymlinks(home); e == nil {
		home = resolved
	}
	if _, err = os.Stat(filepath.Join(home, "thread-writer-locks")); err != nil {
		return nil
	}
	files := processFiles(ctx, parent, home)
	if len(files.locks) == 0 {
		return nil
	}
	cwd := processFiles(ctx, pid, home).cwd
	if !filepath.IsAbs(cwd) {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pipeOrder++
	p := &CodexPipe{registry: r, opened: time.Now(), key: Key{pid, start}, parent: parent, cwd: cwd, order: r.pipeOrder, status: "idle"}
	for _, sibling := range r.codex {
		if sibling.parent == parent {
			delete(files.locks, sibling.thread)
		}
	}
	if len(files.locks) == 1 {
		for id := range files.locks {
			p.thread = id
		}
	}
	r.codex[pid] = p
	return p
}

func (p *CodexPipe) Close() {
	r := p.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.codex[p.key.PID] == p {
		delete(r.codex, p.key.PID)
	}
}

func (p *CodexPipe) Session(ctx context.Context, id string) (Session, error) {
	r := p.registry
	home := CodexHome()
	if resolved, err := filepath.EvalSymlinks(home); err == nil {
		home = resolved
	}
	parent, start, err := r.lookup(p.key.PID)
	if err != nil || parent != p.parent || start != p.key.ProcStart {
		return Session{}, os.ErrNotExist
	}
	files := processFiles(ctx, parent, home)
	r.mu.Lock()
	if r.codex[p.key.PID] != p {
		r.mu.Unlock()
		return Session{}, os.ErrNotExist
	}
	if id != "" && files.locks[id] {
		for _, sibling := range r.codex {
			if sibling != p && sibling.thread == id {
				sibling.thread = ""
			}
		}
		p.thread = id
	}
	thread := p.thread
	visible := files.locks[thread] && (id == "" || id == thread)
	for _, sibling := range r.codex {
		if sibling.parent == parent && sibling.order > p.order {
			visible = false
		}
	}
	r.mu.Unlock()
	if !visible {
		return Session{}, os.ErrNotExist
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s := Session{Key: p.key, SessionID: thread, Codex: true, CWD: p.cwd, ConfigDir: home, Entrypoint: "codex", NameSource: "derived"}
	var rows []struct {
		Source      string `json:"thread_source"`
		Name        string `json:"name"`
		Title       string `json:"title"`
		Cwd         string `json:"cwd"`
		RolloutPath string `json:"rollout_path"`
		CreatedAt   int64  `json:"created_at"`
	}
	query := "select name,title,cwd,rollout_path,created_at,thread_source from threads where id='" + thread + "'"
	// #nosec G204 -- thread is a canonical UUID from a held writer lock.
	if raw, e := exec.CommandContext(ctx, "sqlite3", "-readonly", "-json", filepath.Join(home, "state_5.sqlite"), query).Output(); e == nil && json.Unmarshal(raw, &rows) == nil && len(rows) == 1 {
		row := rows[0]
		if row.Source != "" && row.Source != "user" {
			return Session{}, os.ErrPermission
		}
		s.Name = row.Name
		if s.Name == "" {
			s.Name = row.Title
		}
		if filepath.IsAbs(row.Cwd) {
			s.CWD = row.Cwd
		}
		s.StartedAt = row.CreatedAt * 1000
		if row.RolloutPath != p.rollout {
			p.rollout = row.RolloutPath
			p.offset = 0
			p.turn = ""
			p.status = "idle"
		}
	}
	if s.Name == "" {
		s.Name = filepath.Base(s.CWD) + "-" + thread[len(thread)-8:]
	}
	s.Project = project(s.CWD)
	p.readStatus(ctx, home, thread)
	s.HumanTurn = p.human
	s.Status = p.status
	if r.CodexStatus != nil {
		if status := r.CodexStatus(thread); status != "" {
			s.Status = status
		}
	}
	return s, nil
}

func (p *CodexPipe) readStatus(ctx context.Context, home, thread string) {
	if !strings.HasSuffix(filepath.Base(p.rollout), "-"+thread+".jsonl") {
		return
	}
	root := filepath.Join(home, "sessions")
	rel, err := filepath.Rel(root, p.rollout)
	if err != nil || !filepath.IsLocal(rel) {
		return
	}
	f, err := os.OpenInRoot(root, rel)
	if err != nil {
		return
	}
	defer f.Close()
	if _, err = f.Seek(p.offset, io.SeekStart); err != nil {
		return
	}
	reader := bufio.NewReaderSize(f, 128<<10)
	for ctx.Err() == nil {
		line, err := reader.ReadSlice('\n')
		n := int64(len(line))
		oversized := errors.Is(err, bufio.ErrBufferFull)
		for errors.Is(err, bufio.ErrBufferFull) {
			line, err = reader.ReadSlice('\n')
			n += int64(len(line))
		}
		if err != nil {
			return
		}
		p.offset += n
		if oversized {
			p.trigger = ""
			continue
		}
		p.readEvent(thread, line)
	}
}

func (p *CodexPipe) readEvent(thread string, line []byte) {
	var e struct {
		Type      string    `json:"type"`
		Timestamp time.Time `json:"timestamp"`
		Payload   struct {
			Type        string          `json:"type"`
			Role        string          `json:"role"`
			Content     json.RawMessage `json:"content"`
			TurnID      string          `json:"turn_id"`
			Attribution struct {
				Trigger string `json:"turn_trigger"`
			} `json:"turn_attribution"`
			Last string `json:"last_agent_message"`
		} `json:"payload"`
	}
	if json.Unmarshal(line, &e) != nil {
		return
	}
	v := e.Payload
	switch {
	case e.Type == "event_msg" && v.Type == "task_started":
		p.status = "busy"
		p.turn = v.TurnID
		p.trigger = ""
		p.input = [32]byte{}
		if !e.Timestamp.Before(p.opened) {
			p.trigger = v.Attribution.Trigger
			if p.trigger == "user" {
				p.human = v.TurnID
			}
		}
	case e.Type == "response_item" && v.Role == "user" && (p.trigger == "amesh" || p.trigger == "queue"):
		var parts []struct {
			Text string `json:"text"`
		}
		if p.input != ([32]byte{}) || json.Unmarshal(v.Content, &parts) != nil || len(parts) != 1 || !strings.HasPrefix(parts[0].Text, "<cross-session-message") {
			p.trigger = ""
		} else {
			p.input = sha256.Sum256([]byte(parts[0].Text))
		}
	case e.Type == "event_msg" && (v.Type == "task_complete" || v.Type == "turn_aborted") && (v.TurnID == p.turn || v.Type == "turn_aborted" && v.TurnID == ""):
		p.status = "idle"
		if v.Type == "task_complete" && (p.trigger == "amesh" || p.trigger == "queue") && p.input != ([32]byte{}) && v.Last != "" && len(v.Last) <= 32<<10 && p.registry.CodexComplete != nil {
			p.registry.CodexComplete(thread, p.input, v.Last)
		}
		p.trigger = ""
		p.input = [32]byte{}
	}
}
