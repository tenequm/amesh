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
	"github.com/tenequm/amesh/internal/codex"
)

const (
	writerLocks = "thread-writer-locks"
	stateDB     = "state_5.sqlite"
	// The presence loop scans every second, and lsof plus sqlite3 cost tens of milliseconds, so scans reuse a check this recent.
	codexRecheck = 10 * time.Second
	probeTimeout = 2 * time.Second
)

type threadRow struct {
	Source      string `json:"thread_source"`
	Name        string `json:"name"`
	Title       string `json:"title"`
	Cwd         string `json:"cwd"`
	RolloutPath string `json:"rollout_path"`
	CreatedAt   int64  `json:"created_at"`
}

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
	locks                 map[string]bool
	row                   *threadRow
	rowThread             string
	checked, queried      time.Time
	mu                    sync.Mutex
}

func CodexHome() string {
	if dir := os.Getenv("CODEX_HOME"); filepath.IsAbs(dir) {
		return dir
	}
	dir, _ := os.UserHomeDir()
	return filepath.Join(dir, ".codex")
}

func resolvedCodexHome() string {
	home := CodexHome()
	if resolved, err := filepath.EvalSymlinks(home); err == nil {
		return resolved
	}
	return home
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

func processFiles(ctx context.Context, pid int, home string) (openFiles, error) {
	files := openFiles{locks: make(map[string]bool)}
	dir := filepath.Join(home, writerLocks)
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-F0fn").Output() // #nosec G204 -- pid comes from kernel peer credentials.
	if err != nil {
		return files, err
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
	return files, nil
}

func (r *Registry) OpenCodexPipe(ctx context.Context, pid int) *CodexPipe {
	parent, start, err := r.lookup(pid)
	if err != nil || parent <= 1 {
		return nil
	}
	home := resolvedCodexHome()
	if _, err = os.Stat(filepath.Join(home, writerLocks)); err != nil {
		return nil
	}
	files, err := processFiles(ctx, parent, home)
	if err != nil || len(files.locks) == 0 {
		return nil
	}
	own, err := processFiles(ctx, pid, home)
	if err != nil || !filepath.IsAbs(own.cwd) {
		return nil
	}
	cwd := own.cwd
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pipeOrder++
	p := &CodexPipe{registry: r, opened: time.Now(), key: Key{pid, start}, parent: parent, cwd: cwd, order: r.pipeOrder, status: "idle", locks: files.locks, checked: time.Now()}
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

// Session verifies the pipe's thread now: the parent must hold its writer lock, and id, when set, must name it.
func (p *CodexPipe) Session(ctx context.Context, id string) (Session, error) {
	return p.session(ctx, id, true)
}

// cached serves the presence scan, which may reuse a recent lock and state check.
func (p *CodexPipe) cached(ctx context.Context) (Session, error) {
	return p.session(ctx, "", false)
}

func (p *CodexPipe) session(ctx context.Context, id string, fresh bool) (Session, error) {
	r := p.registry
	home := resolvedCodexHome()
	parent, start, err := r.lookup(p.key.PID)
	if err != nil || parent != p.parent || start != p.key.ProcStart {
		return Session{}, os.ErrNotExist
	}
	p.mu.Lock()
	locks := p.locks
	stale := fresh || time.Since(p.checked) >= codexRecheck
	p.mu.Unlock()
	if stale {
		files, e := processFiles(ctx, parent, home)
		if e != nil && fresh {
			return Session{}, os.ErrNotExist
		}
		if e == nil {
			locks = files.locks
			p.mu.Lock()
			p.locks, p.checked = locks, time.Now()
			p.mu.Unlock()
		}
	}
	r.mu.Lock()
	if r.codex[p.key.PID] != p {
		r.mu.Unlock()
		return Session{}, os.ErrNotExist
	}
	if id != "" && locks[id] {
		for _, sibling := range r.codex {
			if sibling != p && sibling.thread == id {
				sibling.thread = ""
			}
		}
		p.thread = id
	}
	thread := p.thread
	visible := locks[thread] && (id == "" || id == thread)
	// The newest pipe under a parent owns its visible thread; an older one holds the thread that /new left loaded.
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
	if p.rowThread != thread {
		p.row, p.rowThread, p.queried = nil, thread, time.Time{}
	}
	if fresh || time.Since(p.queried) >= codexRecheck {
		row, e := queryThread(ctx, home, thread)
		if e == nil {
			p.queried = time.Now()
			p.row = row
		} else if p.row == nil {
			// Without the state row, thread_source cannot be checked, so stay hidden while the database exists.
			if _, statErr := os.Stat(filepath.Join(home, stateDB)); statErr == nil {
				return Session{}, os.ErrNotExist
			}
		}
	}
	s := Session{Key: p.key, SessionID: thread, Codex: true, CWD: p.cwd, ConfigDir: home, Entrypoint: "codex", NameSource: "derived"}
	if row := p.row; row != nil {
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

// queryThread returns nil without an error when the thread has no row yet.
func queryThread(ctx context.Context, home, thread string) (*threadRow, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	query := "select name,title,cwd,rollout_path,created_at,thread_source from threads where id='" + thread + "'"
	// #nosec G204 -- thread is a canonical UUID from a held writer lock.
	raw, err := exec.CommandContext(ctx, "sqlite3", "-readonly", "-json", filepath.Join(home, stateDB), query).Output()
	if err != nil {
		return nil, err
	}
	var rows []threadRow
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	if err = json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, nil
	}
	return &rows[0], nil
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
		var head []byte
		if oversized {
			head = bytes.Clone(line[:min(len(line), 512)])
		}
		for errors.Is(err, bufio.ErrBufferFull) {
			line, err = reader.ReadSlice('\n')
			n += int64(len(line))
		}
		if err != nil {
			return
		}
		p.offset += n
		if oversized {
			// A long final answer cannot be parsed, but its turn still ended.
			if bytes.Contains(head, []byte(`"type":"task_complete"`)) || bytes.Contains(head, []byte(`"type":"turn_aborted"`)) {
				p.status = "idle"
				p.input = [32]byte{}
			}
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
		// Turns that started before this pipe opened are history, not human turns or peer turns to answer.
		if !e.Timestamp.Before(p.opened) {
			p.trigger = v.Attribution.Trigger
			if p.trigger == "user" {
				p.human = v.TurnID
			}
		}
	case e.Type == "response_item" && v.Role == "user" && (p.trigger == codex.TurnTrigger || p.trigger == "queue"):
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
		if v.Type == "task_complete" && (p.trigger == codex.TurnTrigger || p.trigger == "queue") && p.input != ([32]byte{}) && v.Last != "" && len(v.Last) <= 32<<10 && p.registry.CodexComplete != nil {
			p.registry.CodexComplete(thread, p.input, v.Last)
		}
		p.trigger = ""
		p.input = [32]byte{}
	}
}
