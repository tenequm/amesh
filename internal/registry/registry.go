package registry

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type Key struct {
	PID       int
	ProcStart string
}

type Session struct {
	Codex     bool
	HumanTurn string
	Key
	SessionID, Name, Project, CWD, ConfigDir, SocketPath, Status, FromMode, Version string
	NameSource, Entrypoint                                                          string
	StartedAt                                                                       int64
}

func (s Session) MeshKey() string {
	// A Codex key derives from the thread so it survives a resume, and starts above the PID range so it never names a process.
	if s.Codex {
		digest := sha256.Sum256([]byte(s.SessionID))
		return fmt.Sprintf("%d-%x", 1<<32+uint64(binary.BigEndian.Uint32(digest[:4])), digest[4:8])
	}
	return s.Key.MeshKey()
}

func (k Key) MeshKey() string {
	digest := sha256.Sum256([]byte(k.ProcStart))
	return fmt.Sprintf("%d-%x", k.PID, digest[:4])
}

type Listing struct {
	Sessions []Session
	// A failed process lookup leaves the start time unknown.
	unavailable map[entryPath]string
}

type entryPath struct {
	dir string
	pid int
}

func (l Listing) Unavailable(s Session) bool {
	start, ok := l.unavailable[entryPath{s.ConfigDir, s.PID}]
	return ok && (start == "" || start == s.ProcStart)
}

type Registry struct {
	codex         map[int]*CodexPipe
	pipeOrder     uint64
	CodexStatus   func(string) string
	CodexComplete func(string, [32]byte, string)
	scanMu        sync.Mutex
	reported      map[entryPath]string
	dirs          []string
	lookup        func(int) (int, string, error)
	mu            sync.Mutex
	modes         map[int]permissionClass
}

func ProcessStart(pid int) (string, error) {
	_, start, err := process(pid)
	return start, err
}

func New(configDirs []string) *Registry {
	return &Registry{codex: make(map[int]*CodexPipe), dirs: slices.Clone(configDirs), lookup: process, modes: make(map[int]permissionClass)}
}

func (r *Registry) Resolve(ctx context.Context, pid int) (Session, error) {
	r.mu.Lock()
	pipe := r.codex[pid]
	r.mu.Unlock()
	if pipe != nil {
		return pipe.Session(ctx, "")
	}
	for hops := 0; hops <= 8 && pid > 0; hops++ {
		if err := ctx.Err(); err != nil {
			return Session{}, err
		}
		parent, start, err := r.lookup(pid)
		if err != nil {
			return Session{}, err
		}
		session, err := r.readPID(pid, start)
		if err == nil {
			return r.withMode(session), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return Session{}, err
		}
		pid = parent
	}
	return Session{}, errNoSession
}

var (
	errNoSession        = fmt.Errorf("no live Claude session for caller: %w", os.ErrNotExist)
	ErrEntryUnavailable = errors.New("session registry entry unavailable")
	errNotReady         = errors.New("session registry entry not ready")
	errStaleProcess     = errors.New("registry identity differs from live process")
)

func PendingEntry(err error) bool {
	return errors.Is(err, errNoSession) || errors.Is(err, errNotReady)
}

// A bad entry can hide a valid entry for the same process in another seat.
// If no seat verifies it, Resolve must stop before inheriting an ancestor's identity.
func (r *Registry) readPID(pid int, start string) (Session, error) {
	var unavailable error
	for _, dir := range r.dirs {
		s, err := read(dir, pid, start)
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errStaleProcess) {
			unavailable = errors.Join(unavailable, err)
		}
	}
	if unavailable != nil {
		return Session{}, fmt.Errorf("%w: %w", ErrEntryUnavailable, unavailable)
	}
	return Session{}, os.ErrNotExist
}

func read(dir string, pid int, start string) (Session, error) {
	path := filepath.Join(dir, "sessions", strconv.Itoa(pid)+".json")
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0) // #nosec G304 -- configured seat and numeric kernel pid
	if err != nil {
		return Session{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Session{}, err
	}
	if !info.Mode().IsRegular() {
		return Session{}, fmt.Errorf("session registry %s: expected a regular file", path)
	}
	var entry struct {
		Version    string          `json:"version"`
		PID        int             `json:"pid"`
		ProcStart  string          `json:"procStart"`
		SessionID  string          `json:"sessionId"`
		Name       string          `json:"name"`
		CWD        string          `json:"cwd"`
		SocketPath string          `json:"messagingSocketPath"`
		Status     json.RawMessage `json:"status"`
		NameSource string          `json:"nameSource"`
		Entrypoint string          `json:"entrypoint"`
		StartedAt  int64           `json:"startedAt"`
		Protocol   int             `json:"peerProtocol"`
	}
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&entry); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Session{}, fmt.Errorf("%w: incomplete JSON in %s", errNotReady, path)
		}
		return Session{}, fmt.Errorf("session registry %s: %w", path, err)
	}
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return Session{}, fmt.Errorf("session registry %s: trailing JSON data", path)
	}
	if entry.PID <= 0 || entry.ProcStart == "" {
		return Session{}, fmt.Errorf("session registry %s: invalid pid or procStart", path)
	}
	if entry.PID != pid || entry.ProcStart != start {
		return Session{}, errStaleProcess
	}
	if entry.Status == nil {
		return Session{}, fmt.Errorf("%w: missing status in %s", errNotReady, path)
	}
	for _, field := range []struct{ name, value string }{
		{"sessionId", entry.SessionID}, {"name", entry.Name}, {"cwd", entry.CWD}, {"messagingSocketPath", entry.SocketPath},
	} {
		if field.value == "" {
			return Session{}, fmt.Errorf("session registry %s: missing %s", path, field.name)
		}
	}
	if !filepath.IsAbs(entry.CWD) || !filepath.IsAbs(entry.SocketPath) {
		return Session{}, fmt.Errorf("session registry %s: cwd and messagingSocketPath must be absolute", path)
	}
	if entry.Protocol != 1 {
		return Session{}, fmt.Errorf("session registry %s: expected peerProtocol 1, got %d", path, entry.Protocol)
	}
	var status string
	if json.Unmarshal(entry.Status, &status) != nil || status == "" {
		return Session{}, fmt.Errorf("session registry %s: invalid status", path)
	}
	return Session{
		NameSource: entry.NameSource, Entrypoint: entry.Entrypoint, StartedAt: entry.StartedAt,
		Key: Key{pid, start}, SessionID: entry.SessionID, Name: entry.Name, CWD: entry.CWD,
		Version: entry.Version, Project: project(entry.CWD), ConfigDir: dir, SocketPath: entry.SocketPath, Status: status,
	}, nil
}

func project(cwd string) string {
	for dir := filepath.Clean(cwd); dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		path := filepath.Join(dir, ".git")
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if info.Mode().IsRegular() {
			b, _ := os.ReadFile(path) // #nosec G304 -- Git worktree metadata
			if target, ok := strings.CutPrefix(strings.TrimSpace(string(b)), "gitdir: "); ok {
				if !filepath.IsAbs(target) {
					target = filepath.Join(dir, target)
				}
				if filepath.Base(filepath.Dir(target)) == "worktrees" && filepath.Base(filepath.Dir(filepath.Dir(target))) == ".git" {
					return filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(target))))
				}
			}
		}
		return filepath.Base(dir)
	}
	return filepath.Base(cwd)
}

func (r *Registry) List(ctx context.Context) ([]Session, error) {
	listing, err := r.Snapshot(ctx)
	return listing.Sessions, err
}

func (r *Registry) Snapshot(ctx context.Context) (Listing, error) {
	return r.scan(ctx, false)
}

func (r *Registry) scan(ctx context.Context, check bool) (Listing, error) {
	r.scanMu.Lock()
	defer r.scanMu.Unlock()
	live := Listing{unavailable: make(map[entryPath]string)}
	for _, dir := range r.dirs {
		entries, err := os.ReadDir(filepath.Join(dir, "sessions"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return Listing{}, err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return Listing{}, err
			}
			name, ok := strings.CutSuffix(entry.Name(), ".json")
			pid, err := strconv.Atoi(name)
			if !ok || err != nil || pid < 1 || strconv.Itoa(pid) != name {
				continue
			}
			_, start, err := r.lookup(pid)
			var session Session
			if err == nil {
				session, err = read(dir, pid, start)
			}
			if errors.Is(err, os.ErrNotExist) || (!check && errors.Is(err, errStaleProcess)) {
				continue
			}
			if err != nil {
				path := filepath.Join(dir, "sessions", entry.Name())
				if check && !errors.Is(err, errNotReady) {
					return Listing{}, fmt.Errorf("registry entry %s: %w", path, err)
				}
				key := entryPath{dir, pid}
				live.unavailable[key] = start
				if last, ok := r.reported[key]; !ok || last != start {
					slog.Warn("skipping session registry entry", "path", path, "error", err)
				}
				continue
			}
			if check {
				if e := checkInbox(session); e != nil {
					if _, now, lookupErr := r.lookup(pid); errors.Is(lookupErr, os.ErrNotExist) || (lookupErr == nil && now != start) {
						continue
					}
					return Listing{}, e
				}
			}
			live.Sessions = append(live.Sessions, r.withMode(session))
		}
	}
	if !check {
		r.mu.Lock()
		pipes := slices.Collect(maps.Values(r.codex))
		r.mu.Unlock()
		for _, pipe := range pipes {
			if s, err := pipe.cached(ctx); err == nil {
				live.Sessions = append(live.Sessions, s)
			}
		}
	}
	r.reported = live.unavailable
	return live, nil
}

func (r *Registry) SelfCheck(ctx context.Context) error {
	_, err := r.scan(ctx, true)
	return err
}

func checkInbox(s Session) error {
	if filepath.Base(s.SocketPath) != strconv.Itoa(s.PID)+".sock" {
		return fmt.Errorf("registry messagingSocketPath for pid %d: expected <pid>.sock", s.PID)
	}
	dir := filepath.Dir(s.SocketPath)
	for _, path := range []string{dir, s.SocketPath} {
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("registry messagingSocketPath: %w", err)
		}
		if (path == dir && !info.IsDir()) || (path != dir && info.Mode()&os.ModeSocket == 0) || int(info.Sys().(*syscall.Stat_t).Uid) != os.Geteuid() {
			return fmt.Errorf("registry messagingSocketPath %s: unexpected type or owner", path)
		}
	}
	return nil
}

type permissionClass struct{ start, mode string }

func (r *Registry) RecordPermissionMode(key Key, permission string) {
	mode := "prompting"
	if permission == "bypassPermissions" {
		mode = "bypass"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.modes[key.PID] = permissionClass{key.ProcStart, mode}
}

func (r *Registry) withMode(s Session) Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	if class := r.modes[s.PID]; class.start == s.ProcStart {
		s.FromMode = class.mode
	}
	return s
}

// Live checks the exact stored process, without walking to another session.
func (r *Registry) Live(ctx context.Context, key Key) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if key.PID <= 0 || key.ProcStart == "" {
		return false, nil
	}
	_, start, err := r.lookup(key.PID)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || start != key.ProcStart {
		return false, err
	}
	_, err = r.readPID(key.PID, start)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (s Session) MeshExcluded(roots []string) bool {
	_, excluded := s.MeshExclusionRoot(roots)
	return excluded
}

func (s Session) MeshExclusionRoot(roots []string) (string, bool) {
	var resolved string
	var err error
	if len(roots) > 0 {
		resolved, err = filepath.EvalSymlinks(s.CWD)
	}
	for _, root := range roots {
		if s.CWD == root || strings.HasPrefix(s.CWD, strings.TrimRight(root, "/")+"/") {
			return root, true
		}
		base, e := filepath.EvalSymlinks(root)
		if err == nil && e == nil && (resolved == base || strings.HasPrefix(resolved, strings.TrimRight(base, "/")+"/")) {
			return root, true
		}
	}
	return "", false
}
