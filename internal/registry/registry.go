package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

type Key struct {
	PID       int
	ProcStart string
}

type Session struct {
	Key
	SessionID, Name, Project, CWD, ConfigDir, SocketPath, Status, FromMode string
}

type Registry struct {
	dirs   []string
	lookup func(int) (int, string, error)
}

func New(configDirs []string) *Registry {
	return &Registry{dirs: slices.Clone(configDirs), lookup: process}
}

func (r *Registry) Resolve(ctx context.Context, pid int) (Session, error) {
	for hops := 0; hops <= 8 && pid > 0; hops++ {
		if err := ctx.Err(); err != nil {
			return Session{}, err
		}
		parent, start, err := r.lookup(pid)
		if err != nil {
			return Session{}, err
		}
		for _, dir := range r.dirs {
			session, err := read(dir, pid, start)
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, errStaleProcess) {
				continue
			}
			return session, err
		}
		pid = parent
	}
	return Session{}, fmt.Errorf("no live Claude session for caller: %w", os.ErrNotExist)
}

var errStaleProcess = errors.New("registry procStart differs from live process")

func read(dir string, pid int, start string) (Session, error) {
	path := filepath.Join(dir, "sessions", strconv.Itoa(pid)+".json")
	b, err := os.ReadFile(path) // #nosec G304 -- configured seat and numeric kernel pid
	if err != nil {
		return Session{}, err
	}
	var entry struct {
		PID        int    `json:"pid"`
		ProcStart  string `json:"procStart"`
		SessionID  string `json:"sessionId"`
		Name       string `json:"name"`
		CWD        string `json:"cwd"`
		SocketPath string `json:"messagingSocketPath"`
		Status     string `json:"status"`
		Protocol   int    `json:"peerProtocol"`
	}
	if err := json.Unmarshal(b, &entry); err != nil {
		return Session{}, fmt.Errorf("session registry %s: %w", path, err)
	}
	if entry.PID != pid || entry.ProcStart == "" {
		return Session{}, fmt.Errorf("session registry %s: invalid pid or procStart", path)
	}
	if entry.ProcStart != start {
		return Session{}, errStaleProcess
	}
	for _, field := range []struct{ name, value string }{
		{"sessionId", entry.SessionID}, {"name", entry.Name}, {"status", entry.Status}, {"cwd", entry.CWD}, {"messagingSocketPath", entry.SocketPath},
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
	return Session{
		Key: Key{pid, start}, SessionID: entry.SessionID, Name: entry.Name, CWD: entry.CWD,
		Project: project(entry.CWD), ConfigDir: dir, SocketPath: entry.SocketPath, Status: entry.Status,
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
	return r.scan(ctx, false)
}

func (r *Registry) scan(ctx context.Context, check bool) ([]Session, error) {
	var live []Session
	for _, dir := range r.dirs {
		entries, err := os.ReadDir(filepath.Join(dir, "sessions"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			name, ok := strings.CutSuffix(entry.Name(), ".json")
			pid, err := strconv.Atoi(name)
			if !ok || err != nil || pid < 1 || strconv.Itoa(pid) != name {
				continue
			}
			_, start, err := r.lookup(pid)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			session, err := read(dir, pid, start)
			if errors.Is(err, os.ErrNotExist) || (!check && errors.Is(err, errStaleProcess)) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("registry entry %s: %w", filepath.Join(dir, "sessions", entry.Name()), err)
			}
			if check {
				if e := checkInbox(session); e != nil {
					if _, now, lookupErr := r.lookup(pid); errors.Is(lookupErr, os.ErrNotExist) || (lookupErr == nil && now != start) {
						continue
					}
					return nil, e
				}
			}
			live = append(live, session)
		}
	}
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
