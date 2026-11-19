package control

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/peercred"
	"github.com/tenequm/amesh/internal/registry"
)

type daemon struct {
	cfg      config.Config
	registry *registry.Registry
	lane     mcpserver.LaneBackend
	mesh     mcpserver.MeshBackend
	ctx      context.Context
	started  time.Time
	mu       sync.Mutex
	pipes    map[*registration]struct{}
	work     sync.WaitGroup
}

type registration struct {
	key    registry.Key
	parent string
}

type peerPID struct{}

func connContext(ctx context.Context, conn net.Conn) context.Context {
	u, ok := conn.(*net.UnixConn)
	if ok {
		cred, err := peercred.Read(u)
		if err == nil && int(cred.UID) == os.Geteuid() && cred.PID > 0 {
			return context.WithValue(ctx, peerPID{}, cred.PID)
		}
	}
	_ = conn.Close()
	return ctx
}

func listen(ctx context.Context, path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "unix", path)
	if errors.Is(err, syscall.EADDRINUSE) {
		// The daemon lock serializes startup. Reclaim only a dead socket.
		if fi, e := os.Lstat(path); e == nil && fi.Mode()&os.ModeSocket != 0 {
			conn, e := (&net.Dialer{}).DialContext(ctx, "unix", path)
			if e == nil {
				_ = conn.Close()
			} else if errors.Is(e, syscall.ECONNREFUSED) {
				if e = os.Remove(path); e != nil {
					return nil, e
				}
				l, err = lc.Listen(ctx, "unix", path)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0o600); err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}

func Serve(ctx context.Context, cfg config.Config, sessions *registry.Registry, lane mcpserver.LaneBackend, mesh mcpserver.MeshBackend) error {
	if err := sessions.SelfCheck(ctx); err != nil {
		return fmt.Errorf("registry startup check: %w", err)
	}
	l, err := listen(ctx, cfg.SocketPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	d := &daemon{cfg: cfg, registry: sessions, lane: lane, mesh: mesh, ctx: ctx, started: time.Now(), pipes: make(map[*registration]struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /amesh/mcp", d.upgrade)
	mux.HandleFunc("POST /amesh/hooks", d.hook)
	mux.HandleFunc("GET /amesh/status", d.status)
	server := &http.Server{Handler: mux, ConnContext: connContext, BaseContext: func(net.Listener) context.Context { return ctx }, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() { defer close(done); <-ctx.Done(); _ = server.Shutdown(context.WithoutCancel(ctx)) }()
	err = server.Serve(l)
	cancel()
	<-done
	d.work.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

type localError struct {
	Code    string `json:"error"`
	Message string `json:"message"`
}

func reject(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(localError{code, message})
}

func excluded(cwd string, roots []string) bool {
	for _, root := range roots {
		if cwd == root || strings.HasPrefix(cwd, strings.TrimRight(root, "/")+"/") {
			return true
		}
		resolved, err := filepath.EvalSymlinks(cwd)
		base, e := filepath.EvalSymlinks(root)
		if err == nil && e == nil && (resolved == base || strings.HasPrefix(resolved, strings.TrimRight(base, "/")+"/")) {
			return true
		}
	}
	return false
}

func mode(r *http.Request) (string, bool, error) {
	if r.Method != http.MethodGet || r.ProtoMajor != 1 || r.ProtoMinor != 1 || !strings.EqualFold(r.Header.Get("Upgrade"), "mcp-stdio") || !headerToken(r.Header.Values("Connection"), "Upgrade") {
		return "", false, errors.New("expected an HTTP/1.1 mcp-stdio upgrade")
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return "", false, err
	}
	for k := range q {
		if k != "arg" && k != "reconnect" {
			return "", false, fmt.Errorf("unknown parameter %q", k)
		}
	}
	if v, ok := q["reconnect"]; ok && (len(v) != 1 || v[0] != "1") {
		return "", false, errors.New("invalid reconnect parameter")
	}
	args := q["arg"]
	if len(args) == 0 {
		return "", q.Has("reconnect"), nil
	}
	if len(args) == 2 && args[0] == "--parent" && args[1] != "" && utf8.ValidString(args[1]) && utf8.RuneCountInString(args[1]) <= 200 {
		return args[1], q.Has("reconnect"), nil
	}
	return "", false, fmt.Errorf("unknown or malformed argument %q", args[0])
}

func headerToken(values []string, want string) bool {
	for _, v := range values {
		for token := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func (d *daemon) upgrade(w http.ResponseWriter, r *http.Request) {
	parent, reconnect, err := mode(r)
	if err != nil {
		reject(w, 400, "bad_request", err.Error())
		return
	}
	pid, _ := r.Context().Value(peerPID{}).(int)
	s, err := d.registry.Resolve(r.Context(), pid)
	if err != nil {
		reject(w, 403, "not_in_session", "This only works inside a Claude Code session.")
		return
	}
	if parent == "" && excluded(s.CWD, d.cfg.ExcludeRoots) {
		reject(w, 403, "excluded", "amesh is turned off for this directory.")
		return
	}
	var handle io.Closer
	if parent == "" {
		handle, err = d.mesh.Connect(r.Context(), s)
	} else {
		handle, err = d.lane.Connect(r.Context(), s, parent)
	}
	if err != nil {
		http.Error(w, "cannot register session", http.StatusServiceUnavailable)
		return
	}
	defer handle.Close()
	d.work.Add(1)
	defer d.work.Done()
	conn, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	stop := context.AfterFunc(d.ctx, func() { _ = conn.Close() })
	defer stop()
	reg := &registration{s.Key, parent}
	d.mu.Lock()
	d.pipes[reg] = struct{}{}
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.pipes, reg); d.mu.Unlock() }()
	if _, err = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: mcp-stdio\r\n\r\n"); err != nil {
		return
	}
	if err = rw.Flush(); err != nil {
		return
	}
	caller := func(ctx context.Context) (registry.Session, error) {
		current, e := d.registry.Resolve(ctx, pid)
		if e != nil {
			return registry.Session{}, e
		}
		if current.Key != s.Key || (parent == "" && excluded(current.CWD, d.cfg.ExcludeRoots)) {
			return registry.Session{}, errors.New("registered caller changed")
		}
		return current, nil
	}
	_ = mcpserver.Serve(r.Context(), &bufferedConn{conn, rw.Reader}, caller, parent, reconnect, d.lane, d.mesh)
}

func (d *daemon) status(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
