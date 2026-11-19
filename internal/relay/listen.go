package relay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
)

// Path hashes the domain (namespace + newline + machine ID) and endpoint or lane ID.
func Path(socketDir, domain, id string) (string, error) {
	namespace, machine, ok := strings.Cut(domain, "\n")
	if !ok || (namespace != "amesh-relay" && namespace != "amesh-ln") || machine == "" || strings.ContainsAny(machine, "\r\n") || id == "" || strings.ContainsAny(id, "\r\n") {
		return "", errors.New("expected relay namespace, machine id and endpoint or lane id")
	}
	sum := sha256.Sum256([]byte(domain + "\n" + id))
	return filepath.Join(socketDir, fmt.Sprintf("a%x", sum[:8])[:16]+".sock"), nil
}

var socketName = regexp.MustCompile(`^[0-9a-f]{1,16}\.sock$`)

type endpoint struct {
	closing  bool
	manager  *Manager
	ctx      context.Context
	cancel   context.CancelFunc
	listener *net.UnixListener
	handle   Handler
	done     chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
	pending  map[subscription]*pending
}

func (m *Manager) Listen(ctx context.Context, path string, handle Handler) (io.Closer, error) {
	name := filepath.Base(path)
	if handle == nil || filepath.Dir(path) != filepath.Clean(m.socketDir) || !socketName.MatchString(name) || !strings.ContainsAny(strings.TrimSuffix(name, ".sock"), "abcdef") {
		return nil, errors.New("invalid relay socket or handler")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	from := envelope.Address(path)
	if m.endpoints[from] != nil {
		return nil, errors.New("endpoint already listening")
	}
	addr := &net.UnixAddr{Name: path, Net: "unix"}
	ln, err := net.ListenUnix("unix", addr)
	if errors.Is(err, syscall.EADDRINUSE) {
		// Only an abandoned socket of this uid can be reclaimed after a crash.
		info, statErr := os.Lstat(path)
		if statErr != nil || info.Mode()&os.ModeSocket == 0 || int(info.Sys().(*syscall.Stat_t).Uid) != os.Geteuid() {
			return nil, err
		}
		conn, dialErr := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", path)
		if dialErr == nil {
			_ = conn.Close()
			return nil, err
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, err
		}
		current, statErr := os.Lstat(path)
		if statErr != nil || !os.SameFile(info, current) {
			return nil, err
		}
		if removeErr := os.Remove(path); removeErr != nil {
			return nil, removeErr
		}
		ln, err = net.ListenUnix("unix", addr)
	}
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	child, cancel := context.WithCancel(ctx)
	e := &endpoint{manager: m, ctx: child, cancel: cancel, listener: ln, handle: handle, done: make(chan struct{}), pending: make(map[subscription]*pending)}
	m.endpoints[from] = e
	go e.serve(from)
	return e, nil
}

func (e *endpoint) Close() error { e.cancel(); <-e.done; return nil }

func (e *endpoint) serve(from string) {
	stop := context.AfterFunc(e.ctx, func() { _ = e.listener.Close() })
	readers := make(chan struct{}, 32)
	for e.ctx.Err() == nil {
		conn, err := e.listener.AcceptUnix()
		if err != nil {
			break
		}
		peer, credErr := peercred.Read(conn)
		if credErr != nil || peer.PID <= 0 || int(peer.UID) != os.Geteuid() {
			_ = conn.Close()
			slog.Warn("discarded native connection without matching credentials")
			continue
		}
		select {
		case readers <- struct{}{}:
		case <-e.ctx.Done():
			_ = conn.Close()
			continue
		}
		e.wg.Go(func() {
			defer func() { <-readers }()
			defer conn.Close()
			unhook := context.AfterFunc(e.ctx, func() { _ = conn.Close() })
			defer unhook()
			if err := e.read(conn, peer); err != nil && e.ctx.Err() == nil {
				slog.Warn("discarded native relay frame", "error", err)
			}
		})
	}
	e.cancel()
	stop()
	_ = e.listener.Close()
	e.manager.mu.Lock()
	delete(e.manager.endpoints, from)
	e.manager.mu.Unlock()
	e.mu.Lock()
	e.closing = true
	e.mu.Unlock()
	e.wg.Wait()
	close(e.done)
}

func (e *endpoint) read(conn *net.UnixConn, peer peercred.Credentials) error {
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	// A UTF-16 code unit uses up to three UTF-8 bytes.
	data, err := io.ReadAll(io.LimitReader(conn, 3*lineLimit+2))
	if err != nil {
		return err
	}
	line, ok := bytes.CutSuffix(data, []byte("\n"))
	if !ok || bytes.IndexByte(line, '\n') >= 0 || len(line) > 3*lineLimit || !utf8.Valid(line) || oversized(line) {
		return errors.New("invalid native line")
	}
	event, err := decode(line)
	if err != nil {
		return err
	}
	event.Peer = peer
	if event.Action == "user" {
		return e.handle(e.ctx, event)
	}
	return e.route(event)
}

func decode(line []byte) (Event, error) {
	var f frame
	if err := json.Unmarshal(line, &f); err != nil {
		return Event{}, errors.New("invalid native JSON")
	}
	if f.Version != 1 || !uuidRE.MatchString(f.ID) || !validFrom(f.From) {
		return Event{}, errors.New("invalid native frame header")
	}
	event := Event{Action: f.Action, ID: f.ID, From: f.From, OriginalID: f.OriginalID, Status: f.Status, StatusDetail: f.StatusDetail, DropReason: f.DropReason, DroppedIDs: f.DroppedIDs, Reason: f.Reason, FromMode: f.Mode, FinishedAt: f.FinishedAt}
	switch {
	case f.Type == "user" && f.Message != nil && f.Priority == "next" && f.Message.Role == "user":
		msg, err := envelope.Parse(f.Message.Content)
		if err != nil || msg.From != f.From {
			return Event{}, errors.New("invalid native reply envelope")
		}
		event.Action, event.Message, event.FromMode = "user", msg, msg.FromMode
	case f.Type == "control" && uuidRE.MatchString(f.OriginalID):
		switch f.Action {
		case "peer_message_status":
			if f.Status == "" {
				return Event{}, errors.New("missing native receipt status")
			}
			for _, id := range f.DroppedIDs {
				if !uuidRE.MatchString(id) {
					return Event{}, errors.New("invalid coalesced receipt id")
				}
			}
		case "peer_idle_notice":
			// Native headless sessions may send null mode and no completed turn time.
			if f.State != "idle" || f.FinishedAt < 0 || (f.Mode != "" && f.Mode != "prompting" && f.Mode != "bypass") {
				return Event{}, errors.New("invalid native idle notice")
			}
		default:
			return Event{}, errors.New("unknown native control action")
		}
	default:
		return Event{}, errors.New("unsupported native frame")
	}
	return event, nil
}
