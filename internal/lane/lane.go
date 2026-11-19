package lane

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tenequm/amesh/internal/contract"

	"github.com/google/uuid"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
)

const (
	notFound  = "The parent session is not running on this machine."
	ambiguous = "Two running sessions have that session id."
	gone      = "The session that launched you has ended."
)

type sessions interface {
	List(context.Context) ([]registry.Session, error)
	Live(context.Context, registry.Key) (bool, error)
	Resolve(context.Context, int) (registry.Session, error)
}

type native interface {
	Listen(context.Context, string, relay.Handler) (io.Closer, error)
	Write(context.Context, relay.Delivery) error
	NotifyWhenIdle(context.Context, string, string, string, string) error
}

type Backend struct {
	ctx      context.Context
	sessions sessions
	relays   native
	identity func() (string, error)
	mu       sync.Mutex
	work     sync.WaitGroup
	lanes    map[pair]*conversation
}

type pair struct{ parent, child string }

func (p pair) id() string {
	sum := sha256.Sum256([]byte("amesh-ln-id\n" + p.parent + "\n" + p.child))
	return fmt.Sprintf("ln_%x", sum[:8])
}

type conversation struct {
	backend                *Backend
	key                    pair
	id, address            string
	mu                     sync.Mutex
	parent                 registry.Key
	child                  registry.Session
	chain                  []string
	listener               io.Closer
	retiring               bool
	done                   chan struct{}
	since                  time.Time
	started, busy, replied bool
	active                 string
	turns                  map[string]*turn
}

type turn struct {
	replied bool
	expires time.Time
}

var _ mcpserver.LaneBackend = (*Backend)(nil)

func New(ctx context.Context, sessions *registry.Registry, relays *relay.Manager, stateDir string) *Backend {
	return newBackend(ctx, sessions, relays, sync.OnceValues(func() (string, error) { return hub.MachineID(stateDir) }))
}

func newBackend(ctx context.Context, sessions sessions, relays native, identity func() (string, error)) *Backend {
	return &Backend{ctx: ctx, sessions: sessions, relays: relays, identity: identity, lanes: make(map[pair]*conversation)}
}

func (b *Backend) current(ctx context.Context, key registry.Key) (registry.Session, bool, error) {
	live, err := b.sessions.Live(ctx, key)
	if !live || err != nil {
		return registry.Session{}, false, err
	}
	s, err := b.sessions.Resolve(ctx, key.PID)
	return s, s.Key == key && err == nil, err
}

type refusal string

func (r refusal) Error() string { return string(r) }

func (c *conversation) resolveParent(ctx context.Context) (registry.Session, error) {
	if c.parent.PID != 0 {
		s, live, err := c.backend.current(ctx, c.parent)
		if live || err != nil {
			return s, err
		}
	}
	all, err := c.backend.sessions.List(ctx)
	if err != nil {
		return registry.Session{}, err
	}
	var parent registry.Session
	for _, s := range all {
		if s.SessionID != c.key.parent {
			continue
		}
		if parent.PID != 0 {
			return registry.Session{}, refusal(ambiguous)
		}
		parent = s
	}
	if parent.PID == 0 || parent.Key == c.child.Key {
		if c.parent.PID != 0 {
			return registry.Session{}, refusal(gone)
		}
		return registry.Session{}, refusal(notFound)
	}
	c.parent = parent.Key
	return parent, nil
}

// The native conversation outlives an MCP pipe. A reconnect can overlap the old pipe's close.
type registration struct{}

func (registration) Close() error { return nil }

func (b *Backend) Connect(ctx context.Context, child registry.Session, parentID string) (io.Closer, error) {
	key := pair{parentID, child.SessionID}
	for {
		b.mu.Lock()
		c := b.lanes[key]
		if c == nil {
			err := b.open(ctx, key, child)
			b.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return registration{}, nil
		}
		b.mu.Unlock()
		c.mu.Lock()
		if !c.retiring && c.child.Key == child.Key {
			_, err := c.resolveParent(ctx)
			c.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return registration{}, nil
		}
		if !c.retiring {
			_, live, err := b.current(ctx, c.child.Key)
			if err != nil || live {
				c.mu.Unlock()
				if err != nil {
					return nil, err
				}
				return nil, errors.New("another live child has that session id")
			}
			c.retiring = true
			c.mu.Unlock()
			c.close()
			continue
		}
		c.mu.Unlock()
		select {
		case <-c.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (b *Backend) open(ctx context.Context, key pair, child registry.Session) error {
	c := &conversation{backend: b, key: key, id: key.id(), child: child, done: make(chan struct{}), since: time.Now(), turns: make(map[string]*turn)}
	if _, err := c.resolveParent(ctx); err != nil {
		return err
	}
	id, err := b.identity()
	if err != nil {
		return err
	}
	path, err := relay.Path(filepath.Dir(child.SocketPath), "amesh-ln\n"+id, c.id)
	if err != nil {
		return err
	}
	c.address = envelope.Address(path)
	c.listener, err = b.relays.Listen(b.ctx, path, c.handle)
	if err != nil {
		return err
	}
	b.lanes[key] = c
	b.work.Go(c.watch)
	return nil
}

func (c *conversation) close() {
	if c.started && c.backend.ctx.Err() == nil {
		if err := c.notice(c.backend.ctx, fmt.Sprintf("Lane ended: %s exited. No reply needed.", c.child.Name)); err != nil {
			slog.Warn("lane ended notice", "error", err)
		}
	}
	_ = c.listener.Close()
	b := c.backend
	b.mu.Lock()
	delete(b.lanes, c.key)
	close(c.done)
	b.mu.Unlock()
}

func (b *Backend) SendToParent(ctx context.Context, child registry.Session, parentID, message string) (mcpserver.ParentResult, error) {
	if len(message) > 32*1024 {
		return mcpserver.ParentResult{Reason: new("Message exceeds 32 KiB.")}, nil
	}
	b.mu.Lock()
	c := b.lanes[pair{parentID, child.SessionID}]
	b.mu.Unlock()
	if c == nil {
		return mcpserver.ParentResult{Reason: new(notFound)}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if child.Key != c.child.Key || c.retiring {
		return mcpserver.ParentResult{Reason: new(notFound)}, nil
	}
	parent, err := c.resolveParent(ctx)
	if why, ok := errors.AsType[refusal](err); ok {
		return mcpserver.ParentResult{Reason: new(why.Error())}, nil
	}
	if err != nil {
		return mcpserver.ParentResult{}, err
	}
	id := uuid.NewString()
	err = c.write(ctx, parent, id, envelope.Message{FromName: child.Name, FromMode: child.FromMode, Body: message, HopChain: c.chain})
	if err != nil {
		return mcpserver.ParentResult{}, err
	}
	c.noteReply(child)
	return mcpserver.ParentResult{Accepted: true, To: new("parent"), ID: &id}, nil
}

func (c *conversation) write(ctx context.Context, to registry.Session, id string, msg envelope.Message) error {
	msg.From = c.address
	msg.FromSession = c.id
	return c.backend.relays.Write(ctx, relay.Delivery{Inbox: to.SocketPath, ID: id, Message: msg, ToLaneChild: to.Key == c.child.Key})
}

func (c *conversation) handle(ctx context.Context, e relay.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retiring {
		return nil
	}
	child, live, err := c.backend.current(ctx, c.child.Key)
	if err != nil {
		return err
	}
	if !live || child.SessionID != c.key.child {
		return errors.New("lane child has ended")
	}
	parent, err := c.resolveParent(ctx)
	if err != nil {
		return err
	}
	if e.Action == "peer_idle_notice" {
		if e.Peer.PID != child.PID {
			return errors.New("idle sender is outside this lane")
		}
		t := c.turns[e.OriginalID]
		delete(c.turns, e.OriginalID)
		if t == nil || t.replied {
			return nil
		}
		return c.notice(ctx, child.Name+" finished its turn and is idle. No reply needed.")
	}
	if e.Action != "user" {
		return nil
	}
	msg := e.Message
	switch e.Peer.PID {
	case parent.PID:
		msg.FromName = parent.Name
		if err = c.write(ctx, child, e.ID, msg); err == nil {
			c.chain = msg.HopChain
		}
		return err
	case child.PID:
		msg.FromName = child.Name
		if err = c.write(ctx, parent, e.ID, msg); err == nil {
			c.noteReply(child)
		}
		return err
	default:
		return errors.New("native sender is outside this lane")
	}
}

func noticeMode(s registry.Session) string {
	if s.FromMode == "bypass" {
		return "bypass"
	}
	return ""
}

func (c *conversation) notice(ctx context.Context, body string) error {
	parent, err := c.resolveParent(ctx)
	if err != nil {
		return err
	}
	msg := envelope.Message{From: c.address, FromSession: c.id, FromName: c.child.Name, FromMode: noticeMode(parent), Body: body}
	return c.backend.relays.Write(ctx, relay.Delivery{ID: uuid.NewString(), Inbox: parent.SocketPath, Message: msg, Notice: true})
}

func (c *conversation) noteReply(child registry.Session) {
	// A fast tool call can deliver before the poll first sees a busy turn.
	if !c.busy && child.Status == string(contract.PeerStatusBusy) {
		c.replied = true
		return
	}
	if t := c.turns[c.active]; t != nil {
		t.replied = true
	}
}

func (c *conversation) poll(ctx context.Context) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retiring {
		return false, nil
	}
	child, live, err := c.backend.current(ctx, c.child.Key)
	if err != nil {
		return false, err
	}
	if !live || child.SessionID != c.key.child {
		c.retiring = true
		return true, nil
	}
	c.child = child
	busy := child.Status == string(contract.PeerStatusBusy)
	if !c.started && (busy || time.Since(c.since) >= 3*time.Second) {
		body := fmt.Sprintf("Lane started: %s (a session you launched, working in %s). It can message you, and you can message it by sending to this message's from address. No reply needed.", child.Name, child.CWD)
		if err = c.notice(ctx, body); err != nil {
			return false, err
		}
		c.started = true
	}
	if busy && !c.busy {
		id := uuid.NewString()
		err = c.backend.relays.NotifyWhenIdle(ctx, child.SocketPath, c.address, noticeMode(child), id)
		if err != nil {
			return false, err
		}
		c.active = id
		c.turns[id] = &turn{replied: c.replied, expires: time.Now().Add(12 * time.Hour)}
		c.replied = false
	}
	c.busy = busy
	for id, t := range c.turns {
		if !time.Now().Before(t.expires) {
			delete(c.turns, id)
		}
	}
	return false, nil
}

func (c *conversation) watch() {
	ctx := c.backend.ctx
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ctx.Done():
			c.mu.Lock()
			retire := !c.retiring
			c.retiring = true
			c.mu.Unlock()
			if retire {
				c.close()
			}
			return
		case <-tick.C:
			retire, err := c.poll(ctx)
			if retire {
				c.close()
				return
			}
			if err != nil && ctx.Err() == nil && !errors.Is(err, registry.ErrEntryUnavailable) {
				slog.Warn("lane monitor", "lane", c.id, "error", err)
			}
		}
	}
}

func (b *Backend) Wait() { b.work.Wait() }

func (b *Backend) LaneStatus(ctx context.Context) []contract.DaemonStatusLanesItem {
	b.mu.Lock()
	conversations := slices.Collect(maps.Values(b.lanes))
	b.mu.Unlock()
	rows := make([]contract.DaemonStatusLanesItem, 0, len(conversations))
	for _, c := range conversations {
		c.mu.Lock()
		if !c.retiring {
			parent, _, _ := b.current(ctx, c.parent)
			rows = append(rows, contract.DaemonStatusLanesItem{
				ID: c.id, Relay: c.address,
				Parent: contract.DaemonStatusLanesItemParent{Name: parent.Name, SessionID: c.key.parent},
				Child:  contract.DaemonStatusLanesItemChild{Name: c.child.Name, SessionID: c.key.child, ConfigDir: c.child.ConfigDir},
			})
		}
		c.mu.Unlock()
	}
	slices.SortFunc(rows, func(a, b contract.DaemonStatusLanesItem) int { return strings.Compare(a.ID, b.ID) })
	return rows
}
