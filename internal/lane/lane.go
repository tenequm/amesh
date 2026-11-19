package lane

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/tenequm/amesh/internal/envelope"
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
	ctx       context.Context
	sessions  sessions
	relays    native
	machineID string
	mu        sync.Mutex
	lanes     map[pair]*conversation
}

type pair struct{ parent, child string }

func (p pair) id() string {
	sum := sha256.Sum256([]byte("amesh-ln-id\n" + p.parent + "\n" + p.child))
	return fmt.Sprintf("ln_%x", sum[:8])
}

type conversation struct {
	backend     *Backend
	key         pair
	id, address string
	mu          sync.Mutex
	parent      registry.Key
	child       registry.Session
	chain       []string
	listener    io.Closer
	retiring    bool
	done        chan struct{}
}

var _ mcpserver.LaneBackend = (*Backend)(nil)

func New(ctx context.Context, sessions sessions, relays native, machineID string) *Backend {
	return &Backend{ctx: ctx, sessions: sessions, relays: relays, machineID: machineID, lanes: make(map[pair]*conversation)}
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
	c := &conversation{backend: b, key: key, id: key.id(), child: child, done: make(chan struct{})}
	if _, err := c.resolveParent(ctx); err != nil {
		return err
	}
	path, err := relay.Path(filepath.Dir(child.SocketPath), "amesh-ln\n"+b.machineID, c.id)
	if err != nil {
		return err
	}
	c.address = envelope.Address(path)
	c.listener, err = b.relays.Listen(b.ctx, path, c.handle)
	if err != nil {
		return err
	}
	b.lanes[key] = c
	return nil
}

func (c *conversation) close() {
	_ = c.listener.Close()
	b := c.backend
	b.mu.Lock()
	delete(b.lanes, c.key)
	close(c.done)
	b.mu.Unlock()
}

func (b *Backend) SendToParent(ctx context.Context, child registry.Session, parentID, message string) (mcpserver.ParentResult, error) {
	if len(message) > 32*1024 {
		return mcpserver.ParentResult{Reason: "Message exceeds 32 KiB."}, nil
	}
	b.mu.Lock()
	c := b.lanes[pair{parentID, child.SessionID}]
	b.mu.Unlock()
	if c == nil {
		return mcpserver.ParentResult{Reason: notFound}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if child.Key != c.child.Key || c.retiring {
		return mcpserver.ParentResult{Reason: notFound}, nil
	}
	parent, err := c.resolveParent(ctx)
	if why, ok := errors.AsType[refusal](err); ok {
		return mcpserver.ParentResult{Reason: why.Error()}, nil
	}
	if err != nil {
		return mcpserver.ParentResult{}, err
	}
	id := uuid.NewString()
	err = c.write(ctx, parent, id, envelope.Message{FromName: child.Name, FromMode: child.FromMode, Body: message, HopChain: c.chain})
	if err != nil {
		return mcpserver.ParentResult{}, err
	}
	return mcpserver.ParentResult{Accepted: true, To: "parent", ID: id}, nil
}

func (c *conversation) write(ctx context.Context, to registry.Session, id string, msg envelope.Message) error {
	msg.From = c.address
	msg.FromSession = c.id
	return c.backend.relays.Write(ctx, relay.Delivery{Inbox: to.SocketPath, ID: id, Message: msg})
}

func (c *conversation) handle(ctx context.Context, e relay.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retiring || e.Action != "user" {
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
		return c.write(ctx, parent, e.ID, msg)
	default:
		return errors.New("native sender is outside this lane")
	}
}
