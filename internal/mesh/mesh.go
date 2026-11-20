package mesh

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/tenequm/amesh/internal/codex"
	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/state"
)

type Backend struct {
	threads          map[string]threadDelivery
	links            map[string]replyLink
	ctx              context.Context
	socketDir        string
	endpointMu       sync.Mutex
	endpoints        map[string]endpoint
	logMu            sync.Mutex
	resumeRewrites   func(string)
	log              string
	ack              func(context.Context, int64) error
	cfg              config.Config
	sessions         sessionRegistry
	relays           *relay.Manager
	client           *hub.Client
	store            *state.Store
	mu               sync.RWMutex
	members          map[string]member
	localHops        map[[2]string]int64 // same-machine sends per session pair since either side's last human turn
	wake             chan struct{}
	peers            []contract.Peer
	synchronized     bool
	directoryVersion uint64
	offlineSince     time.Time
	presence         sync.Mutex
	lastHeartbeat    time.Time
	presenceLog      string
	heartbeatDue     bool
}

type sessionRegistry interface {
	SelfCheck(context.Context) error
	Snapshot(context.Context) (registry.Listing, error)
	Resolve(context.Context, int) (registry.Session, error)
}

var _ mcpserver.MeshBackend = (*Backend)(nil)

func New(cfg config.Config, sessions *registry.Registry, relays *relay.Manager, client *hub.Client) *Backend {
	apps := make(map[string]threadDelivery, len(cfg.CodexHomes))
	for _, home := range cfg.CodexHomes {
		apps[home] = codex.New(home)
	}
	b := &Backend{threads: apps, links: map[string]replyLink{}, endpoints: make(map[string]endpoint), ack: client.Ack, cfg: cfg, sessions: sessions, relays: relays, client: client, members: make(map[string]member), localHops: make(map[[2]string]int64), wake: make(chan struct{}, 1)}
	if sessions != nil {
		sessions.CodexStatus = func(home, thread string) string {
			if app := apps[home]; app != nil {
				return app.Status(thread)
			}
			return ""
		}
		sessions.CodexComplete = b.completedThread
	}
	return b
}

func (b *Backend) HumanTurn(ctx context.Context, s registry.Session) error {
	key := s.MeshKey()
	if s.Codex && !b.newHumanTurn(key, s.HumanTurn) {
		return nil
	}
	current, live, err := b.liveSession(ctx, key)
	if err != nil || !live || current.SessionID != s.SessionID {
		return err
	}
	b.mu.Lock()
	b.forgetLocalHops(key)
	b.mu.Unlock()
	if _, halt := b.client.Connection(); !errors.Is(halt, hub.ErrNoHub) {
		req := contract.HumanTurnRequest{At: time.Now().UnixMilli(), Source: "prompt"}
		_, err = b.client.HumanTurn(ctx, key, req)
		if unknownSession(err) {
			if err = b.reregister(ctx, key); err == nil {
				_, err = b.client.HumanTurn(ctx, key, req)
			}
		}
		if err != nil {
			return err
		}
	}
	if s.Codex {
		b.mu.Lock()
		if m, ok := b.members[key]; ok && s.HumanTurn > m.humanTurn {
			m.humanTurn = s.HumanTurn
			b.members[key] = m
		}
		b.mu.Unlock()
	}
	return nil
}

// newHumanTurn compares Codex turn IDs as strings, which orders them because they are UUIDv7.
func (b *Backend) newHumanTurn(key, turn string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return turn != "" && turn > b.members[key].humanTurn
}

// forgetLocalHops clears every same-machine pair count that includes key. The caller holds b.mu.
func (b *Backend) forgetLocalHops(key string) {
	maps.DeleteFunc(b.localHops, func(pair [2]string, _ int64) bool { return slices.Contains(pair[:], key) })
}
