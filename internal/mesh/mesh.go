package mesh

import (
	"context"
	"sync"
	"time"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/state"
)

type Backend struct {
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
	return &Backend{endpoints: make(map[string]endpoint), ack: client.Ack, cfg: cfg, sessions: sessions, relays: relays, client: client, members: make(map[string]member), wake: make(chan struct{}, 1)}
}

func (b *Backend) HumanTurn(ctx context.Context, s registry.Session) error {
	key := sessionKey(s.Key)
	current, live, err := b.liveSession(ctx, key)
	if err != nil || !live || current.SessionID != s.SessionID {
		return err
	}
	req := contract.HumanTurnRequest{At: time.Now().UnixMilli(), Source: "prompt"}
	_, err = b.client.HumanTurn(ctx, key, req)
	if unknownSession(err) {
		if err = b.reregister(ctx, key); err == nil {
			_, err = b.client.HumanTurn(ctx, key, req)
		}
	}
	return err
}
