package relay

import (
	"context"
	"sync"
	"time"

	"github.com/tenequm/amesh/internal/contract"

	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
	"github.com/tenequm/amesh/internal/state"
)

type (
	Manager struct {
		socketDir string
		store     *state.Store
		mu        sync.Mutex
		inboxes   map[string]*inbox
		endpoints map[string]*endpoint
		slots     chan struct{}
	}
	Delivery struct {
		Inbox, ID string
		// Hub deliveries carry the log ID and receiving session key for startup reports.
		Log, Session   string
		AcceptWindow   time.Duration
		Notice         bool
		ToLaneChild    bool
		Message        envelope.Message
		NotifyWhenIdle bool
		IdleFromMode   string // Receiver mode for the optional idle subscription.
	}
)

type Event struct {
	Peer                                 peercred.Credentials
	Action, ID, OriginalID, Log, Session string
	From, FromMode, Reason               string
	Status                               contract.NativeReceiptStatus
	StatusDetail                         contract.NativeReceiptStatusDetail
	DropReason                           contract.NativeReceiptDropReason
	FinishedAt                           int64
	DroppedIDs                           []string
	Message                              envelope.Message
}
type Handler func(context.Context, Event) error

func New(socketDir string, store *state.Store) *Manager {
	return &Manager{socketDir: socketDir, store: store, inboxes: make(map[string]*inbox), slots: make(chan struct{}, 32), endpoints: make(map[string]*endpoint)}
}

func (m *Manager) OwnsRelay(addr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.endpoints[addr]
	return e != nil && e.ctx.Err() == nil
}
