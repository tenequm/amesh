package relay

import (
	"context"
	"errors"
	"io"

	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
	"github.com/tenequm/amesh/internal/state"
)

type (
	Manager  struct{}
	Delivery struct {
		Inbox, ID      string
		Message        envelope.Message
		NotifyWhenIdle bool
	}
)

type Event struct {
	Peer                                                     peercred.Credentials
	Action, ID, OriginalID                                   string
	From, FromMode, Status, StatusDetail, DropReason, Reason string
	FinishedAt                                               int64
	DroppedIDs                                               []string
	Message                                                  envelope.Message
}
type Handler func(context.Context, Event) error

func New(socketDir string, store *state.Store) *Manager { return &Manager{} }
func Path(socketDir, domain, id string) (string, error) { return "", errors.ErrUnsupported }
func (*Manager) Listen(ctx context.Context, path string, handle Handler) (io.Closer, error) {
	return nil, errors.ErrUnsupported
}
func (*Manager) Write(ctx context.Context, delivery Delivery) error { return errors.ErrUnsupported }
func (*Manager) NotifyWhenIdle(ctx context.Context, inbox, from, fromMode, id string) error {
	return errors.ErrUnsupported
}
