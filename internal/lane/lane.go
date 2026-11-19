package lane

import (
	"context"
	"errors"
	"io"

	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
)

type Backend struct{}

var _ mcpserver.LaneBackend = (*Backend)(nil)

func New(sessions *registry.Registry, relays *relay.Manager) *Backend { return &Backend{} }
func (*Backend) Connect(ctx context.Context, child registry.Session, parentID string) (io.Closer, error) {
	return nil, errors.ErrUnsupported
}

func (*Backend) SendToParent(ctx context.Context, child registry.Session, parentID, message string) (mcpserver.ParentResult, error) {
	return mcpserver.ParentResult{}, errors.ErrUnsupported
}
