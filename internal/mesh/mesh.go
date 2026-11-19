package mesh

import (
	"context"
	"errors"
	"io"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
)

type Backend struct{}

var _ mcpserver.MeshBackend = (*Backend)(nil)

func New(cfg config.Config, sessions *registry.Registry, relays *relay.Manager, client *hub.Client) *Backend {
	return &Backend{}
}

func (*Backend) Connect(ctx context.Context, session registry.Session) (io.Closer, error) {
	return nil, errors.ErrUnsupported
}

func (*Backend) HumanTurn(ctx context.Context, session registry.Session) error {
	return errors.ErrUnsupported
}

func (*Backend) ListPeers(ctx context.Context, session registry.Session, filter string) (mcpserver.Directory, error) {
	return mcpserver.Directory{}, errors.ErrUnsupported
}

func (*Backend) SendMessage(ctx context.Context, session registry.Session, args mcpserver.SendArgs) (mcpserver.SendResult, error) {
	return mcpserver.SendResult{}, errors.ErrUnsupported
}
