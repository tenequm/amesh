package control

import (
	"context"
	"errors"
	"io"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
)

func Serve(ctx context.Context, cfg config.Config, sessions *registry.Registry, lane mcpserver.LaneBackend, mesh mcpserver.MeshBackend) error {
	return errors.ErrUnsupported
}

func Pipe(ctx context.Context, socket string, args []string, in io.Reader, out io.Writer) error {
	return errors.ErrUnsupported
}

func Hook(ctx context.Context, socket string, in io.Reader, out io.Writer) error {
	return errors.ErrUnsupported
}

func Status(ctx context.Context, socket string, out io.Writer) error { return errors.ErrUnsupported }
