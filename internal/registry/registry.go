package registry

import (
	"context"
	"errors"
)

type Key struct {
	PID       int
	ProcStart string
}

type Session struct {
	Key
	SessionID, Name, Project, CWD, ConfigDir, SocketPath, Status, FromMode string
}
type Registry struct{}

func New(configDirs []string) *Registry { return &Registry{} }
func (*Registry) Resolve(ctx context.Context, peerPID int) (Session, error) {
	return Session{}, errors.ErrUnsupported
}
func (*Registry) List(ctx context.Context) ([]Session, error) { return nil, errors.ErrUnsupported }
func (*Registry) SelfCheck(ctx context.Context) error         { return errors.ErrUnsupported }
