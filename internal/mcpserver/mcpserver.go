package mcpserver

import (
	"context"
	"errors"
	"io"

	"github.com/tenequm/amesh/internal/registry"
)

type Caller func(context.Context) (registry.Session, error)

// The close handle belongs to this registration, including when another pipe replaces it.
type LaneBackend interface {
	Connect(ctx context.Context, child registry.Session, parentID string) (io.Closer, error)
	SendToParent(ctx context.Context, child registry.Session, parentID, message string) (ParentResult, error)
}

type MeshBackend interface {
	Connect(ctx context.Context, session registry.Session) (io.Closer, error)
	HumanTurn(ctx context.Context, session registry.Session) error
	ListPeers(ctx context.Context, session registry.Session, filter string) (Directory, error)
	SendMessage(ctx context.Context, session registry.Session, args SendArgs) (SendResult, error)
}

type SendArgs struct {
	To             string `json:"to"`
	Message        string `json:"message"`
	NotifyWhenIdle bool   `json:"notify_when_idle,omitempty"`
}

type ParentResult struct {
	Accepted bool   `json:"accepted"`
	To       string `json:"to,omitempty"`
	ID       string `json:"id,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

type SendResult struct {
	Accepted bool      `json:"accepted"`
	To       string    `json:"to,omitempty"`
	ID       string    `json:"id,omitempty"`
	Note     string    `json:"note,omitempty"`
	Reason   string    `json:"reason,omitempty"`
	Known    *[]string `json:"known,omitempty"`
}

type Identity struct {
	Name    string `json:"name"`
	Project string `json:"project"`
	Machine string `json:"machine"`
}

type Peer struct {
	Name     string  `json:"name"`
	Kind     string  `json:"kind"`
	Machine  *string `json:"machine"`
	Project  *string `json:"project"`
	Status   string  `json:"status"`
	LastSeen *string `json:"last_seen"`
}

type Directory struct {
	You   Identity `json:"you"`
	Peers []Peer   `json:"peers"`
	Stale string   `json:"stale,omitempty"`
}

func Serve(ctx context.Context, stream io.ReadWriteCloser, caller Caller, parentID string, reconnect bool, lane LaneBackend, mesh MeshBackend) error {
	return errors.ErrUnsupported
}
