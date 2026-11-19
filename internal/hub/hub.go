package hub

import (
	"context"
	"errors"
	"io"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
)

type Client struct{}

func New(cfg config.Config) *Client { return &Client{} }
func Enroll(ctx context.Context, cfg config.Config, out io.Writer) error {
	return errors.ErrUnsupported
}

func (*Client) Stream(ctx context.Context, handle func(context.Context, contract.EventStreamItem) error) error {
	return errors.ErrUnsupported
}
func (*Client) Ack(ctx context.Context, seq int64) error { return errors.ErrUnsupported }
func (*Client) Hello(ctx context.Context, request contract.HelloRequest) (contract.HelloResponse, error) {
	return contract.HelloResponse{}, errors.ErrUnsupported
}

func (*Client) Peers(ctx context.Context) (contract.PeersResponse, error) {
	return contract.PeersResponse{}, errors.ErrUnsupported
}

func (*Client) Register(ctx context.Context, key string, request contract.SessionRegisterRequest) (contract.SessionRegisterResponse, error) {
	return contract.SessionRegisterResponse{}, errors.ErrUnsupported
}

func (*Client) Heartbeat(ctx context.Context, request contract.HeartbeatRequest) (contract.HeartbeatResponse, error) {
	return contract.HeartbeatResponse{}, errors.ErrUnsupported
}

func (*Client) HumanTurn(ctx context.Context, key string, request contract.HumanTurnRequest) (contract.HumanTurnResponse, error) {
	return contract.HumanTurnResponse{}, errors.ErrUnsupported
}

func (*Client) End(ctx context.Context, key string, request contract.SessionEndRequest) (contract.SessionEndResponse, error) {
	return contract.SessionEndResponse{}, errors.ErrUnsupported
}

func (*Client) Send(ctx context.Context, request contract.SendRequest) (contract.SendResponse, error) {
	return contract.SendResponse{}, errors.ErrUnsupported
}

func (*Client) Report(ctx context.Context, request contract.ReportsRequest) (contract.ReportsResponse, error) {
	return contract.ReportsResponse{}, errors.ErrUnsupported
}
