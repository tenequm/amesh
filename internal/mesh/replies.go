package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
)

func (b *Backend) native(ctx context.Context, log, id string, e relay.Event) error {
	conn, err := b.client.Connection()
	if err != nil {
		return err
	}
	if conn.Log != log || e.Log != "" && e.Log != log {
		return errors.New("relay belongs to a previous hub log")
	}
	switch e.Action {
	case "peer_message_status":
		status := contract.DeliveryState(e.Status)
		if status == contract.DeliveryStateExpired && e.StatusDetail == contract.NativeReceiptStatusDetailRefused {
			status = contract.DeliveryStateRefused
		}
		return b.report(ctx, log, e.OriginalID, e.Session, status, contract.DropReason(e.DropReason))
	case "user":
	default:
		return errors.New("unknown relay action")
	}
	s, err := b.sessions.Resolve(ctx, e.Peer.PID)
	if err != nil {
		return err
	}
	key := sessionKey(s.Key)
	ep, ok := b.endpoint(id)
	if s.PID != e.Peer.PID || !b.IsMember(s.Key) || !b.eligible(s) || !ok || !slices.Contains(ep.parties, key) {
		return errors.New("relay sender is not a hosted party")
	}
	origin := contract.SendRequestOriginHuman
	if len(e.Message.HopChain) > 0 {
		origin = contract.SendRequestOriginPeer
	}
	req := contract.SendRequest{From: contract.SendRequestFrom{Session: key}, ID: e.ID, To: contract.SendRequestTo{Endpoint: &id}, Origin: origin, HopChain: e.Message.HopChain, Text: e.Message.Body, FromMode: optional(e.Message.FromMode)}
	_, err = b.client.SendReply(ctx, req, ep.ref)
	if errors.Is(err, hub.ErrQueued) {
		return nil
	}
	if errors.Is(err, hub.ErrIDConflict) {
		return b.outboxNotice(ctx, conn, hub.SendOutcome{Request: req, Peer: ep.ref.Peer, Name: ep.ref.Name, Err: err})
	}
	return err
}

type notice struct {
	seq                                                             int64
	session, source, kind, cause, endpoint, peer, detail, name, log string
}

func noticeEvent(item contract.EventStreamItem) (notice, error) {
	var n notice
	switch item.Event {
	case "delivery":
		var e contract.DeliveryEvent
		if err := json.Unmarshal([]byte(item.Data), &e); err != nil {
			return n, err
		}
		n = notice{seq: e.Seq, session: e.Session, endpoint: e.To.Endpoint, peer: e.To.Address, detail: e.Message}
		switch e.State {
		case contract.DeliveryStateRefused, contract.DeliveryStateDenied, contract.DeliveryStateExpired, contract.DeliveryStateDropped:
			n.cause = string(e.State)
		}
	case "limit":
		var e contract.LimitEvent
		if err := json.Unmarshal([]byte(item.Data), &e); err != nil {
			return n, err
		}
		n = notice{seq: e.Seq, session: e.Session, endpoint: e.Endpoint.ID, peer: e.Endpoint.Peer, detail: e.Message, cause: "limit"}
	case "idle":
		var e contract.IdleEvent
		if err := json.Unmarshal([]byte(item.Data), &e); err != nil {
			return n, err
		}
		n = notice{seq: e.Seq, session: e.Session, peer: e.Peer, cause: "idle"}
	}
	n.kind = item.Event
	n.source = strconv.FormatInt(n.seq, 10)
	return n, nil
}

func (b *Backend) writeNotice(ctx context.Context, conn contract.HelloResponse, n notice) error {
	if n.cause == "" {
		return nil
	}
	log := n.log
	if log == "" {
		log = conn.Log
	}
	var identityErr error
	if conn.Machine.ID == "" {
		var raw []byte
		raw, identityErr = os.ReadFile(filepath.Join(b.cfg.StateDir, "mesh-machine"))
		conn.Machine.ID = string(raw)
	}
	source := relay.NoticeSource{MachineID: conn.Machine.ID, Log: log, Source: n.source, Kind: n.kind}
	if identityErr == nil && b.store.NoticeWritten(source.ID()) {
		return nil
	}
	s, ok, err := b.deliverySession(ctx, n.session)
	if n.seq != 0 && errors.Is(err, registry.ErrEntryUnavailable) {
		return nil
	}
	if err != nil || !ok {
		return err
	}
	if identityErr != nil {
		return identityErr
	}
	mode := ""
	if s.FromMode == "bypass" {
		mode = s.FromMode
	}
	msg := envelope.Message{FromMode: mode}
	if n.kind == "idle" && n.endpoint == "" {
		b.endpointMu.Lock()
		for id, ep := range b.endpoints {
			if ep.ref.Peer == n.peer && slices.Contains(ep.parties, n.session) {
				if n.endpoint != "" {
					b.endpointMu.Unlock()
					return errors.New("idle notice has ambiguous endpoint")
				}
				n.endpoint = id
			}
		}
		b.endpointMu.Unlock()
	}
	if n.endpoint != "" || n.kind != "outbox" {
		ep, exists := b.endpoint(n.endpoint)
		if !exists && n.kind == "outbox" {
			path, e := relay.Path(b.socketDir, "amesh-relay\n"+conn.Machine.ID, n.endpoint)
			if e != nil {
				return e
			}
			ep = endpoint{ref: contract.EndpointRef{ID: n.endpoint, Name: n.name}, from: envelope.Address(path)}
		} else if !exists {
			return errors.New("missing conversation relay for notice")
		}
		msg.From = ep.from
		msg.FromSession = ep.ref.ID
		msg.FromName = ep.ref.Name
	}
	d := relay.Delivery{Inbox: s.SocketPath, Session: n.session, Log: conn.Log, Message: msg, AcceptWindow: time.Duration(conn.Settings.AcceptWindowMs) * time.Millisecond}
	return b.relays.WriteNotice(ctx, d, source, n.cause, n.peer, n.detail)
}

func (b *Backend) outboxNotice(ctx context.Context, conn contract.HelloResponse, out hub.SendOutcome) error {
	req, err := out.Request, out.Err
	if req.To.Endpoint != nil {
		if h, ok := errors.AsType[*hub.HTTPError](err); ok && h.Status == 409 && h.Body.Error == contract.ErrorCodeHopLimit {
			return nil
		}
	}
	if conn.Log == "" {
		conn.Log = out.NoticeLog
	}
	if conn.Log == "" {
		conn.Log = b.store.Log()
	}
	n := notice{log: out.NoticeLog, session: req.From.Session, source: req.ID, kind: "outbox", cause: "start_refused", endpoint: value(req.To.Endpoint), peer: value(req.To.Address), detail: "The send could not be completed."}
	if n.endpoint != "" {
		n.cause = "reply_refused"
		n.peer, n.name = out.Peer, out.Name
		// Pre-A2 outbox entries have only the endpoint ID when its snapshot is also gone.
		if n.peer == "" {
			n.peer = n.endpoint
		}
		if n.name == "" {
			n.name = n.peer
		}
		if ep, ok := b.endpoint(n.endpoint); ok {
			n.peer, n.name = ep.ref.Peer, ep.ref.Name
		}
	}
	if h, ok := errors.AsType[*hub.HTTPError](err); ok {
		n.detail = h.Body.Message
		switch h.Body.Error {
		case "log_replaced":
			n.cause = "outbox_replaced"
			n.endpoint = ""
		case "queue_expired":
			n.cause = "outbox_expired"
			n.endpoint = ""
		}
	}
	return b.writeNotice(ctx, conn, n)
}

func (b *Backend) completed(out hub.SendOutcome) error {
	if out.Err != nil && !out.NoticeRequired {
		return nil
	}
	conn, _ := b.client.Connection()
	if conn.Log != "" {
		if err := b.prepareLog(b.ctx, conn); err != nil {
			return err
		}
	}
	if out.Err != nil {
		return b.outboxNotice(b.ctx, conn, out)
	}
	_, err := b.bind(conn, out.Response.Message.Endpoint, []string{out.Request.From.Session}, false)
	return err
}
