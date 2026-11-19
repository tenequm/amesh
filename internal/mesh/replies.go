package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/hub"
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
		status := e.Status
		if status == "expired" && e.StatusDetail == "refused" {
			status = "refused"
		}
		return b.report(ctx, log, e.OriginalID, e.Session, status, e.DropReason)
	case "user", "peer_idle_notice":
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
	if e.Action == "peer_idle_notice" {
		at := e.FinishedAt
		if at == 0 {
			at = time.Now().UnixMilli()
		}
		raw, marshalErr := json.Marshal(contract.IdleReport{Kind: "idle", ID: e.OriginalID, Session: key, At: at})
		if marshalErr != nil {
			return marshalErr
		}
		_, err = b.client.Report(ctx, contract.ReportsRequest{Reports: []contract.Report{raw}})
		return err
	}
	origin := "human"
	if len(e.Message.HopChain) > 0 {
		origin = "peer"
	}
	req := contract.SendRequest{From: contract.SendRequestFrom{Session: key}, ID: e.ID, To: contract.SendRequestTo{Endpoint: &id}, Origin: origin, HopChain: e.Message.HopChain, Text: e.Message.Body, FromMode: optional(e.Message.FromMode)}
	_, err = b.send(ctx, req)
	if err != nil && !errors.Is(err, hub.ErrQueued) {
		return b.outboxNotice(ctx, conn, req, err)
	}
	return nil
}

type notice struct {
	seq                                                  int64
	session, source, kind, cause, endpoint, peer, detail string
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
		case "refused", "denied", "expired", "dropped":
			n.cause = e.State
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
	s, ok, err := b.liveSession(ctx, n.session)
	if err != nil || !ok {
		return err
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
		if !exists {
			return errors.New("missing conversation relay for notice")
		}
		msg.From = ep.from
		msg.FromSession = ep.ref.ID
		msg.FromName = ep.ref.Name
	}
	d := relay.Delivery{Inbox: s.SocketPath, Log: conn.Log, Message: msg, AcceptWindow: time.Duration(conn.Settings.AcceptWindowMs) * time.Millisecond}
	return b.relays.WriteNotice(ctx, d, relay.NoticeSource{MachineID: conn.Machine.ID, Source: n.source, Kind: n.kind}, n.cause, n.peer, n.detail)
}

func (b *Backend) outboxNotice(ctx context.Context, conn contract.HelloResponse, req contract.SendRequest, err error) error {
	if conn.Machine.ID == "" {
		raw, readErr := os.ReadFile(filepath.Join(b.cfg.StateDir, "mesh-machine"))
		if readErr != nil {
			return readErr
		}
		conn.Machine.ID = string(raw)
	}
	n := notice{session: req.From.Session, source: req.ID, kind: "outbox", cause: "start_refused", endpoint: value(req.To.Endpoint), peer: value(req.To.Address), detail: "The send could not be completed."}
	if n.endpoint != "" {
		if ep, ok := b.endpoint(n.endpoint); ok {
			n.peer = ep.ref.Peer
		} else {
			n.peer = "the peer"
			n.endpoint = ""
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
		case "hop_limit":
			if n.endpoint != "" {
				n.cause = "limit"
			}
		}
	}
	return b.writeNotice(ctx, conn, n)
}

// Send and the outbox callback can finish in either order. Only a caller that
// already returned a queued result (or disappeared on restart) needs a later notice.
type attempt struct {
	done, captured chan struct{}
	queued, retry  bool
}

func (b *Backend) sendOnce(ctx context.Context, req contract.SendRequest, retry bool) (contract.SendResponse, error) {
	a := &attempt{done: make(chan struct{}), captured: make(chan struct{}), retry: retry}
	b.attemptMu.Lock()
	b.attempts[req.ID] = append(b.attempts[req.ID], a)
	b.attemptMu.Unlock()
	res, err := b.client.Send(ctx, req)
	a.queued = errors.Is(err, hub.ErrQueued)
	close(a.done)
	if unknownSession(err) {
		// The retry has the same id. Let the uploader detach this attempt first.
		select {
		case <-a.captured:
		case <-ctx.Done():
			return res, ctx.Err()
		}
	}
	if err != nil && !a.queued {
		h, remote := errors.AsType[*hub.HTTPError](err)
		if !remote || h.Body.Error == "empty_text" || h.Body.Error == "slash_command" || h.Body.Error == "too_large" || h.Body.Error == "id_conflict" {
			b.attemptMu.Lock()
			rest := slices.DeleteFunc(b.attempts[req.ID], func(old *attempt) bool { return old == a })
			if len(rest) == 0 {
				delete(b.attempts, req.ID)
			} else {
				b.attempts[req.ID] = rest
			}
			b.attemptMu.Unlock()
		}
	}
	if err == nil {
		conn, _ := b.client.Connection()
		_, err = b.bind(conn, res.Message.Endpoint, []string{req.From.Session}, false)
		if err != nil {
			slog.Error("could not bind accepted send's relay", "error", err)
			err = nil
		}
	}
	return res, err
}

func (b *Backend) completed(out hub.SendOutcome) {
	req := out.Request
	b.attemptMu.Lock()
	attempts := b.attempts[req.ID]
	delete(b.attempts, req.ID)
	b.attemptMu.Unlock()
	for _, a := range attempts {
		close(a.captured)
	}
	retry := true
	for _, a := range attempts {
		<-a.done
		if !a.queued {
			return
		}
		retry = retry && a.retry
	}
	conn, _ := b.client.Connection()
	if conn.Log != "" {
		if err := b.prepareLog(b.ctx, conn); err != nil {
			slog.Error("preparing hub log for outbox completion", "error", err)
			return
		}
	}
	if out.Err == nil {
		if _, err := b.bind(conn, out.Response.Message.Endpoint, []string{req.From.Session}, false); err != nil {
			slog.Error("binding queued send relay", "error", err)
		}
		return
	}
	if unknownSession(out.Err) && retry {
		// RunOutbox must be free to post and settle the retry.
		b.retries.Go(func() {
			err := b.reregister(b.ctx, req.From.Session)
			if err == nil {
				_, err = b.sendOnce(b.ctx, req, false)
			}
			if err != nil && !errors.Is(err, hub.ErrQueued) && b.ctx.Err() == nil {
				if e := b.outboxNotice(b.ctx, conn, req, err); e != nil {
					slog.Error("writing send refusal", "error", e)
				}
			}
		})
		return
	}
	if err := b.outboxNotice(b.ctx, conn, req, out.Err); err != nil && b.ctx.Err() == nil {
		slog.Error("writing outbox notice", "error", err)
	}
}
