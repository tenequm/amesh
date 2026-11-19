package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/state"
)

type endpoint struct {
	ref      contract.EndpointRef
	parties  []string
	from     string
	listener io.Closer
}

func (b *Backend) bind(conn contract.HelloResponse, ref contract.EndpointRef, parties []string, replace bool) (endpoint, error) {
	b.endpointMu.Lock()
	defer b.endpointMu.Unlock()
	ep, ok := b.endpoints[ref.ID]
	if !ok {
		path, err := relay.Path(b.socketDir, "amesh-relay\n"+conn.Machine.ID, ref.ID)
		if err != nil {
			return ep, err
		}
		listener, err := b.relays.Listen(b.ctx, path, func(ctx context.Context, e relay.Event) error { return b.native(ctx, conn.Log, ref.ID, e) })
		if err != nil {
			return ep, err
		}
		ep.listener = listener
		ep.from = envelope.Address(path)
	}
	ep.ref = ref
	if replace {
		ep.parties = slices.Clone(parties)
	} else {
		ep.parties = slices.Clone(ep.parties)
		for _, p := range parties {
			if !slices.Contains(ep.parties, p) {
				ep.parties = append(ep.parties, p)
			}
		}
	}
	b.endpoints[ref.ID] = ep
	return ep, nil
}

func (b *Backend) endpoint(id string) (endpoint, bool) {
	b.endpointMu.Lock()
	defer b.endpointMu.Unlock()
	ep, ok := b.endpoints[id]
	return ep, ok
}

func (b *Backend) unbind(id string) error {
	b.endpointMu.Lock()
	ep, ok := b.endpoints[id]
	delete(b.endpoints, id)
	b.endpointMu.Unlock()
	if ok {
		return ep.listener.Close()
	}
	return nil
}

func (b *Backend) closeEndpoints() {
	b.endpointMu.Lock()
	eps := b.endpoints
	b.endpoints = map[string]endpoint{}
	b.endpointMu.Unlock()
	for _, ep := range eps {
		_ = ep.listener.Close()
	}
}

func (b *Backend) report(ctx context.Context, log, id, key string, status contract.DeliveryState, reason contract.DropReason) error {
	raw, err := json.Marshal(contract.StateReport{Kind: "state", ID: id, Session: key, State: string(status), Reason: optional(reason), At: time.Now().UnixMilli()})
	if err != nil {
		return err
	}
	res, err := b.client.Report(ctx, log, contract.ReportsRequest{Reports: []contract.Report{raw}})
	if err != nil {
		return err
	}
	if len(res.Results) != 1 || res.Results[0].ID != id {
		return errors.New("invalid hub report acknowledgement")
	}
	current := value(res.Results[0].State)
	switch current {
	case contract.DeliveryStateAccepted, contract.DeliveryStateDelivered, contract.DeliveryStateDenied, contract.DeliveryStateExpired, contract.DeliveryStateRefused, contract.DeliveryStateDropped, contract.DeliveryStateUnknown:
	case "":
		if res.Results[0].Applied {
			return errors.New("applied report has no state")
		}
	default:
		return nil
	}
	known, err := b.store.Contains(log, id)
	if err != nil || !known {
		return err
	}
	return b.store.MarkReported(log, id)
}

func (b *Backend) prepareLog(ctx context.Context, conn contract.HelloResponse) error {
	b.logMu.Lock()
	defer b.logMu.Unlock()
	if b.log == conn.Log && b.log != "" {
		return nil
	}
	current, _ := b.client.Connection()
	if current.Log != conn.Log {
		return errors.New("hub log changed during recovery")
	}
	if err := b.rememberMachine(conn.Machine.ID); err != nil {
		return err
	}
	if err := b.store.SetLog(conn.Log); err != nil {
		return err
	}
	if b.log != "" {
		b.closeEndpoints()
		b.mu.Lock()
		b.synchronized = false
		b.peers = nil
		b.directoryVersion++
		b.mu.Unlock()
		b.log = conn.Log
		return nil
	}
	entries, err := b.store.OpenEntries(conn.Log)
	if err != nil {
		return err
	}
	for id, e := range entries {
		status, reason := contract.DeliveryStateUnknown, contract.DropReason("")
		if e.Phase == state.Writing {
			status, reason = contract.DeliveryStateDropped, contract.DropReasonInterrupted
		}
		if err = b.report(ctx, conn.Log, id, e.Session, status, reason); err != nil {
			return err
		}
	}
	b.log = conn.Log
	return nil
}

func (b *Backend) deliverySession(ctx context.Context, key string) (registry.Session, bool, error) {
	s, live, err := b.liveSession(ctx, key)
	if !errors.Is(err, registry.ErrEntryUnavailable) || !registry.PendingEntry(err) {
		return s, live, err
	}
	// Give a concurrent registry write one presence interval to finish.
	select {
	case <-time.After(time.Second):
		return b.liveSession(ctx, key)
	case <-ctx.Done():
		return registry.Session{}, false, ctx.Err()
	}
}

func (b *Backend) receive(ctx context.Context, conn contract.HelloResponse, msg contract.MessageEvent) error {
	key := msg.Session
	if known, err := b.store.Contains(conn.Log, msg.ID); known || err != nil {
		return err
	}
	s, live, err := b.deliverySession(ctx, key)
	if errors.Is(err, registry.ErrEntryUnavailable) {
		return b.report(ctx, conn.Log, msg.ID, key, contract.DeliveryStateDropped, contract.DropReasonRouteFailed)
	}
	if err != nil {
		return err
	}
	if !live {
		if err = b.report(ctx, conn.Log, msg.ID, key, contract.DeliveryStateDropped, contract.DropReasonUnknownSession); err != nil {
			return err
		}
		b.presence.Lock()
		defer b.presence.Unlock()
		if _, hosted := b.member(key); hosted {
			b.changed()
			return nil
		}
		return b.end(ctx, key, contract.SessionEndRequestReasonNotHosted)
	}
	if time.Now().UnixMilli() >= msg.ExpiresAt {
		return b.report(ctx, conn.Log, msg.ID, key, contract.DeliveryStateDropped, contract.DropReasonQueueExpired)
	}
	ep, ok := b.endpoint(msg.Endpoint.ID)
	if !ok {
		return errors.New("message endpoint has no bound relay")
	}
	d := relay.Delivery{Inbox: s.SocketPath, ID: msg.ID, Session: key, Log: conn.Log, AcceptWindow: time.Duration(conn.Settings.AcceptWindowMs) * time.Millisecond, Message: envelope.Message{From: ep.from, FromSession: msg.Endpoint.ID, FromName: msg.Endpoint.Name, FromMode: value(msg.FromMode), HopChain: msg.HopChain, Body: msg.Text}}
	err = b.relays.Write(ctx, d)
	if err == nil || ctx.Err() != nil {
		return err
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) || errors.Is(err, syscall.EPIPE) || errors.Is(err, os.ErrNotExist) {
		return b.report(ctx, conn.Log, msg.ID, key, contract.DeliveryStateDropped, contract.DropReasonSessionGone)
	}
	if errors.Is(err, relay.ErrOversized) {
		return b.report(ctx, conn.Log, msg.ID, key, contract.DeliveryStateDropped, contract.DropReasonOversized)
	}
	return err
}

type streamRun struct {
	b            *Backend
	cancel       context.CancelCauseFunc
	wg           sync.WaitGroup
	mu           sync.Mutex
	tails        map[string]chan struct{}
	slots        chan struct{}
	connection   context.Context
	snapshot     []contract.Peer
	synchronized bool
}

func updatePeer(peers []contract.Peer, e contract.StatusEvent) []contract.Peer {
	for i, p := range peers {
		if p.Address == e.Peer.Address {
			if e.Listed {
				peers[i] = e.Peer
				return peers
			}
			return slices.Delete(peers, i, i+1)
		}
	}
	if e.Listed {
		return append(peers, e.Peer)
	}
	return peers
}

func (r *streamRun) enqueue(ctx context.Context, key string, seq int64, effect func() error) error {
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	done := make(chan struct{})
	r.mu.Lock()
	prev := r.tails[key]
	r.tails[key] = done
	r.mu.Unlock()
	r.wg.Go(func() {
		defer func() {
			r.mu.Lock()
			if r.tails[key] == done {
				delete(r.tails, key)
			}
			close(done)
			r.mu.Unlock()
			<-r.slots
		}()
		if prev != nil {
			select {
			case <-prev:
			case <-ctx.Done():
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		err := effect()
		if err == nil {
			err = r.b.ack(ctx, seq)
		}
		if err != nil {
			r.cancel(err)
		}
	})
	return nil
}

func (r *streamRun) handle(ctx context.Context, item contract.EventStreamItem) error {
	b := r.b
	conn, err := b.client.Connection()
	if err != nil {
		return err
	}
	if r.connection != ctx {
		r.connection = ctx
		r.snapshot = nil
		r.synchronized = false
		if err = b.prepareLog(ctx, conn); err != nil {
			return err
		}
	}
	// Snapshot endpoints precede the replay. Their relays must be listening for rewrite receipts.
	if b.resumeRewrites != nil && (item.Event != "endpoint" || item.ID != nil) {
		b.resumeRewrites(conn.Log)
		b.resumeRewrites = nil
	}
	data := []byte(item.Data)
	switch item.Event {
	case "endpoint":
		var e contract.EndpointEvent
		if err = json.Unmarshal(data, &e); err != nil {
			return err
		}
		switch e.State {
		case contract.EndpointEventStateClosed:
			r.wg.Wait()
			err = b.unbind(e.ID)
		case contract.EndpointEventStateOpen:
			_, err = b.bind(conn, contract.EndpointRef{ID: e.ID, Name: e.Name, Peer: e.Peer.Address}, e.Sessions, true)
		default:
			// Unknown endpoint states carry no v1 action.
		}
		if err == nil && item.ID != nil {
			err = b.ack(ctx, e.Seq)
		}
		return err
	case "status":
		var e contract.StatusEvent
		if err = json.Unmarshal(data, &e); err != nil {
			return err
		}
		if !r.synchronized {
			r.snapshot = updatePeer(r.snapshot, e)
		}
		b.mu.Lock()
		b.peers = updatePeer(b.peers, e)
		b.directoryVersion++
		b.mu.Unlock()
	case "synchronized":
		b.mu.Lock()
		b.synchronized = true
		b.peers = r.snapshot
		b.directoryVersion++
		b.mu.Unlock()
		r.synchronized = true
		b.connectivity(nil)
	case "message":
		var e contract.MessageEvent
		if err = json.Unmarshal(data, &e); err != nil {
			return err
		}
		return r.enqueue(ctx, e.Session, e.Seq, func() error { return b.receive(ctx, conn, e) })
	case "delivery", "limit", "idle":
		n, err := noticeEvent(item)
		if err != nil {
			return err
		}
		return r.enqueue(ctx, n.session, n.seq, func() error { return b.writeNotice(ctx, conn, n) })
	default:
		if item.ID != nil {
			n, e := strconv.ParseInt(*item.ID, 10, 64)
			if e != nil {
				return e
			}
			return fmt.Errorf("unhandled %s event at %d", item.Event, n)
		}
	}
	return nil
}

func (b *Backend) stream(ctx context.Context) error {
	for ctx.Err() == nil {
		child, cancel := context.WithCancelCause(ctx)
		r := streamRun{b: b, cancel: cancel, tails: map[string]chan struct{}{}, slots: make(chan struct{}, 256)}
		err := b.client.Stream(child, r.handle)
		cause := context.Cause(child)
		cancel(err)
		r.wg.Wait()
		if cause == nil || ctx.Err() != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return ctx.Err()
}
