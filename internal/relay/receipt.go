package relay

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/tenequm/amesh/internal/contract"

	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/state"

	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
)

const acceptWindow = 8 * time.Second

type (
	subscription struct {
		id   string
		idle bool
	}
	pending struct {
		e        *endpoint
		key      subscription
		d        Delivery
		rewrites int
		accepted bool
		peer     peercred.Credentials
		events   chan Event
		done     chan struct{}
	}
)

func (m *Manager) watch(d Delivery, idle bool) *pending {
	m.mu.Lock()
	e := m.endpoints[d.Message.From]
	m.mu.Unlock()
	return &pending{e: e, d: d, key: subscription{d.ID, idle}}
}

func (p *pending) register(peer peercred.Credentials) error {
	e := p.e
	if e == nil {
		return nil
	} // Direct same-machine delivery has no relay for receipts.
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing || e.ctx.Err() != nil {
		return context.Canceled
	}
	if e.pending[p.key] != nil {
		return errors.New("duplicate active receipt subscription")
	}
	p.peer = peer
	p.events = make(chan Event, 8)
	p.done = make(chan struct{})
	e.pending[p.key] = p
	e.wg.Add(1)
	return nil
}

func (p *pending) remove() {
	p.e.mu.Lock()
	delete(p.e.pending, p.key)
	close(p.done)
	p.e.mu.Unlock()
	p.e.wg.Done()
}

func (p *pending) finish(written time.Time, err error) {
	if p.events == nil {
		return
	}
	if err != nil {
		p.remove()
		return
	}
	go func() { defer p.remove(); p.run(written) }()
}

func (e *endpoint) route(event Event) error {
	ids := []string{event.OriginalID}
	if event.Status == contract.NativeReceiptStatusDropped {
		ids = append(ids, event.DroppedIDs...)
	}
	seen := make(map[string]bool)
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		e.mu.Lock()
		p := e.pending[subscription{id, event.Action == "peer_idle_notice"}]
		e.mu.Unlock()
		if p == nil || p.peer != event.Peer {
			continue
		}
		event.OriginalID = id
		event.DroppedIDs = nil // Each correlated ID receives its own event.
		select {
		case p.events <- event:
		case <-p.done:
		case <-e.ctx.Done():
			return e.ctx.Err()
		}
	}
	return nil
}

func (p *pending) report(event Event) {
	event.Log, event.Session = p.d.Log, p.d.Session
	if event.Action == "peer_message_status" && (p.d.Notice || p.d.lane()) {
		if event.Status != contract.NativeReceiptStatus(contract.DeliveryStateAccepted) {
			slog.Info("local native delivery receipt", "id", p.d.ID, "status", event.Status, "drop_reason", event.DropReason)
		}
		return
	}
	if err := p.e.handle(p.e.ctx, event); err != nil && p.e.ctx.Err() == nil {
		slog.Error("native delivery handler failed", "error", err)
	}
}

func (p *pending) run(written time.Time) {
	window := p.d.AcceptWindow
	if window <= 0 {
		window = acceptWindow
	}
	if p.key.idle {
		window = 12 * time.Hour
	}
	timer := time.NewTimer(time.Until(written.Add(window)))
	defer timer.Stop()
	held, accepted := false, p.accepted
	if accepted {
		timer.Reset(retention)
	}
	rewrites := p.rewrites
	var processed []string
	for {
		select {
		case <-p.e.ctx.Done():
			return
		case <-timer.C:
			if !p.key.idle && !held && !accepted {
				p.report(Event{Action: "peer_message_status", OriginalID: p.d.ID, From: envelope.Address(p.d.Inbox), Peer: p.peer, Status: contract.NativeReceiptStatus(contract.DeliveryStateAccepted)})
				accepted = true
				timer.Reset(time.Until(written.Add(retention)))
				continue
			}
			return
		case event := <-p.events:
			if slices.Contains(processed, event.ID) {
				continue
			}
			if !p.key.idle {
				switch event.Status {
				case contract.NativeReceiptStatusHeld, contract.NativeReceiptStatusDelivered, contract.NativeReceiptStatusDenied, contract.NativeReceiptStatusExpired, contract.NativeReceiptStatusDropped:
				default:
					continue
				}
			}
			if event.retryable(rewrites) {
				processed = append(processed, event.ID)
				rewrites++
				retried, err := p.rewrite(rewrites, accepted)
				if err != nil {
					if p.e.ctx.Err() == nil && !accepted {
						// A final drop must not leave a payload for ResumeRewrites to write after restart.
						if clearErr := p.e.manager.store.SetRewrite(p.d.ID, nil); clearErr != nil {
							slog.Error("clear pending rewrite", "id", p.d.ID, "err", clearErr)
						}
						event.DropReason = contract.NativeReceiptDropReason(contract.DropReasonSessionGone)
						p.report(event)
					}
					return
				}
				if !accepted && !held {
					timer.Reset(time.Until(retried.Add(window)))
				}
				continue
			}
			if accepted {
				if p.d.lane() || p.d.Notice {
					p.report(event)
				}
				if event.Status == contract.NativeReceiptStatusDropped {
					return
				}
				continue
			}
			if event.Status == contract.NativeReceiptStatusHeld {
				if !held {
					p.report(event)
					held = true
					timer.Reset(retention)
				}
				continue
			}
			p.report(event)
			return
		}
	}
}

type rewriteRecord struct {
	Inbox       string        `json:"inbox"`
	ToLaneChild bool          `json:"toLaneChild,omitempty"`
	Content     string        `json:"content"`
	PID         int           `json:"pid"`
	UID         uint32        `json:"uid"`
	Start       string        `json:"procStart"`
	Rewrites    int           `json:"rewrites"`
	Accepted    bool          `json:"accepted"`
	Window      time.Duration `json:"acceptWindow"`
	Ready       time.Time     `json:"ready"`
}

func (p *pending) rewrite(count int, accepted bool) (time.Time, error) {
	start, err := registry.ProcessStart(p.peer.PID)
	if err != nil {
		return time.Time{}, err
	}
	content, err := envelope.Render(p.d.Message)
	if err != nil {
		return time.Time{}, err
	}
	record := rewriteRecord{Inbox: p.d.Inbox, ToLaneChild: p.d.ToLaneChild, Content: content, PID: p.peer.PID, UID: p.peer.UID, Window: p.d.AcceptWindow, Start: start, Rewrites: count, Accepted: accepted, Ready: time.Now().Add(duplicateDelay)}
	raw, err := json.Marshal(record)
	if err != nil {
		return time.Time{}, err
	}
	m := p.e.manager
	if err = m.store.SetRewrite(p.d.ID, raw); err != nil {
		return time.Time{}, err
	}
	return m.rewrite(p.e.ctx, p.d, p.peer)
}

// ResumeRewrites is started once under the daemon lock. Only confirmed drops retain their payload.
func (m *Manager) ResumeRewrites(ctx context.Context, pending map[string]state.Entry, hosted func(context.Context, string, int, string) bool) {
	var workers sync.WaitGroup
	for id, entry := range pending {
		workers.Go(func() {
			var record rewriteRecord
			if err := json.Unmarshal(entry.Pending, &record); err != nil {
				slog.Error("invalid pending rewrite", "id", id)
				return
			}
			if wait(ctx, time.Until(record.Ready)) != nil {
				return
			}
			msg, err := envelope.Parse(record.Content)
			if err != nil {
				slog.Error("invalid pending rewrite envelope", "id", id)
				return
			}
			d := Delivery{Inbox: record.Inbox, ToLaneChild: record.ToLaneChild, ID: id, Message: msg, Session: entry.Session, Log: entry.Log, AcceptWindow: record.Window, Notice: entry.Kind == "notice"}
			if entry.Session != "" && !hosted(ctx, entry.Session, record.PID, record.Inbox) {
				return
			}
			p := m.watch(d, false)
			p.rewrites, p.accepted = record.Rewrites, record.Accepted || entry.Phase == state.Reported
			written, err := m.post(ctx, d, true, func(peer peercred.Credentials) error {
				start, lookupErr := registry.ProcessStart(peer.PID)
				if lookupErr != nil || peer.PID != record.PID || peer.UID != record.UID || start != record.Start {
					return errors.New("pending rewrite recipient changed")
				}
				return p.register(peer)
			})
			p.finish(written, err)
			if err != nil && ctx.Err() == nil {
				slog.Warn("pending inbox rewrite failed", "id", id, "error", err)
			}
		})
	}
	workers.Wait()
}
