package relay

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

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
		e      *endpoint
		key    subscription
		d      Delivery
		peer   peercred.Credentials
		events chan Event
		done   chan struct{}
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
	if event.Status == "dropped" {
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
	if event.Action == "peer_message_status" && (p.d.notice || p.d.lane()) {
		if event.Status != "accepted" {
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
	held, accepted := false, false
	rewrites := 0
	var processed []string
	for {
		select {
		case <-p.e.ctx.Done():
			return
		case <-timer.C:
			if !p.key.idle && !held && !accepted {
				p.report(Event{Action: "peer_message_status", OriginalID: p.d.ID, From: envelope.Address(p.d.Inbox), Peer: p.peer, Status: "accepted"})
				accepted = true
				timer.Reset(time.Until(written.Add(retention)))
				continue
			}
			return
		case event := <-p.events:
			if slices.Contains(processed, event.ID) {
				continue
			}
			if event.retryable(rewrites) {
				processed = append(processed, event.ID)
				rewrites++
				retried, err := p.e.manager.rewrite(p.e.ctx, p.d, p.peer)
				if err != nil {
					if p.e.ctx.Err() == nil && !accepted {
						event.DropReason = "session_gone"
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
				if p.d.lane() || p.d.notice {
					p.report(event)
				}
				if event.Status == "dropped" {
					return
				}
				continue
			}
			if event.Status == "held" {
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
