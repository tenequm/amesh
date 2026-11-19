package hub

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/tenequm/amesh/internal/contract"
)

// ErrQueued means the send is durable and still pending. A paced send also carries its HTTPError.
var ErrQueued = errors.New("message queued in the local outbox")

type SendOutcome struct {
	Request  contract.SendRequest
	Response contract.SendResponse
	Err      error
}

type entry struct {
	Ticket  string               `json:"ticket"`
	Log     string               `json:"log"`
	Base    string               `json:"base"`
	At      int64                `json:"at"`
	Request contract.SendRequest `json:"request"`
}

func (c *Client) statePath(name string) (string, error) {
	id, err := c.key()
	if err != nil {
		return "", err
	}
	return filepath.Join(c.cfg.StateDir, id.id, name), nil
}

func (c *Client) readQueue() ([]entry, error) {
	path, err := c.statePath("outbox.json")
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var entries []entry
	err = json.Unmarshal(raw, &entries)
	return entries, err
}

func (c *Client) writeQueue(entries []entry) error {
	raw, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	path, err := c.statePath("outbox.json")
	if err != nil {
		return err
	}
	return saveFile(path, raw, false)
}

func (c *Client) signalOutbox() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) bindOutbox(log string) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	entries, err := c.readQueue()
	if err != nil {
		return err
	}
	changed := false
	for i := range entries {
		if entries[i].Log == "" && entries[i].Base == c.cfg.HubURL {
			entries[i].Log = log
			changed = true
		}
	}
	if changed {
		return c.writeQueue(entries)
	}
	return nil
}

func sameTarget(a, b *string) bool { return a == b || (a != nil && b != nil && *a == *b) }

func (c *Client) enqueue(req contract.SendRequest, reply chan SendOutcome) (string, error) {
	c.bindingMu.Lock()
	defer c.bindingMu.Unlock()
	if err := c.loadCursor(); err != nil {
		return "", err
	}
	c.mu.Lock()
	log := c.log
	c.mu.Unlock()
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	q, err := c.readQueue()
	if err != nil {
		return "", err
	}
	for _, v := range q {
		if v.Log != log || v.Request.ID != req.ID {
			continue
		}
		old := v.Request
		if old.From != req.From || old.Text != req.Text || !sameTarget(old.To.Address, req.To.Address) || !sameTarget(old.To.Endpoint, req.To.Endpoint) {
			return "", &HTTPError{Status: 409, Body: contract.ErrorBody{Error: "id_conflict", Message: "Message id already queued with different contents."}}
		}
		c.waiters[v.Ticket] = append(c.waiters[v.Ticket], reply)
		return v.Ticket, nil
	}
	e := entry{Ticket: crand.Text(), Base: c.cfg.HubURL, Log: log, At: time.Now().UnixMilli(), Request: req}
	if err = c.writeQueue(append(q, e)); err != nil {
		return "", err
	}
	c.waiters[e.Ticket] = append(c.waiters[e.Ticket], reply)
	return e.Ticket, nil
}

func (c *Client) Send(ctx context.Context, req contract.SendRequest) (contract.SendResponse, error) {
	var empty contract.SendResponse
	conn, err := c.Connection()
	if err != nil {
		return empty, err
	}
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	trimmed := strings.TrimSpace(req.Text)
	switch {
	case trimmed == "":
		return empty, &HTTPError{400, contract.ErrorBody{Error: "empty_text", Message: "Enter a message."}, 0}
	case strings.HasPrefix(trimmed, "/"):
		return empty, &HTTPError{400, contract.ErrorBody{Error: "slash_command", Message: "Slash commands cannot be sent."}, 0}
	case int64(len(req.Text)) > conn.Limits.MaxTextBytes:
		return empty, &HTTPError{413, contract.ErrorBody{Error: "too_large", Message: "Message exceeds the hub text limit."}, 0}
	}
	if req.ID == "" || !sessionKey.MatchString(req.From.Session) {
		return empty, errors.New("send requires a message id and session key")
	}
	req.Epoch = 0
	if req.HopChain == nil {
		req.HopChain = []string{}
	}
	reply := make(chan SendOutcome, 1)
	ticket, err := c.enqueue(req, reply)
	if err != nil {
		return empty, err
	}
	defer func() {
		c.queueMu.Lock()
		defer c.queueMu.Unlock()
		ws := slices.DeleteFunc(c.waiters[ticket], func(w chan SendOutcome) bool { return w == reply })
		if len(ws) == 0 {
			delete(c.waiters, ticket)
		} else {
			c.waiters[ticket] = ws
		}
	}()
	c.signalOutbox()
	timer := time.NewTimer(time.Duration(conn.Settings.SendWaitMs) * time.Millisecond)
	defer timer.Stop()
	select {
	case result := <-reply:
		return result.Response, result.Err
	case <-timer.C:
		return empty, ErrQueued
	case <-ctx.Done():
		return empty, errors.Join(ErrQueued, ctx.Err())
	case <-c.stopped:
		_, err = c.Connection()
		return empty, err
	}
}

// Pending reports how many durable sends remain for a session, or for all sessions if key is empty.
func (c *Client) Pending(key string) (int, error) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	q, err := c.readQueue()
	n := 0
	for _, v := range q {
		if key == "" || v.Request.From.Session == key {
			n++
		}
	}
	return n, err
}

func (c *Client) finish(ticket string, out SendOutcome) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	q, err := c.readQueue()
	if err != nil {
		return err
	}
	if len(q) == 0 || q[0].Ticket != ticket {
		return errors.New("outbox head changed during send")
	}
	if err = c.writeQueue(q[1:]); err != nil {
		return err
	}
	c.notify(ticket, out)
	return nil
}

func (c *Client) notify(ticket string, out SendOutcome) {
	for _, w := range c.waiters[ticket] {
		w <- out
	}
	delete(c.waiters, ticket)
}

func jitter(d time.Duration) time.Duration {
	// This jitter spreads reconnects; it is not a security token.
	return d/2 + time.Duration(rand.Int64N(max(1, int64(d-d/2)))) //nolint:gosec
}

func retryDelay(err error, d time.Duration) time.Duration {
	delay := jitter(d)
	if h, ok := errors.AsType[*HTTPError](err); ok {
		delay += h.RetryAfter
	}
	return delay
}

func (c *Client) pause(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stopped:
		_, err := c.Connection()
		return err
	case <-t.C:
		return nil
	}
}

func (c *Client) waitForOutbox(ctx context.Context, remaining time.Duration) error {
	var expiry <-chan time.Time
	if remaining > 0 {
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		expiry = timer.C
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stopped:
		_, err := c.Connection()
		return err
	case <-c.wake:
		return nil
	case <-expiry:
		return nil
	}
}

// RunOutbox owns the uploader for the lifetime of the daemon. completed receives every
// definitive outcome, including refusals and expiry for callers that have already returned.
func (c *Client) RunOutbox(ctx context.Context, completed func(SendOutcome)) error {
	select {
	case c.draining <- struct{}{}:
		defer func() { <-c.draining }()
	default:
		return errors.New("outbox already running")
	}
	var delay time.Duration
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		conn, err := c.Connection()
		if err != nil {
			return err
		}
		c.queueMu.Lock()
		q, err := c.readQueue()
		c.queueMu.Unlock()
		if err != nil {
			return err
		}
		var remaining time.Duration
		if len(q) > 0 {
			remaining = time.Duration(conn.Limits.QueueTTLMs)*time.Millisecond - time.Since(time.UnixMilli(q[0].At))
		}
		if len(q) == 0 || conn.Epoch == 0 && remaining > 0 {
			if err = c.waitForOutbox(ctx, remaining); err != nil {
				return err
			}
			continue
		}
		head := q[0]
		req := head.Request
		req.Epoch = conn.Epoch
		out := SendOutcome{Request: req}
		var rev uint64
		if remaining <= 0 {
			out.Err = &HTTPError{Status: 410, Body: contract.ErrorBody{Error: "queue_expired", Message: "Message expired in the local outbox."}}
		} else {
			c.helloMu.RLock()
			current, _ := c.Connection()
			req.Epoch = current.Epoch
			out.Request.Epoch = req.Epoch
			rev = c.helloCount
			if head.Log != current.Log {
				out.Err = &HTTPError{Status: 410, Body: contract.ErrorBody{Error: "log_replaced", Message: "The hub was replaced before the message was sent."}}
			} else {
				out.Response, out.Err = rpc[contract.SendResponse](ctx, c, http.MethodPost, "/messages", req)
			}
			c.helloMu.RUnlock()
			if out.Err == nil && out.Response.Message.ID != req.ID {
				out.Err = errors.New("hub returned another message id")
			}
		}
		if _, err = c.Connection(); err != nil {
			return err
		}
		if hasCode(out.Err, 409, "superseded") {
			if out.Err = c.confirmOwner(ctx, rev); out.Err == nil {
				continue
			}
			if _, halt := c.Connection(); halt != nil {
				return halt
			}
		}
		if hasCode(out.Err, 429, "rate_limited") {
			c.queueMu.Lock()
			c.notify(head.Ticket, SendOutcome{Request: req, Err: errors.Join(ErrQueued, out.Err)})
			c.queueMu.Unlock()
		}
		var h *HTTPError
		if out.Err != nil && (!errors.As(out.Err, &h) || h.transient()) {
			if delay == 0 {
				delay = time.Duration(conn.Settings.BackoffInitialMs) * time.Millisecond
			}
			if err = c.pause(ctx, min(remaining, retryDelay(out.Err, delay))); err != nil {
				return err
			}
			delay = min(2*delay, time.Duration(conn.Settings.BackoffMaxMs)*time.Millisecond)
			continue
		}
		delay = 0
		if h != nil && h.Body.Error == "id_conflict" {
			slog.Error("hub message id conflict", "id", req.ID)
		}
		if err = c.finish(head.Ticket, out); err != nil {
			return fmt.Errorf("settle outbox: %w", err)
		}
		if completed != nil {
			completed(out)
		}
	}
}
