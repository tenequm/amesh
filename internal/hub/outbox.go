package hub

import (
	"context"
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

// ErrQueued means the request is durable but sendWaitMs elapsed before a definitive answer.
var ErrQueued = errors.New("message queued in the local outbox")

type SendOutcome struct {
	Request  contract.SendRequest
	Response contract.SendResponse
	Err      error
}

type entry struct {
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

func (c *Client) enqueue(req contract.SendRequest, reply chan SendOutcome) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	q, err := c.readQueue()
	if err != nil {
		return err
	}
	for _, v := range q {
		if v.Request.ID != req.ID {
			continue
		}
		a, _ := json.Marshal(v.Request)
		b, _ := json.Marshal(req)
		if string(a) != string(b) {
			return &HTTPError{Status: 409, Body: contract.ErrorBody{Error: "id_conflict", Message: "Message id already queued with different contents."}}
		}
		c.waiters[req.ID] = append(c.waiters[req.ID], reply)
		return nil
	}
	if err = c.writeQueue(append(q, entry{time.Now().UnixMilli(), req})); err != nil {
		return err
	}
	c.waiters[req.ID] = append(c.waiters[req.ID], reply)
	return nil
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
	if err = c.enqueue(req, reply); err != nil {
		return empty, err
	}
	defer func() {
		c.queueMu.Lock()
		defer c.queueMu.Unlock()
		ws := slices.DeleteFunc(c.waiters[req.ID], func(w chan SendOutcome) bool { return w == reply })
		if len(ws) == 0 {
			delete(c.waiters, req.ID)
		} else {
			c.waiters[req.ID] = ws
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

func (c *Client) finish(out SendOutcome) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	q, err := c.readQueue()
	if err != nil {
		return err
	}
	if len(q) == 0 || q[0].Request.ID != out.Request.ID {
		return errors.New("outbox head changed during send")
	}
	if err = c.writeQueue(q[1:]); err != nil {
		return err
	}
	for _, w := range c.waiters[out.Request.ID] {
		w <- out
	}
	delete(c.waiters, out.Request.ID)
	return nil
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
		if len(q) == 0 || conn.Epoch == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.stopped:
				_, err = c.Connection()
				return err
			case <-c.wake:
				continue
			}
		}
		head := q[0]
		req := head.Request
		req.Epoch = conn.Epoch
		out := SendOutcome{Request: req}
		if time.Since(time.UnixMilli(head.At)) >= time.Duration(conn.Limits.QueueTTLMs)*time.Millisecond {
			out.Err = &HTTPError{Status: 410, Body: contract.ErrorBody{Error: "queue_expired", Message: "Message expired in the local outbox."}}
		} else {
			c.helloMu.RLock()
			current, _ := c.Connection()
			req.Epoch = current.Epoch
			out.Response, out.Err = rpc[contract.SendResponse](ctx, c, http.MethodPost, "/messages", req)
			c.helloMu.RUnlock()
			if out.Err == nil && out.Response.Message.ID != req.ID {
				out.Err = errors.New("hub returned another message id")
			}
		}
		if _, err = c.Connection(); err != nil {
			return err
		}
		var h *HTTPError
		if out.Err != nil && (!errors.As(out.Err, &h) || h.transient()) {
			if delay == 0 {
				delay = time.Duration(conn.Settings.BackoffInitialMs) * time.Millisecond
			}
			if err = c.pause(ctx, retryDelay(out.Err, delay)); err != nil {
				return err
			}
			delay = min(2*delay, time.Duration(conn.Settings.BackoffMaxMs)*time.Millisecond)
			continue
		}
		delay = 0
		if h != nil && h.Body.Error == "id_conflict" {
			slog.Error("hub message id conflict", "id", req.ID)
		}
		if err = c.finish(out); err != nil {
			return fmt.Errorf("settle outbox: %w", err)
		}
		if completed != nil {
			completed(out)
		}
	}
}
