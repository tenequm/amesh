package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
)

const (
	lineLimit = 1 << 20
	retention = 25 * time.Hour
	refill    = 2 * time.Second
	burst     = 30
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type wireMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type frame struct {
	Type         string       `json:"type"`
	Action       string       `json:"action,omitempty"`
	Message      *wireMessage `json:"message,omitempty"`
	Priority     string       `json:"priority,omitempty"`
	From         string       `json:"from"`
	Mode         string       `json:"from_mode,omitempty"`
	Version      int          `json:"msgV"`
	ID           string       `json:"msg_id"`
	OriginalID   string       `json:"orig_msg_id,omitempty"`
	Status       string       `json:"status,omitempty"`
	StatusDetail string       `json:"status_detail,omitempty"`
	DropReason   string       `json:"drop_reason,omitempty"`
	DroppedIDs   []string     `json:"dropped_msg_ids,omitempty"`
	Reason       string       `json:"reason,omitempty"`
	State        string       `json:"state,omitempty"`
	FinishedAt   int64        `json:"finished_at,omitempty"`
}

func (f frame) line() ([]byte, error) {
	if !uuidRE.MatchString(f.ID) || !strings.HasPrefix(f.From, "uds:") || len(f.From) <= 4 || len(f.From) > 300 {
		return nil, errors.New("invalid native frame id or sender")
	}
	b, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	if len(b) > lineLimit {
		n := 0
		for _, r := range string(b) {
			n += utf16.RuneLen(r)
			if n > lineLimit {
				return nil, errors.New("oversized native frame")
			}
		}
	}
	return append(b, '\n'), nil
}

type bucket struct {
	available time.Time // The time at which all tokens have refilled.
	last      time.Time
	body      string
}

func (b bucket) delay(now time.Time, body string) time.Duration {
	ready := b.available.Add(-(burst - 1) * refill)
	if body == b.body && !b.last.IsZero() && now.Sub(b.last) < 30*time.Second && b.last.Add(31*time.Second).After(ready) {
		ready = b.last.Add(31 * time.Second)
	}
	return max(0, ready.Sub(now))
}

func (b *bucket) sent(now time.Time, body string) {
	if b.available.Before(now) {
		b.available = now
	}
	b.available = b.available.Add(refill)
	b.last, b.body = now, body
}

type inbox struct {
	gate    chan struct{}
	users   int
	touched time.Time
	senders map[string]bucket
}

func (m *Manager) acquire(ctx context.Context, path string) (*inbox, func(), error) {
	m.mu.Lock()
	now := time.Now()
	for k, v := range m.inboxes {
		if v.users == 0 && now.Sub(v.touched) >= burst*refill {
			delete(m.inboxes, k)
		}
	}
	box := m.inboxes[path]
	if box == nil {
		box = &inbox{gate: make(chan struct{}, 1), senders: make(map[string]bucket)}
		m.inboxes[path] = box
	}
	box.users++
	m.mu.Unlock()
	put := func() {
		m.mu.Lock()
		box.users--
		box.touched = time.Now()
		m.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		put()
		return nil, nil, ctx.Err()
	case box.gate <- struct{}{}:
		return box, func() { <-box.gate; put() }, nil
	}
}

func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func (m *Manager) send(ctx context.Context, path string, line []byte, before func(peercred.Credentials) error) (int, error) {
	select {
	case m.slots <- struct{}{}:
		defer func() { <-m.slots }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	peer, err := peercred.Read(conn.(*net.UnixConn))
	if err != nil {
		return 0, err
	}
	if peer.PID <= 0 || int(peer.UID) != os.Geteuid() {
		return 0, errors.New("inbox peer credentials mismatch")
	}
	if before != nil {
		if err = before(peer); err != nil {
			return 0, err
		}
	}
	n, err := conn.Write(line)
	if err == nil && n != len(line) {
		err = io.ErrShortWrite
	}
	return n, err
}

// post releases a claim only after definite non-delivery. A partial write is ambiguous.
func (m *Manager) post(ctx context.Context, d Delivery, retry bool, before func(peercred.Credentials) error) (bool, error) {
	content, err := envelope.Render(d.Message)
	if err != nil {
		return false, err
	}
	f := frame{Type: "user", Message: &wireMessage{Role: "user", Content: content}, Priority: "next", From: d.Message.From, Version: 1, ID: d.ID}
	line, err := f.line()
	if err != nil {
		return false, err
	}
	box, release, err := m.acquire(ctx, d.Inbox)
	if err != nil {
		return false, err
	}
	defer release()
	if retry {
		if err = m.store.Forget(d.ID); err != nil {
			return false, err
		}
	}
	claimed, err := m.store.Claim(d.ID, time.Now().Add(retention))
	if err != nil || !claimed {
		return false, err
	}
	now := time.Now()
	for from, b := range box.senders {
		if now.Sub(b.last) >= burst*refill {
			delete(box.senders, from)
		}
	}
	b := box.senders[f.From]
	body := content[strings.IndexByte(content, '\n')+1:]
	if err = wait(ctx, b.delay(now, body)); err != nil {
		return false, errors.Join(err, m.store.Forget(d.ID))
	}
	n, err := m.send(ctx, d.Inbox, line, before)
	if err != nil && n == 0 {
		return false, errors.Join(err, m.store.Forget(d.ID))
	}
	b.sent(time.Now(), body)
	box.senders[f.From] = b
	return err == nil, err
}

func (m *Manager) Write(ctx context.Context, d Delivery) error {
	written, err := m.post(ctx, d, false, nil)
	if written && d.NotifyWhenIdle {
		return m.NotifyWhenIdle(ctx, d.Inbox, d.Message.From, d.Message.FromMode, d.ID)
	}
	return err
}

func (m *Manager) NotifyWhenIdle(ctx context.Context, inbox, from, fromMode, id string) error {
	if fromMode != "" && fromMode != "prompting" && fromMode != "bypass" {
		return errors.New("invalid idle subscription mode")
	}
	line, err := (frame{Type: "control", Action: "notify_when_idle", From: from, Mode: fromMode, Version: 1, ID: id}).line()
	if err != nil {
		return err
	}
	_, release, err := m.acquire(ctx, inbox)
	if err != nil {
		return err
	}
	defer release()
	_, err = m.send(ctx, inbox, line, nil)
	return err
}

func (m *Manager) WriteNotice(ctx context.Context, d Delivery, cause, peer, detail string) error {
	var body string
	switch cause {
	case "denied", "refused":
		body = fmt.Sprintf("your message to %s was not delivered: %s", peer, detail)
	case "expired", "dropped":
		body = fmt.Sprintf("your message to %s did not arrive: %s", peer, detail)
	case "outbox_expired":
		body = fmt.Sprintf("your message to %s was not sent: this machine could not reach the hub for 24 hours.", peer)
	case "limit":
		body = detail
	case "idle":
		body = peer + " finished the turn your message started and is idle."
	default:
		return fmt.Errorf("unknown notice cause %q", cause)
	}
	d.Message.FromMode, d.Message.HopChain = "", nil
	d.NotifyWhenIdle = false
	d.Message.Body = "amesh notice: " + body + " No reply needed."
	return m.Write(ctx, d)
}

const maxRewrites = 3

func (m *Manager) rewrite(ctx context.Context, d Delivery, peer peercred.Credentials) error {
	if err := wait(ctx, refill); err != nil {
		return err
	}
	_, err := m.post(ctx, d, true, func(next peercred.Credentials) error {
		if next != peer {
			return errors.New("inbox process changed before retry")
		}
		return nil
	})
	return err
}

func (e Event) retryable(rewrites int) bool {
	return rewrites < maxRewrites && e.Status == "dropped" && (e.DropReason == "rate-limited" || e.DropReason == "queue-full")
}
