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
	"slices"
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
	if !uuidRE.MatchString(f.ID) || !validFrom(f.From) {
		return nil, errors.New("invalid native frame id or sender")
	}
	b, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	if oversized(b) {
		return nil, errors.New("oversized native frame")
	}
	return append(b, '\n'), nil
}

var fromRE = regexp.MustCompile(`^uds:[A-Za-z0-9%:_/.-]+$`)

func validFrom(s string) bool { return len(s) <= 300 && fromRE.MatchString(s) }

func oversized(b []byte) bool {
	if len(b) <= lineLimit {
		return false
	}
	n := 0
	for _, r := range string(b) {
		n += utf16.RuneLen(r)
		if n > lineLimit {
			return true
		}
	}
	return false
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
	pacing  bucket
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
		box = &inbox{gate: make(chan struct{}, 1)}
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

func (m *Manager) send(ctx context.Context, path string, line []byte, before func(peercred.Credentials) error) (int, time.Time, error) {
	select {
	case m.slots <- struct{}{}:
		defer func() { <-m.slots }()
	case <-ctx.Done():
		return 0, time.Time{}, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return 0, time.Time{}, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	peer, err := peercred.Read(conn.(*net.UnixConn))
	if err != nil {
		return 0, time.Time{}, err
	}
	if peer.PID <= 0 || int(peer.UID) != os.Geteuid() {
		return 0, time.Time{}, errors.New("inbox peer credentials mismatch")
	}
	if before != nil {
		if err = before(peer); err != nil {
			return 0, time.Time{}, err
		}
	}
	n, err := conn.Write(line)
	written := time.Now()
	if err == nil && n != len(line) {
		err = io.ErrShortWrite
	}
	if err == nil {
		// Darwin can lose peer credentials on full close before the receiver accepts.
		// EOF ends this frame while the read half keeps the posting process identifiable.
		unixConn := conn.(*net.UnixConn)
		if unixConn.CloseWrite() == nil && conn.SetReadDeadline(time.Now().Add(time.Second)) == nil {
			var discard [1]byte
			_, _ = conn.Read(discard[:])
		}
	}
	return n, written, err
}

// post releases a claim only after definite non-delivery. A partial write is ambiguous.
func (m *Manager) post(ctx context.Context, d Delivery, retry bool, before func(peercred.Credentials) error) (time.Time, error) {
	content, err := envelope.Render(d.Message)
	if err != nil {
		return time.Time{}, err
	}
	f := frame{Type: "user", Message: &wireMessage{Role: "user", Content: content}, Priority: "next", From: d.Message.From, Version: 1, ID: d.ID}
	line, err := f.line()
	if err != nil {
		return time.Time{}, err
	}
	pair, release, err := m.acquire(ctx, d.Inbox+"\x00"+f.From)
	if err != nil {
		return time.Time{}, err
	}
	defer release()
	if retry {
		if err = m.store.Forget(d.ID); err != nil {
			return time.Time{}, err
		}
	}
	claimed, err := m.store.Claim(d.ID, time.Now().Add(retention))
	if err != nil || !claimed {
		return time.Time{}, err
	}
	body := content[strings.IndexByte(content, '\n')+1:]
	if err = wait(ctx, pair.pacing.delay(time.Now(), body)); err != nil {
		return time.Time{}, errors.Join(err, m.store.Forget(d.ID))
	}
	_, unlockInbox, err := m.acquire(ctx, d.Inbox)
	if err != nil {
		return time.Time{}, errors.Join(err, m.store.Forget(d.ID))
	}
	defer unlockInbox()
	n, written, err := m.send(ctx, d.Inbox, line, before)
	if err != nil && n == 0 {
		return time.Time{}, errors.Join(err, m.store.Forget(d.ID))
	}
	pair.pacing.sent(written, body)
	return written, err
}

func (m *Manager) Write(ctx context.Context, d Delivery) error {
	d.Message.HopChain = slices.Clone(d.Message.HopChain)
	watch := m.watch(d, false)
	written, err := m.post(ctx, d, false, watch.register)
	watch.finish(written, err)
	if err == nil && d.NotifyWhenIdle {
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
	key := "idle:" + id
	claimed, err := m.store.Claim(key, time.Now().Add(retention))
	if err != nil || !claimed {
		return err
	}
	d := Delivery{Inbox: inbox, ID: id, Message: envelope.Message{From: from, FromMode: fromMode}}
	watch := m.watch(d, true)
	n, written, err := m.send(ctx, inbox, line, watch.register)
	watch.finish(written, err)
	if err != nil && n == 0 {
		return errors.Join(err, m.store.Forget(key))
	}
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

func (m *Manager) rewrite(ctx context.Context, d Delivery, peer peercred.Credentials) (time.Time, error) {
	if err := wait(ctx, refill); err != nil {
		return time.Time{}, err
	}
	return m.post(ctx, d, true, func(next peercred.Credentials) error {
		if next != peer {
			return errors.New("inbox process changed before retry")
		}
		return nil
	})
}

func (e Event) retryable(rewrites int) bool {
	return rewrites < maxRewrites && e.Status == "dropped" && (e.DropReason == "rate-limited" || e.DropReason == "queue-full")
}
