package relay

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
	"github.com/tenequm/amesh/internal/relay/relaytest"
	"github.com/tenequm/amesh/internal/state"
)

func manager(t *testing.T) *Manager {
	t.Helper()
	s := state.New(filepath.Join(t.TempDir(), "amesh-wire-state"))
	if err := s.Lock(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return New("/tmp/cc-socks", s)
}

func delivery(inbox string, n int) Delivery {
	return Delivery{Inbox: inbox, ID: fmt.Sprintf("00000000-0000-0000-0000-%012x", n), Message: envelope.Message{From: "uds:/tmp/amesh-wire.sock", FromSession: "amesh-wire", FromName: "peer", FromMode: "prompting", Body: "body"}}
}

func TestInboxWriteReplayAndNotice(t *testing.T) {
	m := manager(t)
	path, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	d := delivery(path, 1)
	if err = m.Write(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	first := <-lines
	var f frame
	if err = json.Unmarshal(first, &f); err != nil || f.ID != d.ID || f.From != d.Message.From || f.Message.Role != "user" || f.Priority != "next" || f.Version != 1 {
		t.Fatal(f, err)
	}
	if err = m.Write(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	d.ID = delivery(path, 2).ID
	if err = m.WriteNotice(t.Context(), d, "denied", "amesh-wire", "It was denied."); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(<-lines, &f); err != nil || f.ID != d.ID {
		t.Fatalf("replayed initial frame: %+v %v", f, err)
	}
	msg, err := envelope.Parse(f.Message.Content)
	if err != nil || msg.Body != "amesh notice: your message to amesh-wire was not delivered: It was denied. No reply needed." || msg.FromMode != "" || msg.FromName != "peer" || len(msg.HopChain) != 0 {
		t.Fatal(msg, err)
	}
}

func TestFailedConnectCanBeRetried(t *testing.T) {
	m := manager(t)
	path, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	d := delivery(path+".missing", 1)
	if err = m.Write(t.Context(), d); err == nil {
		t.Fatal("acknowledged missing inbox")
	}
	d.Inbox = path
	if err = m.Write(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	<-lines
	d.ID = delivery(path, 2).ID
	d.Message.Body = strings.Repeat("x", lineLimit)
	if err = m.Write(t.Context(), d); err == nil || !strings.Contains(err.Error(), "oversized") {
		t.Fatal(err)
	}
}

func TestPacing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var b bucket
		start := time.Now()
		for i := range burst {
			body := fmt.Sprint(i)
			if d := b.delay(time.Now(), body); d != 0 {
				t.Fatal(i, d)
			}
			b.sent(time.Now(), body)
		}
		if err := wait(t.Context(), b.delay(time.Now(), "new")); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 2*time.Second {
			t.Fatal(time.Since(start))
		}
		b.sent(time.Now(), "new")
		if err := wait(t.Context(), b.delay(time.Now(), "new")); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 33*time.Second {
			t.Fatal(time.Since(start))
		}
		if d := b.delay(time.Now(), "new"); d != 0 {
			t.Fatal(d)
		}
	})
}

func TestConfirmedDropRewriteKeepsIDAndPaces(t *testing.T) {
	m := manager(t)
	path, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		d := delivery(path, 3)
		var peer peercred.Credentials
		start := time.Now()
		ok, err := m.post(t.Context(), d, false, func(p peercred.Credentials) error { peer = p; return nil })
		if !ok || err != nil {
			t.Fatal(ok, err)
		}
		first := string(<-lines)
		if err = m.rewrite(t.Context(), d, peer); err != nil {
			t.Fatal(err)
		}
		if string(<-lines) != first || time.Since(start) != 31*time.Second {
			t.Fatal("rewrite changed frame or missed pacing", time.Since(start))
		}
	})
}

func TestRewriteBudget(t *testing.T) {
	for _, reason := range []string{"rate-limited", "queue-full", "duplicate", "hop-loop", "hop-runaway"} {
		e := Event{Status: "dropped", DropReason: reason}
		for rewrites := range 5 {
			want := (reason == "rate-limited" || reason == "queue-full") && rewrites < 3
			if e.retryable(rewrites) != want {
				t.Fatal(reason, rewrites)
			}
		}
	}
}
