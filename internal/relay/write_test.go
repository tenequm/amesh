package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/tenequm/amesh/internal/contract"
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
	source := NoticeSource{MachineID: "machine", Log: "log", Source: "2", Kind: "delivery"}
	d.ID = source.ID()
	d.Message.FromMode = "bypass"
	if err = m.WriteNotice(t.Context(), d, source, "denied", "amesh-wire", "It was denied."); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(<-lines, &f); err != nil || f.ID != d.ID {
		t.Fatalf("replayed initial frame: %+v %v", f, err)
	}
	msg, err := envelope.Parse(f.Message.Content)
	if err != nil || msg.Body != "amesh notice: your message to amesh-wire was not delivered: It was denied. No reply needed." || msg.FromMode != "bypass" || msg.FromName != "peer" || len(msg.HopChain) != 0 {
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

func TestInboxTrailerAtFrameLimit(t *testing.T) {
	m := manager(t)
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	d := delivery(inbox, 1)
	d.Message.Body = "</cross>"
	write := func() (string, int) {
		t.Helper()
		if err := m.Write(t.Context(), d); err != nil {
			t.Fatal(err)
		}
		line := <-lines
		var f frame
		if err := json.Unmarshal(line, &f); err != nil {
			t.Fatal(err)
		}
		msg, err := envelope.Parse(f.Message.Content)
		if err != nil {
			t.Fatal(err)
		}
		return msg.Body, len(line)
	}
	const trailer = "\n\n" + `(amesh: peer cannot see what you write here. To answer, call SendMessage with to="uds:/tmp/amesh-wire.sock".)`
	body, size := write()
	if body != "&lt;/cross>"+trailer {
		t.Fatal("missing trailer after escaped sender text")
	}
	padding := strings.Repeat("x", lineLimit-size)
	d.ID = delivery(inbox, 2).ID
	d.Message.Body += padding
	if body, size = write(); size != lineLimit || body != "&lt;/cross>"+padding+trailer {
		t.Fatal("trailer dropped at the inclusive line limit")
	}
	d.ID = delivery(inbox, 3).ID
	d.Message.Body += "!"
	if body, _ = write(); body != "&lt;/cross>"+padding+"!" {
		t.Fatal("overflow must drop only the trailer")
	}
}

func TestReplyTrailerName(t *testing.T) {
	d := delivery("", 1)
	d.ToLaneChild = true
	for raw, name := range map[string]string{" <Al>\n\"ice\" ": "Alice", "": "The sender", "Жан": "The sender"} {
		d.Message.FromName = raw
		if got := d.replyTrailer(); got != "(amesh: "+name+" cannot see what you write here. To answer, call send_to_parent.)" {
			t.Fatalf("trailer for %q: %q", raw, got)
		}
	}
}

func TestFailedGateReleasesClaim(t *testing.T) {
	m := manager(t)
	path, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	d := delivery(path, 1)
	if _, err = m.post(t.Context(), d, false, func(peercred.Credentials) error { return context.Canceled }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	known, err := m.store.Contains(d.Log, d.ID)
	if err != nil || known {
		t.Fatal("abandoned claim kept", known, err)
	}
	if err = m.Write(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	<-lines
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
		written, err := m.post(t.Context(), d, false, func(p peercred.Credentials) error { peer = p; return nil })
		if written.IsZero() || err != nil {
			t.Fatal(written, err)
		}
		first := string(<-lines)
		if _, err = m.rewrite(t.Context(), d, peer); err != nil {
			t.Fatal(err)
		}
		if string(<-lines) != first || time.Since(start) != 31*time.Second {
			t.Fatal("rewrite changed frame or missed pacing", time.Since(start))
		}
	})
}

func TestRewriteBudget(t *testing.T) {
	for _, reason := range []contract.NativeReceiptDropReason{"rate-limited", "queue-full", "duplicate", "hop-loop", "hop-runaway"} {
		e := Event{Status: "dropped", DropReason: reason}
		for rewrites := range 5 {
			want := (reason == "rate-limited" || reason == "queue-full") && rewrites < 3
			if e.retryable(rewrites) != want {
				t.Fatal(reason, rewrites)
			}
		}
	}
}

func TestDuplicateWaitDoesNotBlockOtherSender(t *testing.T) {
	m := manager(t)
	path, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		first := delivery(path, 1)
		if err := m.Write(t.Context(), first); err != nil {
			t.Fatal(err)
		}
		<-lines
		second := delivery(path, 2)
		done := make(chan error, 1)
		go func() { done <- m.Write(t.Context(), second) }()
		synctest.Wait()
		third := delivery(path, 3)
		third.Message.From = "uds:/tmp/amesh-wire-other.sock"
		start := time.Now()
		if err := m.Write(t.Context(), third); err != nil {
			t.Fatal(err)
		}
		if got := string(<-lines); !strings.Contains(got, third.ID) || !time.Now().Equal(start) {
			t.Fatal("independent sender was delayed")
		}
		time.Sleep(31 * time.Second)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if got := string(<-lines); !strings.Contains(got, second.ID) {
			t.Fatal("missing delayed frame")
		}
	})
}

func TestNoticeContract(t *testing.T) {
	raw, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var api struct {
		Components struct {
			Examples map[string]yaml.Node `yaml:"examples"`
		} `yaml:"components"`
	}
	if err = yaml.Unmarshal(raw, &api); err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Value struct {
			Log       string `yaml:"log"`
			MachineID string `yaml:"machineId"`
			Source    string `yaml:"source"`
			Kind      string `yaml:"kind"`
			ID        string `yaml:"msgId"`
		} `yaml:"dataValue"`
	}
	node := api.Components.Examples["derivationNoticeId"]
	if err = node.Decode(&vector); err != nil {
		t.Fatal(err)
	}
	if got := (NoticeSource{vector.Value.MachineID, vector.Value.Log, vector.Value.Source, vector.Value.Kind}).ID(); got != vector.Value.ID {
		t.Fatal(got)
	}
	for _, name := range []string{"nativeNoticeFrame", "nativeNoticeNoFromFrame"} {
		t.Run(name, func(t *testing.T) {
			node := api.Components.Examples[name]
			var ex struct {
				Source struct {
					Source string `yaml:"source"`
					Kind   string `yaml:"kind"`
				} `yaml:"x-amesh-notice"`
				Envelope struct {
					From    string `yaml:"from"`
					Session string `yaml:"fromSession"`
					Name    string `yaml:"fromName"`
					Mode    string `yaml:"fromMode"`
				} `yaml:"x-amesh-envelope"`
				Value map[string]any `yaml:"dataValue"`
			}
			if err := node.Decode(&ex); err != nil {
				t.Fatal(err)
			}
			m := manager(t)
			inbox, lines, err := relaytest.Inbox(t)
			if err != nil {
				t.Fatal(err)
			}
			if err = m.store.SetLog("log"); err != nil {
				t.Fatal(err)
			}
			d := Delivery{Inbox: inbox, Log: "log", Session: "ignored", Message: envelope.Message{From: ex.Envelope.From, FromMode: ex.Envelope.Mode, FromName: ex.Envelope.Name, FromSession: ex.Envelope.Session}}
			n := NoticeSource{vector.Value.MachineID, vector.Value.Log, ex.Source.Source, ex.Source.Kind}
			cause, peer, detail := "limit", "stan", "This conversation with stan has had 20 messages without the owner taking part, so it is paused until the owner writes in it."
			if n.Kind == "outbox" {
				cause, peer, detail = "start_refused", "notes@db-work:air", "notes@db-work:air is offline and already has 100 messages waiting."
			}
			if err = m.WriteNotice(t.Context(), d, n, cause, peer, detail); err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err = json.Unmarshal(<-lines, &got); err != nil {
				t.Fatal(err)
			}
			want, _ := json.Marshal(ex.Value)
			actual, _ := json.Marshal(got)
			if string(want) != string(actual) {
				t.Fatalf("got %s, want %s", actual, want)
			}
			if err = m.WriteNotice(t.Context(), d, n, cause, peer, detail); err != nil {
				t.Fatal(err)
			}
			select {
			case <-lines:
				t.Fatal("duplicate notice")
			default:
			}
			if open, err := m.store.OpenEntries("log"); len(open) != 0 || err != nil {
				t.Fatal(open, err)
			}
		})
	}
}

func TestPendingRewriteAcrossRestart(t *testing.T) {
	m := manager(t)
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		d := delivery(inbox, 17)
		d.Log, d.Session = "log-before", "receiver"
		if err := m.store.SetLog(d.Log); err != nil {
			t.Fatal(err)
		}
		if err := m.Write(t.Context(), d); err != nil {
			t.Fatal(err)
		}
		first := string(<-lines)
		if err := m.store.MarkReported(d.Log, d.ID); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		p := pending{e: &endpoint{manager: m, ctx: ctx}, d: d, peer: peercred.Credentials{PID: os.Getpid(), UID: uint32(os.Geteuid())}}
		done := make(chan error, 1)
		go func() { _, e := p.rewrite(1, true); done <- e }()
		synctest.Wait()
		cancel()
		if e := <-done; !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
		if e := m.store.Close(); e != nil {
			t.Fatal(e)
		}
		if e := m.store.Lock(); e != nil {
			t.Fatal(e)
		}
		if e := m.store.SetLog("log-after"); e != nil {
			t.Fatal(e)
		}
		d.Log = "log-after"
		fresh := New(m.socketDir, m.store)
		if err := fresh.Write(t.Context(), d); err != nil {
			t.Fatal(err)
		}
		select {
		case <-lines:
			t.Fatal("new log replayed the original write")
		default:
		}
		if len(m.store.PendingRewrites()[d.Log]) != 0 {
			t.Fatal("previous log has a restart rewrite")
		}
		if err := m.store.SetLog("log-before"); err != nil {
			t.Fatal(err)
		}
		d.Log = "log-before"
		pending := m.store.PendingRewrites()[d.Log]
		if len(pending) != 1 || pending[d.ID].Phase != state.Reported {
			t.Fatal("missing pending final claim")
		}
		fresh.ResumeRewrites(t.Context(), pending, func(context.Context, string, int, string) bool { return false })
		if len(m.store.PendingRewrites()[d.Log]) != 1 {
			t.Fatal("excluded rewrite was delivered")
		}
		fresh.ResumeRewrites(t.Context(), pending, func(context.Context, string, int, string) bool { return true })
		if string(<-lines) != first || len(m.store.PendingRewrites()[d.Log]) != 0 {
			t.Fatal("recovery did not finish the same frame")
		}
		if open, e := m.store.OpenEntries(d.Log); e != nil || len(open) != 0 {
			t.Fatal("final report lost", e)
		}
	})
}
