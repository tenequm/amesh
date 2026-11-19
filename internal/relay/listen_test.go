package relay

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
	"github.com/tenequm/amesh/internal/relay/relaytest"
)

func TestPathVectors(t *testing.T) {
	for _, v := range []struct{ ns, id, name string }{{"amesh-relay", "pe_Q7m2Hx4tLs9v", "ab737c79d448059c.sock"}, {"amesh-ln", "ln_0d049992f294f2a2", "a5bce10d4370073a.sock"}} {
		got, err := Path("/tmp/cc-socks", v.ns+"\npm_PCly-PiMxyo5IXRSo4i70g", v.id)
		if err != nil || filepath.Base(got) != v.name {
			t.Fatal(got, err)
		}
	}
}

func TestListenReplyHeldAndIdle(t *testing.T) {
	m := manager(t)
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(inbox)
	m.socketDir = dir
	path, err := Path(dir, "amesh-ln\namesh-wire", "child")
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan Event, 10)
	listener, err := m.Listen(t.Context(), path, func(_ context.Context, e Event) error { events <- e; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal(info, err)
	}
	d := delivery(inbox, 1)
	d.Message.From = envelope.Address(path)
	d.NotifyWhenIdle = true
	if err = m.Write(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	<-lines
	var idle frame
	if err = json.Unmarshal(<-lines, &idle); err != nil || idle.Action != "notify_when_idle" || idle.ID != d.ID || idle.Mode != "prompting" {
		t.Fatal(idle, err)
	}
	send := func(f frame) {
		t.Helper()
		conn, dialErr := (&net.Dialer{}).DialContext(t.Context(), "unix", path)
		if dialErr != nil {
			t.Fatal(dialErr)
		}
		f.Version = 1
		f.From = envelope.Address(inbox)
		if err = json.NewEncoder(conn).Encode(f); err != nil {
			t.Fatal(err)
		}
		if err = conn.(*net.UnixConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		if _, err = io.Copy(io.Discard, conn); err != nil {
			t.Fatal(err)
		}
		if err = conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	assert := func(action, status string) Event {
		t.Helper()
		ev := <-events
		if ev.Action != action || ev.Status != status || ev.Peer.PID != os.Getpid() || int(ev.Peer.UID) != os.Geteuid() {
			t.Fatal(ev)
		}
		return ev
	}
	send(frame{Type: "control", Action: "peer_message_status", ID: delivery(inbox, 2).ID, OriginalID: d.ID, Status: "held", Reason: "Approval pending"})
	if ev := assert("peer_message_status", "held"); ev.OriginalID != d.ID {
		t.Fatal(ev)
	}
	send(frame{Type: "control", Action: "peer_message_status", ID: delivery(inbox, 3).ID, OriginalID: d.ID, Status: "delivered", Reason: "Approved"})
	assert("peer_message_status", "delivered")
	send(frame{Type: "control", Action: "peer_idle_notice", ID: delivery(inbox, 4).ID, OriginalID: d.ID, State: "idle", FinishedAt: 12345, Mode: "prompting"})
	if ev := assert("peer_idle_notice", ""); ev.FinishedAt != 12345 || ev.OriginalID != d.ID {
		t.Fatal(ev)
	}
	msg := d.Message
	msg.From = envelope.Address(inbox)
	msg.Body = "reply"
	content, err := envelope.Render(msg)
	if err != nil {
		t.Fatal(err)
	}
	send(frame{Type: "user", ID: delivery(inbox, 5).ID, Priority: "next", Message: &wireMessage{Role: "user", Content: content}})
	if ev := assert("user", ""); ev.Message.Body != "reply" {
		t.Fatal(ev)
	}
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("socket survived close", err)
	}
}

func TestContractControls(t *testing.T) {
	raw, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// These examples contain only flat scalar fields and an empty dropped-id array.
	for _, example := range contractBlocks(string(raw)) {
		header, values, ok := strings.Cut(example, "      dataValue:\n")
		if !ok || !strings.Contains(header, "x-amesh-schema: NativeReceipt\n") && !strings.Contains(header, "x-amesh-schema: NativeIdleNotice\n") && !strings.Contains(header, "x-amesh-schema: NativeNotifyWhenIdle\n") {
			continue
		}
		t.Run(strings.Fields(header)[0], func(t *testing.T) {
			data := make(map[string]any)
			for line := range strings.SplitSeq(values, "\n") {
				if !strings.HasPrefix(line, "        ") {
					break
				}
				key, value, ok := strings.Cut(strings.TrimSpace(line), ": ")
				if !ok {
					t.Fatal(line)
				}
				var scalar any
				if json.Unmarshal([]byte(value), &scalar) != nil {
					scalar = value
				}
				data[key] = scalar
			}
			bytes, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			if data["action"] == "notify_when_idle" {
				var f frame
				if err = json.Unmarshal(bytes, &f); err != nil {
					t.Fatal(err)
				}
				line, lineErr := f.line()
				if lineErr != nil {
					t.Fatal(lineErr)
				}
				var actual map[string]any
				if err = json.Unmarshal(line, &actual); err != nil || !reflect.DeepEqual(actual, data) {
					t.Fatal(actual, err)
				}
				return
			}
			ev, err := decode(bytes)
			if err != nil || ev.OriginalID != data["orig_msg_id"] || ev.Action != data["action"] || ev.From != data["from"] {
				t.Fatal(ev, err)
			}
			if data["status"] != nil && ev.Status != data["status"] {
				t.Fatal(ev.Status)
			}
		})
	}
}

func contractBlocks(raw string) []string {
	return regexp.MustCompile(`(?m)^    (\w+):\n`).Split(raw, -1)
}

func TestAcceptWindowAndPeerCorrelation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		events := make(chan Event, 4)
		e := &endpoint{ctx: ctx, handle: func(_ context.Context, e Event) error { events <- e; return nil }, pending: make(map[subscription]*pending)}
		peer := peercred.Credentials{PID: 7}
		d := delivery("/tmp/amesh-wire.sock", 1)
		newWatch := func(id string, idle bool) *pending {
			t.Helper()
			d.ID = id
			p := &pending{e: e, key: subscription{id, idle}, d: d}
			if err := p.register(peer); err != nil {
				t.Fatal(err)
			}
			p.finish(time.Now(), nil)
			synctest.Wait()
			return p
		}
		p := newWatch(d.ID, false)
		ev := Event{ID: "receipt", Action: "peer_message_status", Status: "held", OriginalID: d.ID, Peer: peercred.Credentials{PID: 9}}
		if err := e.route(ev); err != nil {
			t.Fatal(err)
		}
		time.Sleep(acceptWindow - time.Nanosecond)
		select {
		case <-events:
			t.Fatal("early accept or foreign receipt")
		default:
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := <-events; got.Status != "accepted" || got.OriginalID != d.ID || got.Peer != peer {
			t.Fatal(got)
		}
		<-p.done
		idle := newWatch(d.ID, true)
		time.Sleep(12 * time.Hour)
		<-idle.done
		select {
		case <-events:
			t.Fatal("manufactured idle notice")
		default:
		}
	})
}

func TestCoalescedDropsRetryToBudget(t *testing.T) {
	m := manager(t)
	path, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		events := make(chan Event, 2)
		e := &endpoint{manager: m, ctx: ctx, handle: func(_ context.Context, e Event) error { events <- e; return nil }, pending: make(map[subscription]*pending)}
		d := delivery(path, 1)
		m.endpoints[d.Message.From] = e
		if err := m.Write(ctx, d); err != nil {
			t.Fatal(err)
		}
		first := string(<-lines)
		synctest.Wait()
		for attempt := range maxRewrites + 1 {
			receipt := Event{Action: "peer_message_status", Status: "dropped", DropReason: "queue-full", Peer: peercred.Credentials{PID: os.Getpid(), UID: uint32(os.Geteuid())}, OriginalID: "uncorrelated", DroppedIDs: []string{d.ID, d.ID}, ID: delivery(path, 10+attempt).ID}
			if err := e.route(receipt); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if attempt < maxRewrites {
				time.Sleep(31 * time.Second)
				if got := string(<-lines); got != first {
					t.Fatal("different retry frame")
				}
				synctest.Wait()
				select {
				case got := <-events:
					t.Fatal("premature report", got)
				default:
				}
			}
		}
		if got := <-events; got.OriginalID != d.ID || got.Status != "dropped" || got.DropReason != "queue-full" {
			t.Fatal(got)
		}
		cancel()
		e.wg.Wait()
	})
}

func TestListenerRejectsBadFrames(t *testing.T) {
	id := delivery("", 1).ID
	for _, f := range []frame{
		{Type: "control", Action: "peer_message_status", OriginalID: id, Status: "invented"},
		{Type: "control", Action: "peer_message_status", OriginalID: id, Status: "dropped", DropReason: "other"},
		{Type: "user", Message: &wireMessage{"user", "raw text"}, Priority: "next"},
		{Type: "user", Message: &wireMessage{"user", "<cross-session-message>\nbody\n</cross-session-message>"}, Priority: "next"},
	} {
		f.ID, f.From, f.Version = id, "uds:/tmp/amesh-wire.sock", 1
		b, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = decode(b); err == nil {
			t.Fatal(f)
		}
	}
}

func TestNativeIdleBeforeFirstTurn(t *testing.T) {
	b := []byte(`{"type":"control","action":"peer_idle_notice","msgV":1,"msg_id":"fa01b25e-0021-4049-beef-a11ce0000002","orig_msg_id":"fa01b25e-0021-4049-beef-a11ce0000001","from":"uds:/tmp/amesh-wire.sock","state":"idle","from_mode":null,"finished_at":null}`)
	got, err := decode(b)
	if err != nil || got.Action != "peer_idle_notice" || got.FromMode != "" || got.FinishedAt != 0 {
		t.Fatal(got, err)
	}
}
