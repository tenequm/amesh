package lane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub/hubtest"

	"github.com/google/uuid"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/relay/relaytest"
	"github.com/tenequm/amesh/internal/state"
)

type seat struct {
	sync.Mutex
	rows map[int]registry.Session
}

func (s *seat) List(context.Context) ([]registry.Session, error) {
	s.Lock()
	defer s.Unlock()
	var all []registry.Session
	for _, r := range s.rows {
		all = append(all, r)
	}
	return all, nil
}
func (s *seat) put(r registry.Session) { s.Lock(); defer s.Unlock(); s.rows[r.PID] = r }
func (s *seat) remove(pid int)         { s.Lock(); defer s.Unlock(); delete(s.rows, pid) }
func (s *seat) Resolve(_ context.Context, pid int) (registry.Session, error) {
	s.Lock()
	defer s.Unlock()
	return s.rows[pid], nil
}

func (s *seat) Live(ctx context.Context, k registry.Key) (bool, error) {
	r, _ := s.Resolve(ctx, k.PID)
	return r.PID != 0 && r.Key == k, nil
}

type recorder struct {
	idleID, idleMode, idleInbox, idleFrom string
	path                                  string
	handler                               relay.Handler
	writes                                []relay.Delivery
}

func (r *recorder) Listen(_ context.Context, p string, h relay.Handler) (io.Closer, error) {
	r.path = p
	r.handler = h
	return registration{}, nil
}

func (r *recorder) Write(_ context.Context, d relay.Delivery) error {
	r.writes = append(r.writes, d)
	return nil
}

func (r *recorder) NotifyWhenIdle(_ context.Context, inbox, from, mode, id string) error {
	r.idleID, r.idleMode, r.idleInbox, r.idleFrom = id, mode, inbox, from
	return nil
}

func TestLaneRouting(t *testing.T) {
	ctx := t.Context()
	p := registry.Session{PID: 101, ProcStart: "p", SessionID: "4b0f3c2d-1e9a-4c8b-a7d6-5e4f3a2b1c0d", Name: "parent", SocketPath: "/tmp/cc-socks/101.sock"}
	child := registry.Session{PID: 102, ProcStart: "c", SessionID: "2b9c951f-7a3e-4d1c-9b8a-6f5e4d3c2b1a", Name: "child", SocketPath: "/tmp/cc-socks/102.sock"}
	s := &seat{rows: map[int]registry.Session{}}
	s.put(child)
	r := new(recorder)
	b := newBackend(ctx, s, r, func() (string, error) { return "pm_PCly-PiMxyo5IXRSo4i70g", nil })
	checkRefusal := func(id, reason string) {
		t.Helper()
		_, err := b.Connect(ctx, child, id)
		if !errors.Is(err, refusal(reason)) {
			t.Fatalf("connect: %v", err)
		}
	}
	checkRefusal(p.SessionID, notFound)
	checkRefusal(child.SessionID, notFound)
	s.put(p)
	copyParent := p
	copyParent.PID = 103
	s.put(copyParent)
	checkRefusal(p.SessionID, ambiguous)
	s.remove(103)
	parentID := p.SessionID
	h, err := b.Connect(ctx, child, parentID)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	if filepath.Base(r.path) != "a5bce10d4370073a.sock" {
		t.Fatal(r.path)
	}
	nativeSend := func(pid int, chain []string) error {
		return r.handler(ctx, relay.Event{Action: "user", ID: uuid.NewString(), Peer: peercred.Credentials{PID: pid}, Message: envelope.Message{Body: "native", HopChain: chain, FromName: "claimed", FromMode: "bypass"}})
	}
	if err = nativeSend(103, nil); err == nil || len(r.writes) != 0 {
		t.Fatal("outside sender accepted")
	}
	chain := []string{"b551102c847d34ee8a425f54"}
	for _, hops := range [][]string{chain, nil} {
		if err = nativeSend(p.PID, hops); err != nil {
			t.Fatal(err)
		}
		d := r.writes[len(r.writes)-1]
		if d.Inbox != child.SocketPath || d.Message.FromMode != "bypass" || d.Message.FromName != p.Name {
			t.Fatal(d)
		}
		got, e := b.SendToParent(ctx, child, parentID, "reply")
		d = r.writes[len(r.writes)-1]
		if e != nil || !got.Accepted || got.ID == nil || *got.ID != d.ID || got.To == nil || *got.To != "parent" || d.Inbox != p.SocketPath || d.Message.From != "uds:"+r.path || d.Message.FromSession != "ln_0d049992f294f2a2" || d.Message.FromMode != "" || len(d.Message.HopChain) != len(hops) {
			t.Fatalf("%+v %+v %v", got, d, e)
		}
	}
	p.SessionID = "after-clear"
	s.put(p)
	if err = nativeSend(child.PID, nil); err != nil {
		t.Fatal(err)
	}
	s.remove(p.PID)
	got, err := b.SendToParent(ctx, child, parentID, "gone")
	if err != nil || got.Reason == nil || *got.Reason != gone {
		t.Fatal(got, err)
	}
	copyParent.Name = "resumed"
	copyParent.SocketPath = "/tmp/cc-socks/103.sock"
	s.put(copyParent)
	if err = nativeSend(copyParent.PID, chain); err != nil {
		t.Fatal(err)
	}
	s.remove(child.PID)
	oldPath := r.path
	child.PID = 104
	s.put(child)
	if _, err = b.Connect(ctx, child, parentID); err != nil || r.path != oldPath {
		t.Fatal(r.path, err)
	}
}

func TestLaneReplyTrailers(t *testing.T) {
	parentPath, parentLines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	childPath, childLines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	parent := registry.Session{PID: 101, SessionID: "p", Name: "parent", SocketPath: parentPath}
	child := registry.Session{PID: 102, SessionID: "c", Name: "child", SocketPath: childPath}
	s := &seat{rows: map[int]registry.Session{101: parent, 102: child}}
	store := state.New(t.TempDir())
	if err = store.Lock(); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(t.Context())
	b := newBackend(ctx, s, relay.New(filepath.Dir(childPath), store), func() (string, error) { return "local", nil })
	defer func() { cancel(); b.Wait() }()
	if _, err = b.Connect(ctx, child, parent.SessionID); err != nil {
		t.Fatal(err)
	}
	c := b.lanes[pair{parent.SessionID, child.SessionID}]
	checkBody := func(lines <-chan []byte, want string) {
		t.Helper()
		var f contract.NativeUserFrame
		if decodeErr := json.Unmarshal(<-lines, &f); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		msg, parseErr := envelope.Parse(f.Message.Content)
		if parseErr != nil || msg.From != c.address || msg.Body != want {
			t.Fatalf("lane inbox body %q, want %q (%v)", msg.Body, want, parseErr)
		}
	}
	if err = c.handle(ctx, relay.Event{Action: "user", Peer: peercred.Credentials{PID: parent.PID}, ID: uuid.NewString(), Message: envelope.Message{Body: "Ready?"}}); err != nil {
		t.Fatal(err)
	}
	checkBody(childLines, "Ready?\n\n(amesh: parent cannot see what you write here. To answer, call send_to_parent.)")
	got, err := b.SendToParent(ctx, child, parent.SessionID, "Ready.")
	if err != nil || !got.Accepted {
		t.Fatal(got, err)
	}
	checkBody(parentLines, "Ready.\n\n"+`(amesh: child cannot see what you write here. To answer, call SendMessage with to="`+c.address+`".)`)
}

func TestLaneNotices(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		s := &seat{rows: map[int]registry.Session{}}
		r := new(recorder)
		b := newBackend(ctx, s, r, func() (string, error) { return "local", nil })
		defer func() { cancel(); b.Wait() }()
		p := registry.Session{PID: 11, ProcStart: "p", SessionID: "p", Name: "parent", FromMode: "bypass", SocketPath: "/tmp/cc-socks/11.sock"}
		child := registry.Session{PID: 12, ProcStart: "c", SessionID: "c", Name: "child", CWD: "/project", SocketPath: "/tmp/cc-socks/12.sock", Status: "idle"}
		s.put(p)
		s.put(child)
		connect := func() {
			t.Helper()
			if _, e := b.Connect(ctx, child, "p"); e != nil {
				t.Fatal(e)
			}
			synctest.Wait()
		}
		advance := func(d time.Duration) { time.Sleep(d); synctest.Wait() }
		connect()
		advance(2 * time.Second)
		s.remove(child.PID)
		advance(time.Second)
		if len(r.writes) != 0 || len(b.lanes) != 0 {
			t.Fatal("transient child announced")
		}
		s.put(child)
		connect()
		advance(3 * time.Second)
		if len(r.writes) != 1 || !strings.HasPrefix(r.writes[0].Message.Body, "Lane started: child") || r.writes[0].Message.FromMode != "bypass" {
			t.Fatal(r.writes)
		}
		child.Status = "busy"
		s.put(child)
		advance(time.Second)
		idle := func(id string) {
			t.Helper()
			if err := r.handler(ctx, relay.Event{Action: "peer_idle_notice", OriginalID: id, Peer: peercred.Credentials{PID: child.PID}}); err != nil {
				t.Fatal(err)
			}
		}
		first := r.idleID
		if first == "" || r.idleMode != "" {
			t.Fatal("missing or misclassified idle subscription")
		}
		if _, e := b.SendToParent(ctx, child, "p", "reply"); e != nil {
			t.Fatal(e)
		}
		child.Status = "idle"
		s.put(child)
		advance(time.Second)
		child.Status = "busy"
		child.FromMode = "bypass"
		s.put(child)
		advance(time.Second)
		if r.idleID == first || r.idleMode != "bypass" {
			t.Fatal("second busy edge")
		}
		idle(first)
		if len(r.writes) != 2 {
			t.Fatal("replied turn sent a separate notice")
		}
		p.FromMode = "prompting"
		s.put(p)
		idle(r.idleID)
		d := r.writes[len(r.writes)-1]
		if d.Message.Body != "child finished its turn and is idle. No reply needed." || d.Message.FromMode != "" {
			t.Fatal(d)
		}
		s.remove(child.PID)
		advance(time.Second)
		d = r.writes[len(r.writes)-1]
		if d.Message.Body != "Lane ended: child exited. No reply needed." || d.Message.From != r.writes[0].Message.From || len(b.lanes) != 0 {
			t.Fatal(d)
		}
		child.PID++
		s.put(child)
		connect()
		advance(250 * time.Millisecond)
		if len(r.writes) != 5 || !strings.HasPrefix(r.writes[4].Message.Body, "Lane started:") {
			t.Fatal("busy child should announce immediately")
		}
	})
}

func TestLocalMachineIdentity(t *testing.T) {
	d := filepath.Join(t.TempDir(), "amesh-fix1-identity")
	identity := func() (string, error) { return New(t.Context(), nil, nil, d).identity() }
	first, err := identity()
	if err != nil || !strings.HasPrefix(first, "pm_") {
		t.Fatal(first, err)
	}
	again, err := identity()
	if err != nil || again != first {
		t.Fatal(again, err)
	}
	if err = os.Chmod(filepath.Join(d, "machine.key"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err = identity(); err == nil {
		t.Fatal("accepted group-readable key")
	}
}

func TestLaneIdleContract(t *testing.T) {
	var sub contract.NativeNotifyWhenIdle
	var idle contract.NativeIdleNotice
	var turn contract.NativeUserFrame
	var vector contract.LaneSocketVector
	for name, target := range map[string]any{"nativeNotifyWhenIdle": &sub, "nativePeerIdleNotice": &idle, "laneTurnEndFrame": &turn, "derivationLaneSocket": &vector} {
		if err := json.Unmarshal(hubtest.Example(t, name), target); err != nil {
			t.Fatal(err)
		}
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		p := registry.Session{PID: 11, ProcStart: "parent", SessionID: vector.ParentArgument, Name: "parent", SocketPath: "/tmp/cc-socks/11.sock"}
		child := registry.Session{PID: 61844, ProcStart: "child", SessionID: vector.ChildSessionID, Name: "c-claude-e6", SocketPath: strings.TrimPrefix(idle.From, "uds:"), FromMode: idle.FromMode, Status: "busy"}
		s := &seat{rows: map[int]registry.Session{p.PID: p, child.PID: child}}
		r := new(recorder)
		b := newBackend(ctx, s, r, func() (string, error) { return vector.MachineID, nil })
		defer func() { cancel(); b.Wait() }()
		if _, err := b.Connect(ctx, child, p.SessionID); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(250 * time.Millisecond)
		synctest.Wait()
		if sub.FromMode != nil || r.idleMode != "" || r.idleFrom != sub.From || r.idleInbox != child.SocketPath || r.idleID == "" || idle.OrigMsgID != sub.MsgID {
			t.Fatal("wrong lane subscription")
		}
		if err := r.handler(ctx, relay.Event{Peer: peercred.Credentials{PID: child.PID}, Action: idle.Action, OriginalID: r.idleID, FromMode: idle.FromMode, FinishedAt: idle.FinishedAt}); err != nil {
			t.Fatal(err)
		}
		d := r.writes[len(r.writes)-1]
		content, err := envelope.Render(d.Message)
		if err != nil || content != turn.Message.Content || !d.Notice || d.Inbox != p.SocketPath {
			t.Fatal("wrong turn-end frame", err)
		}
	})
}
