package lane

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/peercred"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
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
	path    string
	handler relay.Handler
	writes  []relay.Delivery
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

func (*recorder) NotifyWhenIdle(context.Context, string, string, string, string) error { return nil }

func TestLaneRouting(t *testing.T) {
	ctx := t.Context()
	p := registry.Session{PID: 101, ProcStart: "p", SessionID: "4b0f3c2d-1e9a-4c8b-a7d6-5e4f3a2b1c0d", Name: "parent", SocketPath: "/tmp/cc-socks/101.sock"}
	child := registry.Session{PID: 102, ProcStart: "c", SessionID: "2b9c951f-7a3e-4d1c-9b8a-6f5e4d3c2b1a", Name: "child", SocketPath: "/tmp/cc-socks/102.sock"}
	s := &seat{rows: map[int]registry.Session{}}
	s.put(child)
	r := new(recorder)
	b := New(ctx, s, r, "pm_PCly-PiMxyo5IXRSo4i70g")
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
		if e != nil || !got.Accepted || got.ID != d.ID || got.To != "parent" || d.Inbox != p.SocketPath || d.Message.From != "uds:"+r.path || d.Message.FromSession != "ln_0d049992f294f2a2" || d.Message.FromMode != "" || len(d.Message.HopChain) != len(hops) {
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
	if err != nil || got.Reason != gone {
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
