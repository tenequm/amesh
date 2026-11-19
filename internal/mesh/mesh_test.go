package mesh

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/relay/relaytest"

	"github.com/tenequm/amesh/internal/mcpserver"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/hub/hubtest"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/state"
)

type testRegistry []registry.Session

func (r *testRegistry) List(context.Context) ([]registry.Session, error) { return *r, nil }
func (r *testRegistry) Resolve(_ context.Context, pid int) (registry.Session, error) {
	for _, s := range *r {
		if s.PID == pid {
			return s, nil
		}
	}
	return registry.Session{}, os.ErrNotExist
}

func meshDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "amesh-mesh-") //nolint:usetesting // Unix sockets need short paths shared with subprocess tests.
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func fixture(t *testing.T, routes map[string]http.HandlerFunc) (*Backend, *testRegistry) {
	t.Helper()
	routes["POST /peer/hello"] = hubtest.Reply(t, "helloResponse")
	server, err := hubtest.New(t, routes)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{StateDir: meshDir(t), MachineName: "mac", HubURL: server.URL + "/peer"}
	if err = hub.Enroll(t.Context(), cfg, io.Discard); err != nil {
		t.Fatal(err)
	}
	st := state.New(cfg.StateDir)
	if err = st.Lock(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	b := New(cfg, nil, relay.New(cfg.StateDir, st), hub.New(cfg))
	r := new(testRegistry)
	b.sessions = r
	b.store = st
	_, err = b.client.Hello(t.Context(), contract.HelloRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return b, r
}

func testSession() registry.Session {
	return registry.Session{PID: os.Getpid(), ProcStart: "amesh-mesh", SessionID: "one", Name: "review", Project: "pond", Status: "idle"}
}

func TestMembership(t *testing.T) {
	s := testSession()
	key := sessionKey(s.Key)
	var mu sync.Mutex
	var operations []string
	record := func(v string) { mu.Lock(); defer mu.Unlock(); operations = append(operations, v) }
	registered := func(w http.ResponseWriter, r *http.Request) {
		var req contract.SessionRegisterRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		record("register " + req.Name)
		_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: key, Address: "review@pond:mac"}})
	}
	b, r := fixture(t, map[string]http.HandlerFunc{
		"PUT /peer/sessions/{key}": registered,
		"POST /peer/heartbeat": func(w http.ResponseWriter, r *http.Request) {
			var req contract.HeartbeatRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			record("heartbeat")
			if len(req.Sessions) > 0 {
				if req.Sessions[0].Key != key {
					t.Error("wrong heartbeat key")
				}
				_ = json.NewEncoder(w).Encode(contract.HeartbeatResponse{Ended: []string{key}})
			} else {
				_, _ = io.WriteString(w, "{}")
			}
		},
		"POST /peer/sessions/{key}/end": func(w http.ResponseWriter, r *http.Request) {
			record("end")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"unknown_session","message":"already ended"}`)
		},
	})
	*r = append(*r, s)
	p, err := b.Connect(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	b.members = map[string]member{}
	if err = b.restore(t.Context()); err != nil || !b.IsMember(s.Key) {
		t.Fatalf("restore: %v", err)
	}
	(*r)[0].Name = "renamed"
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	*r = nil
	b.lastHeartbeat = time.Time{}
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	saved, err := b.store.Members()
	if err != nil || len(saved) != 0 {
		t.Fatalf("saved members: %v %v", saved, err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"register review", "heartbeat", "register review", "register renamed", "end", "heartbeat"}
	if !slices.Equal(operations, want) {
		t.Fatalf("operations %v", operations)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	b.runPresence(ctx)
}

func TestDirectoryAndSend(t *testing.T) {
	s := testSession()
	key := sessionKey(s.Key)
	recv := make(chan contract.SendRequest, 1)
	b, _ := fixture(t, map[string]http.HandlerFunc{
		"GET /peer/peers": hubtest.Reply(t, "peersResponse"),
		"POST /peer/messages": func(w http.ResponseWriter, r *http.Request) {
			var req contract.SendRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			recv <- req
			var res contract.SendResponse
			_ = json.Unmarshal(hubtest.Example(t, "sendResponse"), &res)
			res.Message.ID = req.ID
			_ = json.NewEncoder(w).Encode(res)
		},
	})
	b.members[key] = member{session: s, hosted: true, address: "review@pond:mac"}
	dir, err := b.ListPeers(t.Context(), s, "")
	if err != nil {
		t.Fatal(err)
	}
	var want mcpserver.Directory
	_ = json.Unmarshal(hubtest.Example(t, "mcpListPeers"), &want)
	if !reflect.DeepEqual(dir, want) {
		t.Fatalf("directory %#v", dir)
	}
	for _, tc := range []struct{ to, text, example string }{{"stna", "hi", "mcpSendRefusedNotFound"}, {"stan", strings.Repeat("a", 32769), "mcpSendRefusedTooLarge"}} {
		got, e := b.SendMessage(t.Context(), s, mcpserver.SendArgs{To: tc.to, Message: tc.text})
		var expected mcpserver.SendResult
		_ = json.Unmarshal(hubtest.Example(t, tc.example), &expected)
		if e != nil || !reflect.DeepEqual(got, expected) {
			t.Fatalf("%s: %#v %v", tc.example, got, e)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.client.RunOutbox(ctx, nil) }()
	defer func() { cancel(); <-done }()
	got, err := b.SendMessage(t.Context(), s, mcpserver.SendArgs{To: "stan", Message: "hello"})
	var expected mcpserver.SendResult
	_ = json.Unmarshal(hubtest.Example(t, "mcpSendAccepted"), &expected)
	expected.ID = got.ID
	if err != nil || got.ID == "" || !reflect.DeepEqual(got, expected) {
		t.Fatalf("send %#v %v", got, err)
	}
	req := <-recv
	if req.ID != got.ID || req.From.Session != key || req.FromMode != nil || req.Origin != "unknown" || req.NotifyWhenIdle {
		t.Fatal("incorrect remote send metadata")
	}
}

func TestLocalWinsUsesAssignedAddressAndNativeSender(t *testing.T) {
	b, r := fixture(t, map[string]http.HandlerFunc{})
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	sender := testSession()
	sender.SocketPath = filepath.Join(b.cfg.StateDir, "123.sock")
	sender.FromMode = "prompting"
	target := sender
	target.PID++
	target.Name = "review"
	target.SocketPath = inbox
	*r = append(*r, sender, target)
	b.members[sessionKey(sender.Key)] = member{session: sender, hosted: true, address: "review@pond:mac"}
	b.members[sessionKey(target.Key)] = member{session: target, hosted: true, address: "review-2@pond:mac"}
	got, err := b.SendMessage(t.Context(), sender, mcpserver.SendArgs{To: "review-2@pond", Message: "hello locally"})
	if err != nil || !got.Accepted || got.To != "review-2@pond:mac" {
		t.Fatalf("%#v %v", got, err)
	}
	var frame contract.NativeUserFrame
	if err = json.Unmarshal(<-lines, &frame); err != nil {
		t.Fatal(err)
	}
	msg, err := envelope.Parse(frame.Message.Content)
	if err != nil || msg.Body != "hello locally" || msg.From != envelope.Address(sender.SocketPath) || msg.FromName != sender.Name || msg.FromMode != sender.FromMode || value(frame.From) != msg.From || frame.MsgID != got.ID {
		t.Fatal("wrong local native frame")
	}
	if n, e := b.client.Pending(""); e != nil || n != 0 {
		t.Fatal("local send entered outbox")
	}
	*r = (*r)[:1]
	got, err = b.SendMessage(t.Context(), sender, mcpserver.SendArgs{To: "review-2@pond:mac", Message: "after exit"})
	if err != nil || got.Accepted {
		t.Fatal("delivered to a former member")
	}
}
