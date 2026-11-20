package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/hub/hubtest"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/relay/relaytest"
	"github.com/tenequm/amesh/internal/state"
)

type testRegistry []registry.Session

func (*testRegistry) SelfCheck(context.Context) error { return nil }

func (r *testRegistry) Snapshot(context.Context) (registry.Listing, error) {
	return registry.Listing{Sessions: *r}, nil
}

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
	if routes["POST /peer/hello"] == nil {
		routes["POST /peer/hello"] = hubtest.Reply(t, "helloResponse")
	}
	server := hubtest.New(t, routes)
	cfg := config.Config{StateDir: meshDir(t), MachineName: "mac", HubURL: server.URL + "/peer"}
	if err := hub.Enroll(t.Context(), cfg, io.Discard); err != nil {
		t.Fatal(err)
	}
	st := state.New(cfg.StateDir)
	if err := st.Lock(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	b := New(cfg, nil, relay.New(cfg.StateDir, st), hub.New(cfg))
	r := new(testRegistry)
	b.ctx = t.Context()
	b.socketDir = cfg.StateDir
	t.Cleanup(b.closeEndpoints)
	b.sessions = r
	b.store = st
	_, err := b.client.Hello(t.Context(), contract.HelloRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return b, r
}

func testSession() registry.Session {
	return registry.Session{PID: os.Getpid(), ProcStart: "amesh-mesh", SessionID: "one", Name: "review", Project: "webapp", Status: "idle"}
}

func TestMembership(t *testing.T) {
	s := testSession()
	key := s.Key.MeshKey()
	var mu sync.Mutex
	var operations []string
	record := func(v string) { mu.Lock(); defer mu.Unlock(); operations = append(operations, v) }
	registered := func(w http.ResponseWriter, r *http.Request) {
		var req contract.SessionRegisterRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		record("register " + req.Name)
		_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: key, Address: "review@webapp:mac"}})
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
	b.localHops[[2]string{"0-other", key}] = 3
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	saved, err := b.store.Members()
	if err != nil || len(saved) != 0 || len(b.localHops) != 0 {
		t.Fatalf("saved members: %v %v, hops %v", saved, err, b.localHops)
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
	key := s.Key.MeshKey()
	recv := make(chan contract.SendRequest, 1)
	b, r := fixture(t, map[string]http.HandlerFunc{
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
	b.members[key] = member{session: s, hosted: true, address: "review@webapp:mac"}
	local := s
	local.PID++
	local.Name, local.Project = "helper", "demo"
	*r = testRegistry{s, local}
	b.members[local.Key.MeshKey()] = member{session: local, hosted: true, address: "helper@demo:mac"}
	dir, err := b.ListPeers(t.Context(), s, "")
	if err != nil {
		t.Fatal(err)
	}
	var want mcpserver.Directory
	_ = json.Unmarshal(hubtest.Example(t, "mcpListPeers"), &want)
	if !reflect.DeepEqual(dir, want) {
		t.Fatalf("directory %#v", dir)
	}
	b.synchronized = true
	for _, tc := range []struct{ to, text, example string }{{"atlsa", "hi", "mcpSendRefusedNotFound"}, {"atlas", strings.Repeat("a", 32769), "mcpSendRefusedTooLarge"}} {
		got, e := b.SendMessage(t.Context(), s, mcpserver.SendArgs{To: tc.to, Message: tc.text})
		var expected mcpserver.SendResult
		_ = json.Unmarshal(hubtest.Example(t, tc.example), &expected)
		if e != nil || !reflect.DeepEqual(got, expected) {
			t.Fatalf("%s: %#v %v", tc.example, got, e)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.client.Drain(ctx, b.reregister, b.completed) }()
	defer func() { cancel(); <-done }()
	got, err := b.SendMessage(t.Context(), s, mcpserver.SendArgs{To: "atlas", Message: "hello"})
	var expected mcpserver.SendResult
	_ = json.Unmarshal(hubtest.Example(t, "mcpSendAccepted"), &expected)
	expected.ID = got.ID
	if err != nil || value(got.ID) == "" || !reflect.DeepEqual(got, expected) {
		t.Fatalf("send %#v %v", got, err)
	}
	req := <-recv
	if req.Text != "hello" || req.ID != value(got.ID) || req.From.Session != key || req.FromMode != nil || req.Origin != "unknown" || req.NotifyWhenIdle {
		t.Fatal("incorrect remote send metadata")
	}
}

func TestDirectoryMergesLiveLocalMembers(t *testing.T) {
	b, r := fixture(t, map[string]http.HandlerFunc{})
	caller := testSession()
	local := caller
	local.PID++
	local.Project = "Demo"
	local.Status = "busy"
	const localAddress = "worker-2@demo:mac"
	stale := local
	stale.PID++
	unregistered := local
	unregistered.PID += 2
	*r = testRegistry{caller, local, unregistered}
	b.members[caller.Key.MeshKey()] = member{session: caller, hosted: true, address: "worker@demo:mac"}
	b.members[local.Key.MeshKey()] = member{session: local, hosted: true, address: localAddress}
	b.members[stale.Key.MeshKey()] = member{session: stale, hosted: true, address: "gone@demo:mac"}
	b.synchronized = true
	b.peers = []contract.Peer{
		{Address: "worker@demo:mac", Kind: "session", Machine: new("mac"), Project: new("demo")},
		{Address: "gone@demo:mac", Kind: "session", Machine: new("mac"), Project: new("demo")},
		{Address: "worker@demo:east", Kind: "session", Machine: new("east"), Project: new("demo"), Status: "idle"},
		{Address: "helper", Kind: "agent", Status: "idle"},
	}
	check := func(filter string, names ...string) {
		t.Helper()
		dir, err := b.ListPeers(t.Context(), caller, filter)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, peer := range dir.Peers {
			got = append(got, peer.Name)
			if peer.Name == localAddress && (peer.Kind != "session" || peer.Status != "busy" || value(peer.Project) != "demo" || value(peer.Machine) != "mac") {
				t.Fatal(peer)
			}
		}
		slices.Sort(got)
		if !slices.Equal(got, names) {
			t.Fatalf("filter %q: got %v, want %v", filter, got, names)
		}
	}
	check("", "helper", localAddress, "worker@demo:east")
	b.peers = append(b.peers, contract.Peer{Address: localAddress, Kind: "session", Project: new("demo"), Machine: new("mac"), Status: "offline"})
	check("@demo", localAddress, "worker@demo:east")
	check("@demo:mac", localAddress)
	check("@demo:east", "worker@demo:east")
}

func TestLocalWinsUsesAssignedAddressAndNativeSender(t *testing.T) {
	b, r := fixture(t, map[string]http.HandlerFunc{
		"POST /peer/sessions/{key}/human": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(hubtest.Example(t, "humanTurnResponse"))
		},
	})
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	sender := testSession()
	replyInbox, replies, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	sender.SocketPath = replyInbox
	sender.FromMode = "prompting"
	target := sender
	target.PID++
	target.Name = "review"
	target.SocketPath = inbox
	*r = append(*r, sender, target)
	b.members[sender.Key.MeshKey()] = member{session: sender, hosted: true, address: "review@webapp:mac"}
	b.members[target.Key.MeshKey()] = member{session: target, hosted: true, address: "review-2@webapp:mac"}
	body := strings.Repeat("x", 32*1024)
	got, err := b.SendMessage(t.Context(), sender, mcpserver.SendArgs{To: "review-2@webapp", Message: body})
	if err != nil || !got.Accepted || value(got.To) != "review-2@webapp:mac" {
		t.Fatalf("%#v %v", got, err)
	}
	var frame contract.NativeUserFrame
	if err = json.Unmarshal(<-lines, &frame); err != nil {
		t.Fatal(err)
	}
	msg, err := envelope.Parse(frame.Message.Content)
	wantBody := body + "\n\n" + `(amesh: review cannot see what you write here. To answer, call send_message with to="review@webapp:mac".)`
	if err != nil || msg.Body != wantBody || msg.From != envelope.Address(sender.SocketPath) || msg.FromName != sender.Name || msg.FromMode != sender.FromMode || value(frame.From) != msg.From || frame.MsgID != value(got.ID) {
		t.Fatal("wrong local native frame")
	}
	got, err = b.SendMessage(t.Context(), target, mcpserver.SendArgs{To: b.members[sender.Key.MeshKey()].address, Message: "answer"})
	if err != nil || !got.Accepted {
		t.Fatalf("reply: %#v %v", got, err)
	}
	if err = json.Unmarshal(<-replies, &frame); err != nil {
		t.Fatal(err)
	}
	answer, err := envelope.Parse(frame.Message.Content)
	if err != nil || answer.From != envelope.Address(target.SocketPath) || answer.Body != "answer\n\n"+`(amesh: review cannot see what you write here. To answer, call send_message with to="review-2@webapp:mac".)` {
		t.Fatalf("wrong local reply: %#v %v", answer, err)
	}
	if n, e := b.client.Pending(""); e != nil || n != 0 {
		t.Fatal("local send entered outbox")
	}
	pair := [2]string{sender.Key.MeshKey(), target.Key.MeshKey()}
	slices.Sort(pair[:])
	if b.localHops[pair] != 2 {
		t.Fatal("local sends were not counted", b.localHops)
	}
	b.localHops[pair] = defaultHopLimit - 1
	results := make(chan bool, 2)
	for range 2 {
		go func() {
			res, sendErr := b.SendMessage(t.Context(), target, mcpserver.SendArgs{To: "review@webapp:mac", Message: "racing"})
			results <- sendErr == nil && res.Accepted
		}()
	}
	if first, second := <-results, <-results; first == second {
		t.Fatal("concurrent sends at the last hop were not limited to one")
	}
	<-replies
	if got, err = b.SendMessage(t.Context(), target, mcpserver.SendArgs{To: "review@webapp:mac", Message: "over the limit"}); err != nil || got.Accepted || !strings.Contains(value(got.Reason), "A human turn resets the limit.") {
		t.Fatalf("local hop limit: %#v %v", got, err)
	}
	if err = b.HumanTurn(t.Context(), sender); err != nil {
		t.Fatal(err)
	}
	if got, err = b.SendMessage(t.Context(), target, mcpserver.SendArgs{To: "review@webapp:mac", Message: "after a human turn"}); err != nil || !got.Accepted {
		t.Fatalf("human turn did not reset the local hop count: %#v %v", got, err)
	}
	<-replies
	*r = (*r)[:1]
	got, err = b.SendMessage(t.Context(), sender, mcpserver.SendArgs{To: "review-2@webapp:mac", Message: "after exit"})
	if err != nil || got.Accepted {
		t.Fatal("delivered to a former member")
	}
}

func TestHumanTurnReregistersOnce(t *testing.T) {
	s := testSession()
	key := s.Key.MeshKey()
	var calls []string
	var at int64
	b, r := fixture(t, map[string]http.HandlerFunc{
		"POST /peer/sessions/{key}/human": func(w http.ResponseWriter, r *http.Request) {
			var req contract.HumanTurnRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.Source != "prompt" || req.At <= 0 || req.Epoch != 7 || r.PathValue("key") != key {
				t.Error("invalid human turn")
			}
			calls = append(calls, "human")
			if len(calls) == 1 {
				at = req.At
				hubtest.Reply(t, "errUnknownSession")(w, r)
				return
			}
			if at != req.At {
				t.Error("human turn timestamp changed")
			}
			hubtest.Reply(t, "humanTurnResponse")(w, r)
		},
		"PUT /peer/sessions/{key}": func(w http.ResponseWriter, _ *http.Request) {
			calls = append(calls, "register")
			_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: key, Address: "review@webapp:mac"}})
		},
	})
	*r = append(*r, s)
	if err := b.HumanTurn(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Connect(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if err := b.HumanTurn(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	(*r)[0].SessionID = "cleared"
	if err := b.HumanTurn(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(calls, []string{"human", "register", "human"}) {
		t.Fatal(calls)
	}
}

func TestDaemonLifecycle(t *testing.T) {
	dir := meshDir(t)
	store := state.New(dir)
	heartbeat := make(chan struct{}, 1)
	stream := make(chan struct{})
	server := hubtest.New(t, map[string]http.HandlerFunc{
		"POST /peer/hello": func(w http.ResponseWriter, r *http.Request) {
			var req contract.HelloRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if !slices.Equal(req.Capabilities, []string{"receipts", "human-turn"}) || !req.Takeover || req.Amesh.Version == "" || req.ClaudeCode.NativeCheck != "ok" || req.ClaudeCode.PeerProtocol != 1 {
				t.Error("hello before native checks")
			}
			hubtest.Reply(t, "helloResponse")(w, r)
		},
		"GET /peer/events": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "event: synchronized\ndata: {\"kind\":\"synchronized\",\"seq\":0}\n\n")
			w.(http.Flusher).Flush()
			close(stream)
			<-r.Context().Done()
		},
		"POST /peer/heartbeat": func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "{}")
			select {
			case heartbeat <- struct{}{}:
			default:
			}
		},
	})
	cfg := config.Config{MachineName: "mac", StateDir: dir, HubURL: server.URL + "/peer", ClaudeConfigDirs: []string{dir}}
	if err := hub.Enroll(t.Context(), cfg, io.Discard); err != nil {
		t.Fatal(err)
	}
	b := New(cfg, registry.New(cfg.ClaudeConfigDirs), relay.New(dir, store), hub.New(cfg))
	stop, err := b.Start(t.Context(), store, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	lock := state.New(dir)
	if err = lock.Lock(); err == nil {
		t.Fatal("daemon did not take state lock")
	}
	select {
	case <-heartbeat:
	case <-time.After(5 * time.Second):
		t.Fatal("no heartbeat")
	}
	select {
	case <-stream:
	case <-time.After(5 * time.Second):
		t.Fatal("no event stream")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var snapshot contract.DaemonStatus
		if err = b.Status(&snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.Hub.State == "connected" && snapshot.Hub.Epoch != nil && *snapshot.Hub.Epoch == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream status: %+v", snapshot.Hub)
		}
		time.Sleep(time.Millisecond)
	}
	if err = stop(); err != nil {
		t.Fatal(err)
	}
	if err = lock.Lock(); err != nil {
		t.Fatal(err)
	}
	_ = lock.Close()
}

func TestUnknownSessionSendKeepsID(t *testing.T) {
	s := testSession()
	key := s.Key.MeshKey()
	var ids []string
	b, sessions := fixture(t, map[string]http.HandlerFunc{
		"GET /peer/peers": hubtest.Reply(t, "peersResponse"),
		"POST /peer/messages": func(w http.ResponseWriter, r *http.Request) {
			var req contract.SendRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			ids = append(ids, req.ID)
			if len(ids) == 1 {
				hubtest.Reply(t, "errUnknownSession")(w, r)
				return
			}
			var res contract.SendResponse
			_ = json.Unmarshal(hubtest.Example(t, "sendResponse"), &res)
			res.Message.ID = req.ID
			_ = json.NewEncoder(w).Encode(res)
		},
		"PUT /peer/sessions/{key}": func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: key, Address: "review@webapp:mac"}})
		},
	})
	*sessions = append(*sessions, s)
	if _, err := b.Connect(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.client.Drain(ctx, b.reregister, b.completed) }()
	defer func() { cancel(); <-done }()
	got, err := b.SendMessage(ctx, s, mcpserver.SendArgs{To: "atlas", Message: "retry this id"})
	if err != nil || !got.Accepted || !slices.Equal(ids, []string{value(got.ID), value(got.ID)}) {
		t.Fatalf("retry: %#v %v", got, err)
	}
}

func TestQueuedSendWaitAndDirectoryReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := hubtest.MemoryScript(t, hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")})
		cfg := config.Config{MachineName: "mac", StateDir: meshDir(t), HubURL: "http://amesh-mesh.test/peer"}
		if err := hub.Enroll(t.Context(), cfg, io.Discard); err != nil {
			t.Fatal(err)
		}
		c := hub.NewWithTransport(cfg, server.Client().Transport)
		b := New(cfg, nil, nil, c)
		if _, err := c.Hello(t.Context(), contract.HelloRequest{}); err != nil {
			t.Fatal(err)
		}
		b.store = state.New(cfg.StateDir)
		if err := b.store.Lock(); err != nil {
			t.Fatal(err)
		}
		defer b.store.Close()
		s := testSession()
		b.members[s.Key.MeshKey()] = member{session: s, hosted: true, address: "review@webapp:mac"}
		b.peers = []contract.Peer{{Address: "gone", Kind: "agent"}}
		r := streamRun{b: b}
		item := contract.EventStreamItem{Event: "status", Data: `{"kind":"status","listed":true,"peer":{"address":"atlas","kind":"agent","status":"idle"}}`}
		if err := r.handle(t.Context(), item); err != nil {
			t.Fatal(err)
		}
		early, err := b.SendMessage(t.Context(), s, mcpserver.SendArgs{To: "absent", Message: "before sync"})
		if err != nil || !early.Accepted {
			t.Fatal(early, err)
		}
		if err = r.handle(t.Context(), contract.EventStreamItem{Event: "synchronized", Data: `{"kind":"synchronized","seq":0}`}); err != nil {
			t.Fatal(err)
		}
		if len(b.peers) != 1 || b.peers[0].Address != "atlas" {
			t.Fatal("stale directory entry survived synchronization")
		}
		for _, addr := range []string{"absent", "other@webapp"} {
			b.peers = append(b.peers, contract.Peer{Address: "other@webapp:server", Kind: "session"})
			miss, e := b.SendMessage(t.Context(), s, mcpserver.SendArgs{To: addr, Message: "after sync"})
			if e != nil || miss.Accepted {
				t.Fatal("post-sync cache miss accepted", addr, e)
			}
		}
		start := time.Now()
		result, err := b.SendMessage(t.Context(), s, mcpserver.SendArgs{To: "__ATLAS..", Message: "durably queued"})
		var want mcpserver.SendResult
		_ = json.Unmarshal(hubtest.Example(t, "mcpSendAcceptedHubDown"), &want)
		want.ID = result.ID
		if err != nil || value(result.ID) == "" || !reflect.DeepEqual(result, want) || time.Since(start) != 5*time.Second {
			t.Fatalf("queued result %#v: %v", result, err)
		}
		if n, e := c.Pending(s.Key.MeshKey()); e != nil || n != 2 {
			t.Fatal("queued send was not durable")
		}
	})
}

func TestRestoreEndsAfterAnswer(t *testing.T) {
	for _, source := range []string{"cwd", "removed-seat"} {
		t.Run(source, func(t *testing.T) {
			var calls int
			var store *state.Store
			s := testSession()
			s.ConfigDir = "/amesh-a2-primary"
			key := s.Key.MeshKey()
			b, r := fixture(t, map[string]http.HandlerFunc{
				"POST /peer/sessions/{key}/end": func(w http.ResponseWriter, r *http.Request) {
					var req contract.SessionEndRequest
					want := contract.SessionEndRequestReasonExcluded
					if source == "removed-seat" {
						want = contract.SessionEndRequestReasonExited
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Reason != want {
						t.Error(req, err)
					}
					saved, err := store.Members()
					if err != nil || len(saved) != 1 {
						t.Error("member disappeared before end answer", err)
					}
					calls++
					if calls == 1 {
						hubtest.Reply(t, "errUnavailable")(w, r)
						return
					}
					hubtest.Reply(t, "errUnknownSession")(w, r)
				}, "POST /peer/heartbeat": hubtest.Reply(t, "heartbeatResponse"),
			})
			store = b.store
			b.cfg.ClaudeConfigDirs = []string{s.ConfigDir}
			if _, err := b.Connect(t.Context(), s); err != nil {
				t.Fatal(err)
			}
			b.members = map[string]member{}
			if source == "cwd" {
				s.CWD = "/amesh-a2-exclude/sub"
				b.cfg.ExcludeRoots = []string{"/amesh-a2-exclude"}
				*r = append(*r, s)
			} else {
				b.cfg.ClaudeConfigDirs = []string{"/amesh-a2-new"}
			}
			if err := b.restore(t.Context()); err != nil || b.IsMember(s.Key) {
				t.Fatal("unexpected restored member", err)
			}
			if err := b.reconcile(t.Context()); err == nil {
				t.Fatal("lost failed end")
			}
			if err := b.reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			saved, err := store.Members()
			if err != nil || len(saved) != 0 || calls != 2 {
				t.Fatal(key, saved, err, calls)
			}
		})
	}
}

func TestPresenceChangesAndStatus(t *testing.T) {
	var registrations []contract.SessionRegisterRequest
	var heartbeats []contract.HeartbeatRequest
	b, r := fixture(t, map[string]http.HandlerFunc{
		"PUT /peer/sessions/{key}": func(w http.ResponseWriter, r *http.Request) {
			var req contract.SessionRegisterRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			registrations = append(registrations, req)
			_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: r.PathValue("key"), Address: "review-2@webapp:mac"}})
		},
		"POST /peer/heartbeat": func(w http.ResponseWriter, r *http.Request) {
			var req contract.HeartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			heartbeats = append(heartbeats, req)
			_, _ = io.WriteString(w, "{}")
		},
	})
	s := testSession()
	*r = append(*r, s)
	if _, err := b.Connect(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if err := b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	b.members = map[string]member{}
	b.presenceLog = ""
	if err := b.restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	(*r)[0].Status = "busy"
	(*r)[0].Version = "changed"
	(*r)[0].FromMode = "prompting"
	b.lastHeartbeat = time.Now().Add(-2 * time.Second)
	if err := b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 1 || registrations[0].Status != "idle" || len(heartbeats) != 2 || heartbeats[1].Sessions[0].Status != "busy" {
		t.Fatal(registrations, heartbeats)
	}
	(*r)[0].SessionID = "changed"
	if err := b.reconcile(t.Context()); err != nil || len(registrations) != 2 {
		t.Fatal(err, registrations)
	}
	conn, _ := b.client.Connection()
	if err := b.store.SetLog(conn.Log); err != nil {
		t.Fatal(err)
	}
	if _, err := b.store.BeginWrite(conn.Log, "open-id", s.Key.MeshKey(), time.Now().Add(time.Hour), "message"); err != nil {
		t.Fatal(err)
	}
	if err := b.store.MarkWritten(conn.Log, "open-id"); err != nil {
		t.Fatal(err)
	}
	var status contract.DaemonStatus
	if err := b.Status(&status); err != nil || len(status.Sessions) != 1 || status.Sessions[0].Address != "review-2@webapp:mac" || status.Sessions[0].Status != "busy" || status.Open.Count != 1 || status.Machine.ID != conn.Machine.ID {
		t.Fatal(status, err)
	}
}
