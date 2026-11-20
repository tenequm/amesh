package mesh

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/hub/hubtest"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay/relaytest"
)

type threadRecorder struct {
	thread, id, text string
	queued           bool
	writes           int
}

func (r *threadRecorder) Deliver(_ context.Context, thread, id, text string, live func() bool) (bool, error) {
	if !live() {
		return false, os.ErrNotExist
	}
	r.thread, r.id, r.text = thread, id, text
	r.writes++
	return r.queued, nil
}

func TestCodexLocalHopsAndReplyAttribution(t *testing.T) {
	b, r := fixture(t, map[string]http.HandlerFunc{})
	cfg := b.cfg
	cfg.HubURL = ""
	b.client = hub.New(cfg)
	record := &threadRecorder{queued: true}
	b.threads = record
	native := testSession()
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	native.SocketPath = inbox
	thread := registry.Session{Codex: true, PID: native.PID + 1, ProcStart: "start", SessionID: uuid.NewString(), Name: "worker", Project: "demo", Status: "idle"}
	*r = testRegistry{native, thread}
	for _, s := range *r {
		handle, e := b.Connect(t.Context(), s)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { handle.Close() })
	}
	dir, e := b.ListPeers(t.Context(), native, "")
	if e != nil || len(dir.Peers) != 1 || dir.Peers[0].Name != "worker@demo:mac" || dir.Peers[0].Kind != contract.KindCodex || dir.Stale != nil {
		t.Fatal(dir, e)
	}
	res, e := b.SendMessage(t.Context(), native, mcpserver.SendArgs{To: dir.Peers[0].Name, Message: "ping"})
	if e != nil || !res.Accepted || !strings.Contains(value(res.Note), "Queued") || record.id != value(res.ID) || record.thread != thread.SessionID || !strings.HasSuffix(record.text, `and reply_to="`+record.id+`".)`) {
		t.Fatal(res, e)
	}
	first := record.id
	res, e = b.SendMessage(t.Context(), thread, mcpserver.SendArgs{To: dir.You.Name, Message: "pong", ReplyTo: &first})
	if e != nil || !res.Accepted {
		t.Fatal(res, e)
	}
	<-lines
	hash := sha256.Sum256([]byte(record.text))
	b.completedThread(thread.SessionID, hash, "second reply")
	if b.links[first].answer != "" {
		t.Fatal("explicit reply did not suppress fallback")
	}
	pair := [2]string{thread.MeshKey(), native.MeshKey()}
	slices.Sort(pair[:])
	if b.localHops[pair] != 2 {
		t.Fatal("reply bypassed hop count")
	}
	b.localHops[pair] = defaultHopLimit
	res, e = b.SendMessage(t.Context(), thread, mcpserver.SendArgs{To: dir.You.Name, Message: "again", ReplyTo: &first})
	if e != nil || res.Accepted || !strings.Contains(value(res.Reason), "paused") {
		t.Fatal(res, e)
	}
	thread.HumanTurn = "new-turn"
	if e = b.HumanTurn(t.Context(), thread); e != nil {
		t.Fatal(e)
	}
	res, e = b.SendMessage(t.Context(), native, mcpserver.SendArgs{To: dir.Peers[0].Name, Message: "another ping"})
	if e != nil || !res.Accepted {
		t.Fatal(res, e)
	}
	b.completedThread(thread.SessionID, sha256.Sum256([]byte(record.text)), "fallback")
	b.flushThreadReplies(t.Context())
	<-lines
	if e = b.HumanTurn(t.Context(), thread); e != nil || b.localHops[pair] != 2 {
		t.Fatal("same turn reset a second time", e)
	}
}

func TestCodexHubRegistrationAndConversationReply(t *testing.T) {
	thread := registry.Session{Codex: true, SessionID: uuid.NewString(), PID: 12, ProcStart: "start", Entrypoint: "codex", Name: "review", Project: "demo", Status: "idle"}
	key := thread.MeshKey()
	resumed := thread
	resumed.PID++
	resumed.ProcStart = "resumed"
	if resumed.MeshKey() != key {
		t.Fatal("process change lost thread identity")
	}
	const peer = "atlas"
	sent := make(chan contract.SendRequest, 1)
	registered := false
	b, r := fixture(t, map[string]http.HandlerFunc{
		"PUT /peer/sessions/{key}": func(w http.ResponseWriter, r *http.Request) {
			var v contract.SessionRegisterRequest
			_ = json.NewDecoder(r.Body).Decode(&v)
			registered = r.PathValue("key") == key && v.SessionID == thread.SessionID && v.Entrypoint == "codex" && v.ClaudeCode == ""
			_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: key, Address: "review@demo:mac"}})
		},
		"POST /peer/heartbeat": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{}")) },
		"POST /peer/reports": func(w http.ResponseWriter, r *http.Request) {
			var req contract.ReportsRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			var rep contract.StateReport
			_ = json.Unmarshal(req.Reports[0], &rep)
			if rep.State != "accepted" {
				t.Error("wrong Codex receipt", rep.State)
			}
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: rep.ID, Applied: true, State: optional(contract.DeliveryStateAccepted)}}})
		},
		"POST /peer/messages": func(w http.ResponseWriter, r *http.Request) {
			var req contract.SendRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			sent <- req
			var res contract.SendResponse
			_ = json.Unmarshal(hubtest.Example(t, "sendResponse"), &res)
			res.Message.ID = req.ID
			_ = json.NewEncoder(w).Encode(res)
		},
	})
	*r = testRegistry{thread}
	record := new(threadRecorder)
	b.threads = record
	handle, err := b.Connect(t.Context(), thread)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err = b.reconcile(t.Context()); err != nil || !registered {
		t.Fatal("registration", err)
	}
	conn, _ := b.client.Connection()
	if err = b.prepareLog(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	var msg contract.MessageEvent
	_ = json.Unmarshal(hubtest.Example(t, "eventMessage"), &msg)
	msg.Session = key
	msg.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	msg.Endpoint = contract.EndpointRef{ID: "pe_demo", Peer: peer, Name: "atlas"}
	if _, err = b.bind(conn, msg.Endpoint, []string{key}, true); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = b.receive(t.Context(), conn, msg); err != nil {
			t.Fatal(err)
		}
	}
	if record.writes != 1 || record.id != msg.ID || !strings.Contains(record.text, `reply_to="pe_demo"`) {
		t.Fatal("incorrect delivery or duplicate")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.client.Drain(ctx, b.reregister, b.completed) }()
	defer func() { cancel(); <-done }()
	res, err := b.SendMessage(ctx, thread, mcpserver.SendArgs{To: peer, Message: "answer", ReplyTo: &msg.ID})
	if err != nil || !res.Accepted {
		t.Fatal(res, err)
	}
	req := <-sent
	if value(req.To.Endpoint) != "pe_demo" || req.From.Session != key || req.To.Address != nil {
		t.Fatal("reply left the hub conversation")
	}
}

func TestCodexFallbackAnswerIsNotAnsweredAgain(t *testing.T) {
	b, r := fixture(t, map[string]http.HandlerFunc{})
	cfg := b.cfg
	cfg.HubURL = ""
	b.client = hub.New(cfg)
	record := &threadRecorder{}
	b.threads = record
	first := registry.Session{Codex: true, PID: 50, ProcStart: "start", SessionID: uuid.NewString(), Name: "first", Project: "demo", Status: "idle"}
	second := first
	second.PID, second.SessionID, second.Name = 51, uuid.NewString(), "second"
	*r = testRegistry{first, second}
	for _, s := range *r {
		handle, err := b.Connect(t.Context(), s)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { handle.Close() })
	}
	dir, err := b.ListPeers(t.Context(), first, "")
	if err != nil || len(dir.Peers) != 1 {
		t.Fatal(dir, err)
	}
	if res, e := b.SendMessage(t.Context(), first, mcpserver.SendArgs{To: dir.Peers[0].Name, Message: "ping"}); e != nil || !res.Accepted {
		t.Fatal(res, e)
	}
	b.completedThread(second.SessionID, sha256.Sum256([]byte(record.text)), "pong")
	b.flushThreadReplies(t.Context())
	if record.writes != 2 || record.thread != first.SessionID {
		t.Fatal("final answer did not return to the sender", record.writes)
	}
	b.completedThread(first.SessionID, sha256.Sum256([]byte(record.text)), "pong again")
	b.flushThreadReplies(t.Context())
	if record.writes != 2 {
		t.Fatal("a fallback answer was answered automatically")
	}
}

func TestCodexMemberSurvivesMissedScanWhilePipeOpen(t *testing.T) {
	thread := registry.Session{Codex: true, SessionID: uuid.NewString(), PID: 12, ProcStart: "start", Entrypoint: "codex", Name: "review", Project: "demo", Status: "idle"}
	key := thread.MeshKey()
	registrations, ends := 0, 0
	b, r := fixture(t, map[string]http.HandlerFunc{
		"PUT /peer/sessions/{key}": func(w http.ResponseWriter, _ *http.Request) {
			registrations++
			_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: key, Address: "review@demo:mac"}})
		},
		"POST /peer/sessions/{key}/end": func(w http.ResponseWriter, r *http.Request) {
			ends++
			hubtest.Reply(t, "sessionEndResponse")(w, r)
		},
		"POST /peer/heartbeat": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{}")) },
	})
	b.threads = new(threadRecorder)
	*r = testRegistry{thread}
	handle, err := b.Connect(t.Context(), thread)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err = b.reconcile(t.Context()); err != nil || registrations != 1 {
		t.Fatal("registration", registrations, err)
	}
	*r = testRegistry{}
	for range 2 {
		if err = b.reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, kept := b.members[key]; !kept || ends != 1 {
		t.Fatal("open pipe lost its membership", kept, ends)
	}
	*r = testRegistry{thread}
	if err = b.reconcile(t.Context()); err != nil || registrations != 2 {
		t.Fatal("thread did not register again", registrations, err)
	}
	if _, ok := b.member(key); !ok {
		t.Fatal("thread is not a member after it reappeared")
	}
}
