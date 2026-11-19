package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/control"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/hub/hubtest"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/relay/relaytest"
	"github.com/tenequm/amesh/internal/state"
)

func processSession(t *testing.T, inbox, dir string) registry.Session {
	t.Helper()
	pid := os.Getpid()
	var start string
	if runtime.GOOS == "darwin" {
		cmd := exec.CommandContext(t.Context(), "ps", "-o", "lstart=", "-p", strconv.Itoa(pid))
		cmd.Env = append(os.Environ(), "LC_ALL=C", "TZ=UTC")
		out, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		start = strings.TrimSpace(string(out))
	} else {
		raw, err := os.ReadFile("/proc/self/stat")
		if err != nil {
			t.Fatal(err)
		}
		start = strings.Fields(string(raw[strings.LastIndexByte(string(raw), ')')+1:]))[19]
	}
	path := filepath.Join(dir, strconv.Itoa(pid)+".sock")
	if err := os.Link(inbox, path); err != nil {
		t.Fatal(err)
	}
	entries := filepath.Join(dir, "sessions")
	if err := os.Mkdir(entries, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"pid": pid, "procStart": start, "sessionId": "amesh-mesh-test", "name": "review", "cwd": dir, "messagingSocketPath": path, "status": "idle", "version": "2.1.328", "peerProtocol": 1, "nameSource": "user", "entrypoint": "cli", "startedAt": time.Now().UnixMilli()})
	if err := os.WriteFile(filepath.Join(entries, strconv.Itoa(pid)+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := registry.New([]string{dir}).Resolve(t.Context(), pid)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPresenceWhileRegistryEntryIsBeingWritten(t *testing.T) {
	inbox, _, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	dir := meshDir(t)
	healthy := processSession(t, inbox, dir)
	pid := os.Getppid()
	start, err := registry.ProcessStart(pid)
	if err != nil {
		t.Fatal(err)
	}
	entry := map[string]any{"pid": pid, "procStart": start, "sessionId": "other", "name": "other", "cwd": dir, "messagingSocketPath": filepath.Join(dir, strconv.Itoa(pid)+".sock"), "peerProtocol": 1}
	path := filepath.Join(dir, "sessions", strconv.Itoa(pid)+".json")
	writeEntry := func() {
		t.Helper()
		raw, marshalErr := json.Marshal(entry)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if writeErr := os.WriteFile(path, raw, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	writeEntry()
	var registrations []contract.SessionRegisterRequest
	var beats []contract.HeartbeatRequest
	b, _ := fixture(t, map[string]http.HandlerFunc{
		"PUT /peer/sessions/{key}": func(w http.ResponseWriter, r *http.Request) {
			var req contract.SessionRegisterRequest
			if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
				t.Error(decodeErr)
			}
			registrations = append(registrations, req)
			_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: r.PathValue("key"), Address: req.Name + "@test:mac"}})
		},
		"POST /peer/heartbeat": func(w http.ResponseWriter, r *http.Request) {
			var req contract.HeartbeatRequest
			if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
				t.Error(decodeErr)
			}
			beats = append(beats, req)
			_, _ = io.WriteString(w, "{}")
		},
	})
	r := registry.New([]string{dir})
	b.sessions = r
	if err = r.SelfCheck(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Connect(t.Context(), healthy); err != nil {
		t.Fatal(err)
	}
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 1 || registrations[0].SessionID != healthy.SessionID || registrations[0].NameSource != "user" || len(beats) != 1 || len(beats[0].Sessions) != 1 || beats[0].Sessions[0].Key != sessionKey(healthy.Key) {
		t.Fatal(registrations, beats)
	}

	entry["status"] = "busy"
	writeEntry()
	other, err := r.Resolve(t.Context(), pid)
	if err != nil {
		t.Fatal(err)
	}
	pipe, err := b.Connect(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	b.lastHeartbeat = time.Time{}
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 2 || registrations[1].SessionID != "other" || len(beats) != 2 || len(beats[1].Sessions) != 2 {
		t.Fatal(registrations, beats)
	}

	if err = pipe.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte(`{"pid":`), 0o600); err != nil {
		t.Fatal(err)
	}
	b.lastHeartbeat = time.Time{}
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !b.IsMember(other.Key) || len(beats) != 3 || !slices.Equal(beats[1].Sessions, beats[2].Sessions) {
		t.Fatal("truncated registry file ended a member", beats)
	}
	entry["name"] = "renamed"
	writeEntry()
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 3 || registrations[2].Name != "renamed" {
		t.Fatal(registrations)
	}
}

func TestDeliveryRetriesAnIncompleteEntry(t *testing.T) {
	inbox, _, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	dir := meshDir(t)
	s := processSession(t, inbox, dir)
	path := filepath.Join(dir, "sessions", strconv.Itoa(s.PID)+".json")
	complete, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	key := sessionKey(s.Key)
	b := &Backend{sessions: registry.New([]string{dir}), members: map[string]member{key: {session: s, hosted: true}}}
	synctest.Test(t, func(t *testing.T) {
		done := make(chan bool, 1)
		go func() {
			got, live, lookupErr := b.deliverySession(t.Context(), key)
			done <- live && lookupErr == nil && got.Key == s.Key
		}()
		synctest.Wait()
		if err := os.WriteFile(path, complete, 0o600); err != nil {
			t.Fatal(err)
		}
		if !<-done {
			t.Fatal("delivery did not recover after the entry became ready")
		}
		if err := os.WriteFile(path, []byte("!"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := b.deliverySession(t.Context(), key); !errors.Is(err, registry.ErrEntryUnavailable) {
			t.Fatalf("bad entry did not release the delivery worker: %v", err)
		}
	})
}

func TestCrashBeforeAcknowledgement(t *testing.T) {
	if dir := os.Getenv("AMESH_MESH_CRASH_TEST_DIR"); dir != "" {
		cfg := config.Config{StateDir: dir, MachineName: "mac", HubURL: os.Getenv("AMESH_MESH_CRASH_TEST_URL"), ClaudeConfigDirs: []string{os.Getenv("AMESH_MESH_TEST_SEAT")}, SocketPath: filepath.Join(dir, "amesh-mesh-control.sock")}
		st := state.New(dir)
		sessions := registry.New(cfg.ClaudeConfigDirs)
		relays := relay.New(dir, st)
		b := New(cfg, sessions, relays, hub.New(cfg))
		b.ack = func(ctx context.Context, _ int64) error {
			f := os.NewFile(3, "amesh-mesh-before-ack")
			_, _ = f.Write([]byte{1})
			_ = f.Close()
			<-ctx.Done()
			return ctx.Err()
		}
		stop, err := b.Start(t.Context(), st, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer stop()
		t.Fatal(control.Serve(t.Context(), cfg, sessions, nil, b, relays.OwnsRelay))
		return
	}

	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	seat := meshDir(t)
	s := processSession(t, inbox, seat)
	key := sessionKey(s.Key)
	reports := make(chan contract.StateReport, 4)
	var msg contract.MessageEvent
	_ = json.Unmarshal(hubtest.Example(t, "eventMessage"), &msg)
	msg.Session = key
	msg.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	end := contract.EndpointEvent{Kind: "endpoint", ID: msg.Endpoint.ID, Name: msg.Endpoint.Name, State: "open", Sessions: []string{key}, Peer: contract.EndpointEventPeer{Address: "stan", Kind: "agent"}}
	endpointData, _ := json.Marshal(end)
	messageData, _ := json.Marshal(msg)
	b, _ := fixture(t, map[string]http.HandlerFunc{
		"POST /peer/heartbeat": func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{}") },
		"PUT /peer/sessions/{key}": func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: key, Address: "review@pond:mac"}})
		},
		"GET /peer/events": func(w http.ResponseWriter, r *http.Request) {
			if after, e := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64); e != nil || after >= msg.Seq {
				t.Error("cursor advanced before the process was killed")
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "event: endpoint\ndata: %s\n\nevent: message\nid: %d\ndata: %s\n\n", endpointData, msg.Seq, messageData)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		},
		"POST /peer/reports": func(w http.ResponseWriter, r *http.Request) {
			var req contract.ReportsRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			var report contract.StateReport
			_ = json.Unmarshal(req.Reports[0], &report)
			reports <- report
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: report.ID, State: optional(contract.DeliveryState(report.State)), Applied: true}}})
		},
	})
	b.cfg.ClaudeConfigDirs = []string{seat}
	b.sessions = registry.New(b.cfg.ClaudeConfigDirs)
	if _, err = b.Connect(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if err = b.store.Close(); err != nil {
		t.Fatal(err)
	}
	rd, wr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	defer wr.Close()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCrashBeforeAcknowledgement$")
	cmd.ExtraFiles = []*os.File{wr}
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stderr
	cmd.Env = append(os.Environ(), "AMESH_MESH_CRASH_TEST_DIR="+b.cfg.StateDir, "AMESH_MESH_CRASH_TEST_URL="+b.cfg.HubURL, "AMESH_MESH_TEST_SEAT="+seat)
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	_ = wr.Close()
	_ = rd.SetReadDeadline(time.Now().Add(10 * time.Second))
	var marker [1]byte
	if _, err = io.ReadFull(rd, marker[:]); err != nil {
		t.Fatal(err)
	}
	if err = control.Status(t.Context(), filepath.Join(b.cfg.StateDir, "amesh-mesh-control.sock"), io.Discard); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("crash helper survived kill")
	}
	var frame contract.NativeUserFrame
	if err = json.Unmarshal(<-lines, &frame); err != nil {
		t.Fatal(err)
	}
	parsed, err := envelope.Parse(frame.Message.Content)
	wantBody := msg.Text + "\n\n" + fmt.Sprintf(`(amesh: %s cannot see what you write here. To answer, call SendMessage with to="%s".)`, end.Name, value(frame.From))
	if err != nil || parsed.FromSession != end.ID || parsed.From != value(frame.From) || parsed.Body != wantBody || frame.MsgID != msg.ID || !slices.Equal(parsed.HopChain, msg.HopChain) {
		t.Fatal("incorrect inbound native frame")
	}
	b.client = hub.New(b.cfg)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ack := make(chan int64, 1)
	b.ack = func(ctx context.Context, seq int64) error { e := b.client.Ack(ctx, seq); ack <- seq; return e }
	stop, err := b.Start(ctx, b.store, b.cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	select {
	case seq := <-ack:
		if seq != msg.Seq {
			t.Fatal(seq)
		}
	case <-ctx.Done():
		t.Fatal("replay was not acknowledged")
	}
	select {
	case report := <-reports:
		if report.ID != msg.ID || report.State != "unknown" || report.Session != key {
			t.Fatal(report)
		}
	default:
		t.Fatal("missing startup report")
	}
	select {
	case <-lines:
		t.Fatal("replay duplicated the inbox write")
	default:
	}
	entries, err := b.store.OpenEntries("lg_3Xq9Vm2Lp7Kd")
	if err != nil || len(entries) != 0 {
		t.Fatal("recovery left an open record")
	}
}

func TestOrphanAndInterruptedReports(t *testing.T) {
	var states []contract.StateReport
	b, _ := fixture(t, map[string]http.HandlerFunc{
		"POST /peer/reports": func(w http.ResponseWriter, r *http.Request) {
			var req contract.ReportsRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			var rep contract.StateReport
			_ = json.Unmarshal(req.Reports[0], &rep)
			states = append(states, rep)
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: rep.ID, State: optional(contract.DeliveryState(rep.State)), Applied: true}}})
		},
		"POST /peer/sessions/{key}/end": hubtest.Reply(t, "sessionEndResponse"),
	})
	conn, _ := b.client.Connection()
	key := sessionKey(testSession().Key)
	if err := b.store.SetLog(conn.Log); err != nil {
		t.Fatal(err)
	}
	if _, err := b.store.BeginWrite(conn.Log, "first", key, time.Now().Add(time.Hour), "message"); err != nil {
		t.Fatal(err)
	}
	if err := b.prepareLog(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	if err := b.receive(t.Context(), conn, contract.MessageEvent{ID: "second", Session: key}); err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || value(states[0].Reason) != "interrupted" || value(states[1].Reason) != "unknown_session" {
		t.Fatalf("unexpected reports: %v", states)
	}
}

func postNative(t *testing.T, addr string, raw []byte) {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", strings.TrimPrefix(addr, "uds:"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
	if err = conn.(*net.UnixConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err = io.Copy(io.Discard, conn); err != nil {
		t.Fatal(err)
	}
}

func TestRelayReplyReceiptAndNotice(t *testing.T) {
	s := testSession()
	key := sessionKey(s.Key)
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	s.SocketPath = inbox
	s.FromMode = "bypass"
	received := make(chan contract.SendRequest, 1)
	reported := make(chan contract.StateReport, 1)
	b, r := fixture(t, map[string]http.HandlerFunc{
		"POST /peer/messages": func(w http.ResponseWriter, r *http.Request) {
			var req contract.SendRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			received <- req
			var res contract.SendResponse
			_ = json.Unmarshal(hubtest.Example(t, "sendResponse"), &res)
			res.Message.ID = req.ID
			_ = json.NewEncoder(w).Encode(res)
		},
		"POST /peer/reports": func(w http.ResponseWriter, r *http.Request) {
			var req contract.ReportsRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			var rep contract.StateReport
			_ = json.Unmarshal(req.Reports[0], &rep)
			reported <- rep
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: rep.ID, State: optional(contract.DeliveryState(rep.State)), Applied: true}}})
		},
	})
	*r = append(*r, s)
	_, err = b.Connect(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := b.client.Connection()
	if err = b.prepareLog(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.client.Drain(ctx, b.reregister, b.completed) }()
	defer func() { cancel(); <-done }()
	var msg contract.MessageEvent
	_ = json.Unmarshal(hubtest.Example(t, "eventMessage"), &msg)
	msg.Session = key
	msg.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	ep, err := b.bind(conn, msg.Endpoint, []string{key}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.receive(ctx, conn, msg); err != nil {
		t.Fatal(err)
	}
	<-lines
	postNative(t, ep.from, hubtest.Example(t, "nativeReplyFrame"))
	select {
	case req := <-received:
		if req.Text != "Yes: the deploy finished at 11:58 UTC and the health check is green." || req.From.Session != key || value(req.To.Endpoint) != ep.ref.ID || req.ID != "1a616f5d-ad9d-4185-a462-0bf442288517" || req.Origin != "peer" || value(req.FromMode) != "bypass" || len(req.HopChain) != 1 {
			t.Fatal("incorrect native reply request")
		}
	default:
		t.Fatal("no reply posted")
	}
	postNative(t, ep.from, hubtest.Example(t, "receiptDelivered"))
	select {
	case rep := <-reported:
		if rep.Session != key || rep.ID != msg.ID || rep.State != "delivered" {
			t.Fatal(rep)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no report")
	}
	n, err := noticeEvent(contract.EventStreamItem{Event: "limit", Data: string(hubtest.Example(t, "eventLimit"))})
	if err != nil {
		t.Fatal(err)
	}
	n.session = key
	for range 2 {
		if err = b.writeNotice(ctx, conn, n); err != nil {
			t.Fatal(err)
		}
	}
	var frame contract.NativeUserFrame
	if err = json.Unmarshal(<-lines, &frame); err != nil {
		t.Fatal(err)
	}
	note, err := envelope.Parse(frame.Message.Content)
	if err != nil || value(frame.From) != ep.from || note.FromName != ep.ref.Name || note.FromMode != "bypass" || len(note.HopChain) != 0 || !strings.HasSuffix(note.Body, "No reply needed.") {
		t.Fatal("incorrect conversation notice")
	}
	select {
	case <-lines:
		t.Fatal("duplicate notice")
	default:
	}
	addr := "stan"
	req := contract.SendRequest{ID: msg.ID, From: contract.SendRequestFrom{Session: key}, To: contract.SendRequestTo{Address: &addr}}
	if err = b.completed(hub.SendOutcome{NoticeRequired: true, Request: req, Err: &hub.HTTPError{Body: contract.ErrorBody{Error: "log_replaced"}}}); err != nil {
		t.Fatal(err)
	}
	frame = contract.NativeUserFrame{}
	if err = json.Unmarshal(<-lines, &frame); err != nil {
		t.Fatal(err)
	}
	note, err = envelope.Parse(frame.Message.Content)
	if err != nil || frame.From != nil || note.FromName != "amesh" || !strings.Contains(note.Body, "the hub was replaced") {
		t.Fatal("missing no-from notice")
	}
}

func TestOfflineRestartWritesExpiryNotice(t *testing.T) {
	s := testSession()
	key := sessionKey(s.Key)
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	s.SocketPath = inbox
	b, r := fixture(t, map[string]http.HandlerFunc{})
	*r = append(*r, s)
	if _, err = b.Connect(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	conn, _ := b.client.Connection()
	if err = b.prepareLog(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	to := "stan"
	req := contract.SendRequest{ID: "c8ce54bb-1dc0-4aa0-9d60-f131492a685e", From: contract.SendRequestFrom{Session: key}, To: contract.SendRequestTo{Address: &to}, Text: "queued"}
	// Use virtual time for the full TTL. The fake inbox lives outside the bubble.
	synctest.Test(t, func(t *testing.T) {
		b.client = hub.New(b.cfg)
		if _, sendErr := b.client.Send(t.Context(), req); !errors.Is(sendErr, hub.ErrQueued) {
			t.Fatal(sendErr)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if runErr := b.client.Drain(ctx, nil, func(out hub.SendOutcome) error {
			defer cancel()
			return b.completed(out)
		}); !errors.Is(runErr, context.Canceled) {
			t.Fatal(runErr)
		}
	})
	var frame contract.NativeUserFrame
	select {
	case line := <-lines:
		if err = json.Unmarshal(line, &frame); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("expired send did not notify the offline session")
	}
	want := (relay.NoticeSource{MachineID: conn.Machine.ID, Log: conn.Log, Kind: "outbox", Source: req.ID}).ID()
	if frame.MsgID != want || frame.From != nil || !strings.Contains(frame.Message.Content, "could not reach the hub for 24 hours") {
		t.Fatal("incorrect offline expiry notice")
	}
}

func TestDeferredRefusalSurvivesFailedNoticeWrite(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		reply      bool
	}{
		{"start", "new_permanent_code", false}, {"reply", "id_conflict", true}, {"start-conflict", "id_conflict", false}, {"reply-limit", "hop_limit", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts atomic.Int32
			var hello contract.HelloResponse
			if err := json.Unmarshal(hubtest.Example(t, "helloResponse"), &hello); err != nil {
				t.Fatal(err)
			}
			hello.Settings.SendWaitMs = 0
			b, r := fixture(t, map[string]http.HandlerFunc{"POST /peer/hello": func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(hello) }, "POST /peer/messages": func(w http.ResponseWriter, _ *http.Request) {
				posts.Add(1)
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(contract.ErrorBody{Error: contract.ErrorCode(tc.code), Message: "refusal detail"})
			}})
			conn, _ := b.client.Connection()
			if err := b.prepareLog(t.Context(), conn); err != nil {
				t.Fatal(err)
			}
			s := testSession()
			s.FromMode = "bypass"
			s.SocketPath = filepath.Join(b.cfg.StateDir, "amesh-a2-missing.sock")
			*r = append(*r, s)
			if _, err := b.Connect(t.Context(), s); err != nil {
				t.Fatal(err)
			}
			ep := contract.EndpointRef{ID: "pe_amesha2example", Peer: "stan", Name: "stan"}
			req := contract.SendRequest{ID: "54ad69a0-b410-4a59-9fb1-90ff72440102", From: contract.SendRequestFrom{Session: sessionKey(s.Key)}, Text: "test", Origin: contract.SendRequestOriginUnknown}
			if tc.reply {
				req.To.Endpoint = &ep.ID
			} else {
				req.To.Address = &ep.Peer
			}
			_, sendErr := b.client.SendReply(t.Context(), req, ep)
			if !errors.Is(sendErr, hub.ErrQueued) {
				t.Fatal(sendErr)
			}

			drain := func() error {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				return b.client.Drain(ctx, b.reregister, func(out hub.SendOutcome) error {
					err := b.completed(out)
					cancel()
					return err
				})
			}
			err := drain()
			if tc.code == "hop_limit" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if n, e := b.client.Pending(""); n != 0 || e != nil {
					t.Fatal(n, e)
				}
				return
			}
			if err == nil || errors.Is(err, context.Canceled) {
				t.Fatal("missing inbox did not fail the notice")
			}
			if n, e := b.client.Pending(""); n != 1 || e != nil {
				t.Fatal(n, e)
			}
			inbox, lines, e := relaytest.Inbox(t)
			if e != nil {
				t.Fatal(e)
			}
			(*r)[0].SocketPath = inbox
			b.client = hub.New(b.cfg)
			if err = drain(); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if n, e := b.client.Pending(""); n != 0 || e != nil || posts.Load() != 1 {
				t.Fatal(n, e, posts.Load())
			}
			var f contract.NativeUserFrame
			if err = json.Unmarshal(<-lines, &f); err != nil {
				t.Fatal(err)
			}
			note, err := envelope.Parse(f.Message.Content)
			word := "message"
			if tc.reply {
				word = "reply"
				path, e := relay.Path(b.socketDir, "amesh-relay\n"+conn.Machine.ID, ep.ID)
				if e != nil || note.From != envelope.Address(path) || note.FromSession != ep.ID {
					t.Fatal("wrong reply relay")
				}
			} else if f.From != nil || note.From != "" {
				t.Fatal("unexpected start relay")
			}
			if err != nil || note.Body != "amesh notice: your "+word+" to stan was not delivered: refusal detail No reply needed." || note.FromMode != "bypass" {
				t.Fatal("wrong refusal notice", err)
			}
			if f.MsgID != (relay.NoticeSource{MachineID: conn.Machine.ID, Log: conn.Log, Source: req.ID, Kind: "outbox"}).ID() {
				t.Fatal("wrong notice identity")
			}
		})
	}
}

func TestAgentDeliveryAndEndpointClosure(t *testing.T) {
	b, _ := fixture(t, map[string]http.HandlerFunc{})
	e := streamRun{b: b}
	item := contract.EventStreamItem{Event: "endpoint", Data: string(hubtest.Example(t, "eventEndpointSnapshot"))}
	if err := e.handle(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	var ep contract.EndpointEvent
	if err := json.Unmarshal([]byte(item.Data), &ep); err != nil {
		t.Fatal(err)
	}
	bound, ok := b.endpoint(ep.ID)
	if !ok || !b.relays.OwnsRelay(bound.from) {
		t.Fatal("snapshot relay missing")
	}
	ep.State = "closed"
	ep.Sessions = nil
	data, _ := json.Marshal(ep)
	item.Data = string(data)
	if err := e.handle(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	if b.relays.OwnsRelay(bound.from) {
		t.Fatal("closed relay still bound")
	}
	for _, reason := range []string{"route_failed", "not_member"} {
		ev := contract.DeliveryEvent{State: "accepted", Reason: &reason}
		raw, _ := json.Marshal(ev)
		n, err := noticeEvent(contract.EventStreamItem{Event: "delivery", Data: string(raw)})
		if err != nil || n.cause != "" {
			t.Fatal("agent acceptance produced a notice", err)
		}
	}
}

func TestStartupRewriteWaitsForHubLog(t *testing.T) {
	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	s := testSession()
	s.SocketPath = inbox
	s.ProcStart, err = registry.ProcessStart(s.PID)
	if err != nil {
		t.Fatal(err)
	}
	key := sessionKey(s.Key)
	const id = "54ad69a0-b410-4a59-9fb1-90ff72440102"
	hello := hubtest.Reply(t, "helloResponse")
	helloFinished := make(chan struct{})
	var hellos atomic.Int32
	reports := make(chan contract.StateReport, 2)
	ep := contract.EndpointEvent{Kind: "endpoint", ID: "pe_test", Name: "stan", Sessions: []string{key}, State: contract.EndpointEventStateOpen, Peer: contract.EndpointEventPeer{Address: "stan", Kind: contract.KindAgent}}
	b, r := fixture(t, map[string]http.HandlerFunc{
		"POST /peer/hello": func(w http.ResponseWriter, r *http.Request) {
			if hellos.Add(1) == 2 {
				defer close(helloFinished)
				select {
				case <-lines:
					t.Error("rewrite reached inbox before hello")
				case <-time.After(3 * time.Second):
				case <-r.Context().Done():
					return
				}
			}
			hello(w, r)
		},
		"POST /peer/reports": func(w http.ResponseWriter, r *http.Request) {
			var req contract.ReportsRequest
			var report contract.StateReport
			if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil || len(req.Reports) != 1 || json.Unmarshal(req.Reports[0], &report) != nil {
				t.Error("invalid report")
				return
			}
			reports <- report
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: id}}})
		},
		"PUT /peer/sessions/{key}": func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(contract.SessionRegisterResponse{Session: contract.SessionInfo{Key: key, Address: "review@pond:mac"}})
		},
		"POST /peer/heartbeat": func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{}") },
		"GET /peer/events": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			data, _ := json.Marshal(ep)
			fmt.Fprintf(w, "event: endpoint\ndata: %s\n\n", data)
			fmt.Fprint(w, "event: synchronized\ndata: {\"kind\":\"synchronized\",\"seq\":0}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		},
	})
	*r = append(*r, s)
	if _, err = b.Connect(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	conn, _ := b.client.Connection()
	if err = b.store.SetLog(conn.Log); err != nil {
		t.Fatal(err)
	}
	if _, err = b.store.BeginWrite(conn.Log, id, key, time.Now().Add(time.Hour), "message"); err != nil {
		t.Fatal(err)
	}
	path, err := relay.Path(b.socketDir, "amesh-relay\n"+conn.Machine.ID, ep.ID)
	if err != nil {
		t.Fatal(err)
	}
	content, err := envelope.Render(envelope.Message{From: envelope.Address(path), FromName: ep.Name, FromSession: ep.ID, Body: "test"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"inbox": inbox, "content": content, "pid": s.PID, "uid": os.Geteuid(), "procStart": s.ProcStart, "rewrites": 1, "acceptWindow": time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.store.SetRewrite(id, raw); err != nil {
		t.Fatal(err)
	}
	if err = b.store.Close(); err != nil {
		t.Fatal(err)
	}
	b.client = hub.New(b.cfg)
	stop, err := b.Start(t.Context(), b.store, b.cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	<-helloFinished
	select {
	case <-lines:
	case <-time.After(8 * time.Second):
		t.Error("missing rewrite after log preparation")
	}
	select {
	case report := <-reports:
		if report.State != "accepted" || report.ID != id {
			t.Fatalf("rewritten delivery reported as %q", report.State)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rewritten delivery had no receipt subscription")
	}
}

func TestNullReportAcknowledgementClosesEntry(t *testing.T) {
	const id = "unknown-id"
	b, _ := fixture(t, map[string]http.HandlerFunc{"POST /peer/reports": func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: id}}})
	}})
	conn, _ := b.client.Connection()
	if err := b.store.SetLog(conn.Log); err != nil {
		t.Fatal(err)
	}
	if _, err := b.store.BeginWrite(conn.Log, id, "session", time.Now().Add(time.Hour), "message"); err != nil {
		t.Fatal(err)
	}
	if err := b.prepareLog(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	if open, err := b.store.OpenEntries(conn.Log); err != nil || len(open) != 0 {
		t.Fatal(open, err)
	}
}

func TestNotHostedEndWaitsForGoneRecipientsNotice(t *testing.T) {
	var ends atomic.Int32
	var hello contract.HelloResponse
	if err := json.Unmarshal(hubtest.Example(t, "helloResponse"), &hello); err != nil {
		t.Fatal(err)
	}
	hello.Settings.SendWaitMs = 0
	key := sessionKey(testSession().Key)
	b, reg := fixture(t, map[string]http.HandlerFunc{
		"POST /peer/hello": func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(hello) },
		"POST /peer/reports": func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: "incoming"}}})
		},
		"POST /peer/heartbeat": func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{}") },
		"POST /peer/messages":  hubtest.Reply(t, "errNotFound"),
		"POST /peer/sessions/{key}/end": func(w http.ResponseWriter, r *http.Request) {
			ends.Add(1)
			hubtest.Reply(t, "sessionEndResponse")(w, r)
		},
	})
	to := "stan"
	req := contract.SendRequest{From: contract.SendRequestFrom{Session: key}, ID: "outgoing", To: contract.SendRequestTo{Address: &to}, Text: "queued"}
	if _, err := b.client.Send(t.Context(), req); !errors.Is(err, hub.ErrQueued) {
		t.Fatal(err)
	}
	msg := contract.MessageEvent{ID: "incoming", Session: key}
	if err := b.store.SetLog(hello.Log); err != nil {
		t.Fatal(err)
	}
	if err := b.receive(t.Context(), hello, msg); err != nil {
		t.Errorf("orphan delivery ended before drain: %v", err)
	}
	if n := ends.Load(); n != 0 {
		t.Errorf("not-hosted end passed pending outbox: %d", n)
	}
	s := testSession()
	*reg = append(*reg, s)
	pipe, err := b.Connect(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	_ = pipe.Close()
	*reg = nil
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err = b.client.Drain(ctx, nil, func(out hub.SendOutcome) error { defer cancel(); return b.completed(out) })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("gone recipient retained notice: %v", err)
	}
	if n, pendingErr := b.client.Pending(key); n != 0 || pendingErr != nil {
		t.Fatal(n, pendingErr)
	}
	if err = b.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := ends.Load(); n != 1 {
		t.Fatalf("expected one end, got %d", n)
	}
}

func TestFailedBindKeepsAcceptedOutboxEntry(t *testing.T) {
	var hello contract.HelloResponse
	if err := json.Unmarshal(hubtest.Example(t, "helloResponse"), &hello); err != nil {
		t.Fatal(err)
	}
	hello.Settings.SendWaitMs = 0
	var res contract.SendResponse
	if err := json.Unmarshal(hubtest.Example(t, "sendResponse"), &res); err != nil {
		t.Fatal(err)
	}
	b, _ := fixture(t, map[string]http.HandlerFunc{
		"POST /peer/hello":    func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(hello) },
		"POST /peer/messages": hubtest.Reply(t, "sendResponse"),
	})
	req := contract.SendRequest{ID: res.Message.ID, From: contract.SendRequestFrom{Session: sessionKey(testSession().Key)}, To: contract.SendRequestTo{Address: &res.Message.To.Address}, Text: "queued"}
	if _, err := b.client.Send(t.Context(), req); !errors.Is(err, hub.ErrQueued) {
		t.Fatal(err)
	}
	path, err := relay.Path(b.socketDir, "amesh-relay\n"+hello.Machine.ID, res.Message.Endpoint.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = b.client.Drain(t.Context(), nil, b.completed); err == nil {
		t.Fatal("expected bind failure")
	}
	if n, pendingErr := b.client.Pending(""); n != 1 || pendingErr != nil {
		t.Fatalf("pending after bind failure: %d, %v", n, pendingErr)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err = b.client.Drain(ctx, nil, func(out hub.SendOutcome) error { defer cancel(); return b.completed(out) })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, ok := b.endpoint(res.Message.Endpoint.ID); !ok {
		t.Fatal("missing bound relay")
	}
	if n, pendingErr := b.client.Pending(""); n != 0 || pendingErr != nil {
		t.Fatal(n, pendingErr)
	}
}
