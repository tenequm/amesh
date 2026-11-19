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
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: report.ID, State: &report.State, Applied: true}}})
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
	if err != nil || parsed.FromSession != end.ID || parsed.Body != msg.Text || frame.MsgID != msg.ID || !slices.Equal(parsed.HopChain, msg.HopChain) {
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
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: rep.ID, State: &rep.State, Applied: true}}})
		},
		"POST /peer/sessions/{key}/end": hubtest.Reply(t, "sessionEndResponse"),
	})
	conn, _ := b.client.Connection()
	key := sessionKey(testSession().Key)
	if err := b.store.SetLog(conn.Log); err != nil {
		t.Fatal(err)
	}
	if _, err := b.store.BeginWrite(conn.Log, "first", key, time.Now().Add(time.Hour), false); err != nil {
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
			_ = json.NewEncoder(w).Encode(contract.ReportsResponse{Results: []contract.ReportsResponseResultsItem{{ID: rep.ID, State: &rep.State, Applied: true}}})
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
	go func() { done <- b.client.RunOutbox(ctx, b.completed) }()
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
		if req.From.Session != key || value(req.To.Endpoint) != ep.ref.ID || req.ID != "1a616f5d-ad9d-4185-a462-0bf442288517" || req.Origin != "peer" || value(req.FromMode) != "bypass" || len(req.HopChain) != 1 {
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
	b.completed(hub.SendOutcome{Request: req, Err: &hub.HTTPError{Body: contract.ErrorBody{Error: "log_replaced"}}})
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
		if runErr := b.client.RunOutbox(ctx, func(out hub.SendOutcome) { b.completed(out); cancel() }); !errors.Is(runErr, context.Canceled) {
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
	want := (relay.NoticeSource{MachineID: conn.Machine.ID, Kind: "outbox", Source: req.ID}).ID()
	if frame.MsgID != want || frame.From != nil || !strings.Contains(frame.Message.Content, "could not reach the hub for 24 hours") {
		t.Fatal("incorrect offline expiry notice")
	}
}
