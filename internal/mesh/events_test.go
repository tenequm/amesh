package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/hub/hubtest"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/relay/relaytest"
	"github.com/tenequm/amesh/internal/state"
)

func TestCrashBeforeAcknowledgement(t *testing.T) {
	if dir := os.Getenv("AMESH_MESH_CRASH_TEST_DIR"); dir != "" {
		cfg := config.Config{StateDir: dir, MachineName: "mac", HubURL: os.Getenv("AMESH_MESH_CRASH_TEST_URL")}
		st := state.New(dir)
		if err := st.Lock(); err != nil {
			t.Fatal(err)
		}
		b := New(cfg, nil, relay.New(dir, st), hub.New(cfg))
		s := testSession()
		s.PID, _ = strconv.Atoi(os.Getenv("AMESH_MESH_CRASH_TEST_PID"))
		s.SocketPath = os.Getenv("AMESH_MESH_CRASH_TEST_INBOX")
		b.sessions = &testRegistry{s}
		b.store = st
		b.socketDir = dir
		b.ctx = t.Context()
		if err := b.restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := b.client.Hello(t.Context(), contract.HelloRequest{}); err != nil {
			t.Fatal(err)
		}
		b.ack = func(ctx context.Context, _ int64) error {
			f := os.NewFile(3, "amesh-mesh-before-ack")
			_, _ = f.Write([]byte{1})
			_ = f.Close()
			<-ctx.Done()
			return ctx.Err()
		}
		t.Fatal(b.stream(t.Context()))
		return
	}

	inbox, lines, err := relaytest.Inbox(t)
	if err != nil {
		t.Fatal(err)
	}
	s := testSession()
	s.SocketPath = inbox
	key := sessionKey(s.Key)
	reports := make(chan contract.StateReport, 4)
	var msg contract.MessageEvent
	_ = json.Unmarshal(hubtest.Example(t, "eventMessage"), &msg)
	msg.Session = key
	msg.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	end := contract.EndpointEvent{Kind: "endpoint", ID: msg.Endpoint.ID, Name: msg.Endpoint.Name, State: "open", Sessions: []string{key}, Peer: contract.EndpointEventPeer{Address: "stan", Kind: "agent"}}
	endpointData, _ := json.Marshal(end)
	messageData, _ := json.Marshal(msg)
	b, r := fixture(t, map[string]http.HandlerFunc{
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
	*r = append(*r, s)
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
	cmd.Env = append(os.Environ(), "AMESH_MESH_CRASH_TEST_DIR="+b.cfg.StateDir, "AMESH_MESH_CRASH_TEST_URL="+b.cfg.HubURL, "AMESH_MESH_CRASH_TEST_INBOX="+inbox, "AMESH_MESH_CRASH_TEST_PID="+strconv.Itoa(s.PID))
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
	if err = b.store.Lock(); err != nil {
		t.Fatal(err)
	}
	b.client = hub.New(b.cfg)
	if _, err = b.client.Hello(t.Context(), contract.HelloRequest{}); err != nil {
		t.Fatal(err)
	}
	if err = b.restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	b.socketDir = b.cfg.StateDir
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	b.ctx = ctx
	ack := make(chan int64, 1)
	b.ack = func(ctx context.Context, seq int64) error { e := b.client.Ack(ctx, seq); ack <- seq; return e }
	done := make(chan error, 1)
	go func() { done <- b.stream(ctx) }()
	defer func() { cancel(); <-done; b.closeEndpoints() }()
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
