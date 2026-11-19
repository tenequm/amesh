package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/registry"

	"github.com/tenequm/amesh/internal/version"
)

func TestVersionBeforeConfiguration(t *testing.T) {
	t.Setenv("AMESH_STATE_DIR", "relative")
	var out bytes.Buffer
	err := run(t.Context(), []string{"-version"}, nil, &out, io.Discard)
	if err != nil || out.String() != version.Value+"\n" {
		t.Fatalf("version output %q, error %v", out.String(), err)
	}
}

func TestHelpKeepsEnvironmentPrivate(t *testing.T) {
	t.Setenv("AMESH_HUB_URL", "private-value")
	var help bytes.Buffer
	err := run(t.Context(), []string{"-help"}, nil, io.Discard, &help)
	if !errors.Is(err, flag.ErrHelp) || bytes.Contains(help.Bytes(), []byte("private-value")) {
		t.Fatal("expected help using built-in defaults")
	}
}

func TestLaneThroughUnenrolledDaemon(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "amesh-a2-") //nolint:usetesting // Native and control sockets need short paths.
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("XDG_RUNTIME_DIR", dir)
	cfg := config.Config{StateDir: filepath.Join(dir, "state"), HubURL: "http://127.0.0.1:0/peer", SocketPath: filepath.Join(dir, "amesh-a2.sock"), ClaudeConfigDirs: []string{filepath.Join(dir, "amesh-a2-parent"), filepath.Join(dir, "amesh-a2-child")}}
	sockets := filepath.Join(dir, "cc-socks")
	if err = os.Mkdir(sockets, 0o700); err != nil {
		t.Fatal(err)
	}
	var parentInbox *net.UnixListener
	for i, pid := range []int{os.Getppid(), os.Getpid()} {
		seat := cfg.ClaudeConfigDirs[i]
		if err = os.MkdirAll(filepath.Join(seat, "sessions"), 0o700); err != nil {
			t.Fatal(err)
		}
		start, e := registry.ProcessStart(pid)
		if e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(sockets, strconv.Itoa(pid)+".sock")
		ln, e := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = ln.Close() })
		if i == 0 {
			parentInbox = ln
		}
		row := map[string]any{"pid": pid, "procStart": start, "sessionId": filepath.Base(seat), "name": filepath.Base(seat), "cwd": dir, "messagingSocketPath": path, "peerProtocol": 1, "status": "idle"}
		raw, e := json.Marshal(row)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(seat, "sessions", strconv.Itoa(pid)+".json"), raw, 0o600); e != nil {
			t.Fatal(e)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	done := make(chan error, 1)
	go func() { done <- daemon(ctx, cfg) }()
	defer func() {
		cancel()
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	status := func() (contract.DaemonStatus, error) {
		var out bytes.Buffer
		var result contract.DaemonStatus
		e := run(ctx, []string{"status", "-socket-path", cfg.SocketPath}, nil, &out, io.Discard)
		if e == nil {
			e = json.Unmarshal(out.Bytes(), &result)
		}
		return result, e
	}
	for {
		st, e := status()
		if e == nil && st.Hub.State == "halted" && st.Hub.Halt != nil && st.Hub.Halt.Error == "unknown_machine" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("offline daemon status: %v, %v", st.Hub, e)
		case <-time.After(10 * time.Millisecond):
		}
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	limit, _ := ctx.Deadline()
	if err = conn.SetDeadline(limit); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	_, err = fmt.Fprint(conn, "GET /amesh/mcp?arg=--parent&arg=amesh-a2-parent&reconnect=1 HTTP/1.1\r\nHost: amesh.localhost\r\nConnection: Upgrade\r\nUpgrade: mcp-stdio\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatal(resp.Status)
	}
	_, err = fmt.Fprintln(conn, `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	if err != nil {
		t.Fatal(err)
	}
	var tools struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err = json.NewDecoder(reader).Decode(&tools); err != nil || len(tools.Result.Tools) != 1 || tools.Result.Tools[0].Name != "send_to_parent" {
		t.Fatal(tools, err)
	}
	st, err := status()
	if err != nil || len(st.Lanes) != 1 || st.Lanes[0].Child.ConfigDir != cfg.ClaudeConfigDirs[1] || st.Lanes[0].Parent.Name != "amesh-a2-parent" || st.Lanes[0].Relay == "" || len(st.Sessions) != 0 || len(st.ConfigDirs) != 2 || st.ConfigDirs[1].Sessions != 1 || st.ConfigDirs[0].Sessions != 1 {
		t.Fatal(st, err)
	}
	if err = parentInbox.SetDeadline(limit); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintln(conn, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"send_to_parent","arguments":{"message":"amesh-a2 local delivery"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	posted, err := parentInbox.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer posted.Close()
	if err = posted.SetDeadline(limit); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(posted).ReadBytes('\n')
	if err != nil || !bytes.Contains(line, []byte("amesh-a2 local delivery")) {
		t.Fatal("missing offline lane message", err)
	}
	if err = posted.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Result struct {
			Structured struct {
				Accepted bool `json:"accepted"`
			} `json:"structuredContent"`
		} `json:"result"`
	}
	if err = json.NewDecoder(reader).Decode(&result); err != nil || !result.Result.Structured.Accepted {
		t.Fatal(result, err)
	}
}
