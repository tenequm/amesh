package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/tenequm/amesh/internal/version"
)

// TurnTrigger marks the turns amesh starts, and MessageKey carries the message ID back on their tool calls.
const (
	TurnTrigger = "amesh"
	MessageKey  = "amesh_msg"
)

var ErrUnconfirmed = errors.New("app-server delivery was not confirmed")

type rpcError struct {
	Code int `json:"code"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("app-server request failed (%d)", e.Code) }

type App struct {
	socket string
	mu     sync.RWMutex
	status map[string]string
}

func New(home string) *App {
	return &App{socket: filepath.Join(home, "app-server-control", "app-server-control.sock"), status: map[string]string{}}
}

type rpc struct {
	send  func(context.Context, any) error
	read  func(context.Context, *packet) error
	close func()
	id    int
}

type packet struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func (r *rpc) call(ctx context.Context, method string, params, out any) error {
	r.id++
	if err := r.send(ctx, map[string]any{"id": r.id, "method": method, "params": params}); err != nil {
		return err
	}
	for {
		var p packet
		if err := r.read(ctx, &p); err != nil {
			return err
		}
		if p.Method != "" || string(p.ID) != strconv.Itoa(r.id) {
			continue
		}
		if p.Error != nil {
			return fmt.Errorf("%s: %w", method, p.Error)
		}
		if out != nil {
			return json.Unmarshal(p.Result, out)
		}
		return nil
	}
}

func (a *App) dial(ctx context.Context) (*rpc, error) {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		path, err := filepath.EvalSymlinks(a.socket)
		if err != nil {
			return nil, err
		}
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	//nolint:bodyclose // websocket.Dial owns the upgrade response body.
	conn, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: tr}})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(32 << 20)
	return &rpc{
		send:  func(ctx context.Context, v any) error { return wsjson.Write(ctx, conn, v) },
		read:  func(ctx context.Context, p *packet) error { return wsjson.Read(ctx, conn, p) },
		close: func() { _ = conn.CloseNow(); tr.CloseIdleConnections() },
	}, nil
}

func stdio(ctx context.Context) (*rpc, error) {
	cmd := exec.CommandContext(ctx, "codex", "app-server", "--listen", "stdio://")
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		_ = in.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		_ = in.Close()
		_ = out.Close()
		return nil, err
	}
	enc, dec := json.NewEncoder(in), json.NewDecoder(out)
	return &rpc{
		send: func(_ context.Context, v any) error { return enc.Encode(v) },
		read: func(_ context.Context, p *packet) error { return dec.Decode(p) },
		close: func() {
			_ = in.Close()
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		},
	}, nil
}

func (r *rpc) init(ctx context.Context) error {
	err := r.call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "amesh", "version": version.Value}, "capabilities": map[string]bool{"experimentalApi": true}}, nil)
	if err != nil {
		return err
	}
	return r.send(ctx, map[string]string{"method": "initialized"})
}

func (r *rpc) hosts(ctx context.Context, thread string) (bool, error) {
	params := map[string]string{}
	for {
		var page struct {
			Data       []string `json:"data"`
			NextCursor string   `json:"nextCursor"`
		}
		if err := r.call(ctx, "thread/loaded/list", params, &page); err != nil {
			return false, err
		}
		if slices.Contains(page.Data, thread) {
			return true, nil
		}
		if page.NextCursor == "" {
			return false, nil
		}
		params["cursor"] = page.NextCursor
	}
}

func (a *App) Deliver(ctx context.Context, thread, id, content string, live func() bool) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	r, err := a.dial(ctx)
	daemon := err == nil
	if err != nil {
		if _, statErr := os.Stat(a.socket); !errors.Is(statErr, os.ErrNotExist) && !errors.Is(err, syscall.ECONNREFUSED) {
			return false, err
		}
		r, err = stdio(ctx)
		if err != nil {
			return false, err
		}
	}
	defer r.close()
	if err = r.init(ctx); err != nil {
		return false, err
	}
	hosted := false
	if daemon {
		hosted, err = r.hosts(ctx, thread)
		if err != nil {
			return false, err
		}
	}
	if !live() {
		return false, os.ErrNotExist
	}
	params := map[string]any{"threadId": thread, "input": []map[string]string{{"type": "text", "text": content}}, "clientUserMessageId": id, "responsesapiClientMetadata": map[string]string{MessageKey: id}}
	method := "thread/queue/add"
	if hosted {
		method = "turn/start"
		params["turnTrigger"] = TurnTrigger
	}
	err = r.call(ctx, method, params, nil)
	if err != nil {
		if _, rejected := errors.AsType[*rpcError](err); !rejected {
			err = errors.Join(ErrUnconfirmed, err)
		}
	}
	// Only a hosting app-server starts or steers the turn now; the durable queue waits for the next idle turn.
	return !hosted, err
}

func (a *App) Status(thread string) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.status[thread]
}

func (a *App) monitor(ctx context.Context) error {
	r, err := a.dial(ctx)
	if err != nil {
		return err
	}
	defer r.close()
	if err = r.init(ctx); err != nil {
		return err
	}
	for {
		var p packet
		if err = r.read(ctx, &p); err != nil {
			return err
		}
		if p.Method != "thread/status/changed" && p.Method != "thread/closed" {
			continue
		}
		var params struct {
			ThreadID string `json:"threadId"`
			Status   struct {
				Type string `json:"type"`
			} `json:"status"`
		}
		if json.Unmarshal(p.Params, &params) != nil || params.ThreadID == "" {
			continue
		}
		a.mu.Lock()
		switch params.Status.Type {
		case "active":
			a.status[params.ThreadID] = "busy"
		case "idle":
			a.status[params.ThreadID] = "idle"
		default:
			delete(a.status, params.ThreadID)
		}
		a.mu.Unlock()
	}
}

func (a *App) Watch(ctx context.Context) {
	for ctx.Err() == nil {
		_ = a.monitor(ctx)
		a.mu.Lock()
		clear(a.status)
		a.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
