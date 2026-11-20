package codex

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestThreadDeliverySelectsHostAndBindsMetadata(t *testing.T) {
	for _, tc := range []struct {
		name         string
		hosted, held bool
	}{{"hosted", true, true}, {"embedded", false, true}, {"unlocked", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			const thread = "thread"
			const id = "message"
			dir, err := os.MkdirTemp("/tmp", "amesh-app-") //nolint:usetesting // The Unix socket path must fit on Darwin.
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) })
			sock := filepath.Join(dir, "app.sock")
			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", sock)
			if err != nil {
				t.Fatal(err)
			}
			sent := make(chan string, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, e := websocket.Accept(w, r, nil)
				if e != nil {
					t.Error(e)
					return
				}
				defer ws.CloseNow()
				for {
					var req struct {
						ID     int
						Method string
						Params json.RawMessage
					}
					if wsjson.Read(r.Context(), ws, &req) != nil {
						return
					}
					var result any = map[string]any{}
					switch req.Method {
					case "initialize":
						var params struct {
							Capabilities struct {
								ExperimentalAPI bool `json:"experimentalApi"`
							} `json:"capabilities"`
						}
						if json.Unmarshal(req.Params, &params) != nil || !params.Capabilities.ExperimentalAPI {
							t.Error("experimental API disabled")
						}
					case "initialized":
						continue
					case "thread/loaded/list":
						var p struct {
							Cursor string `json:"cursor"`
						}
						_ = json.Unmarshal(req.Params, &p)
						ids := []string{"other"}
						if p.Cursor == "" {
							result = map[string]any{"data": ids, "nextCursor": "next"}
						} else {
							if tc.hosted {
								ids = []string{thread}
							}
							result = map[string]any{"data": ids}
						}
						if e = wsjson.Write(r.Context(), ws, map[string]any{"method": "other/notification", "params": map[string]string{"status": "ok"}}); e != nil {
							return
						}
					case "turn/start", "thread/queue/add":
						var p struct {
							ThreadID                   string            `json:"threadId"`
							ClientUserMessageID        string            `json:"clientUserMessageId"`
							TurnTrigger                string            `json:"turnTrigger"`
							ResponsesapiClientMetadata map[string]string `json:"responsesapiClientMetadata"`
							Input                      []struct {
								Text string `json:"text"`
							} `json:"input"`
						}
						if json.Unmarshal(req.Params, &p) != nil || p.ThreadID != thread || p.ClientUserMessageID != id || p.ResponsesapiClientMetadata["amesh_msg"] != id || len(p.Input) != 1 || p.Input[0].Text != "body" || (p.TurnTrigger == "amesh") != tc.hosted {
							t.Error("incorrect thread delivery parameters")
						}
						sent <- req.Method
					default:
						t.Error(req.Method)
						return
					}
					if wsjson.Write(r.Context(), ws, map[string]any{"id": req.ID, "result": result}) != nil {
						return
					}
				}
			}))
			_ = server.Listener.Close()
			server.Listener = ln
			server.Start()
			t.Cleanup(server.Close)
			app := &App{socket: sock}
			queued, err := app.Deliver(t.Context(), thread, id, "body", func() bool { return tc.held })
			if !tc.held {
				if !errors.Is(err, os.ErrNotExist) || len(sent) != 0 {
					t.Fatal("sent after release", err)
				}
				return
			}
			if err != nil || queued == tc.hosted {
				t.Fatal(queued, err)
			}
			select {
			case method := <-sent:
				if (method == "turn/start") != tc.hosted {
					t.Fatal(method)
				}
			default:
				t.Fatal("no delivery")
			}
		})
	}
}

func TestQueueFallbackUsesThreadHome(t *testing.T) {
	daemonHome, threadHome := t.TempDir(), t.TempDir()
	t.Setenv("CODEX_HOME", daemonHome)
	bin := t.TempDir()
	script := `#!/bin/sh
set -eu
read -r init
echo '{"id":1,"result":{}}'
read -r initialized
read -r request
echo "$request" > "$CODEX_HOME/request"
echo '{"id":2,"result":{}}'
`
	if err := os.WriteFile(filepath.Join(bin, "codex"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	queued, err := New(threadHome).Deliver(t.Context(), "thread", "id", "hello", func() bool { return true })
	if err != nil || !queued {
		t.Fatal(queued, err)
	}
	raw, err := os.ReadFile(filepath.Join(threadHome, "request"))
	if err != nil {
		t.Fatal(err)
	}
	var req struct {
		Method string `json:"method"`
		Params struct {
			ThreadID string `json:"threadId"`
		} `json:"params"`
	}
	if err = json.Unmarshal(raw, &req); err != nil || req.Method != "thread/queue/add" || req.Params.ThreadID != "thread" {
		t.Fatal(req, err)
	}
}
