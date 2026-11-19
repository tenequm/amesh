package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tenequm/amesh/internal/registry"
	"go.yaml.in/yaml/v3"
)

type testMesh struct{ MeshBackend }

func (testMesh) ListPeers(_ context.Context, s registry.Session, filter string) (Directory, error) {
	return Directory{You: Identity{Name: s.Name, Project: filter, Machine: "mac"}}, nil
}

func (testMesh) SendMessage(_ context.Context, s registry.Session, args SendArgs) (SendResult, error) {
	return SendResult{Reason: s.Name + " refused " + args.To, Known: &[]string{}}, nil
}

type testLane struct{ LaneBackend }

func (testLane) SendToParent(_ context.Context, s registry.Session, parent, message string) (ParentResult, error) {
	return ParentResult{Reason: s.Name + " refused " + parent + " " + message}, nil
}

type rpc struct {
	t      *testing.T
	conn   net.Conn
	reader *bufio.Reader
}

func newRPC(t *testing.T, parent string, reconnect bool) *rpc {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan error, 1)
	var calls atomic.Int32
	caller := func(context.Context) (registry.Session, error) {
		return registry.Session{Name: fmt.Sprintf("caller%d", calls.Add(1))}, nil
	}
	go func() { done <- Serve(t.Context(), server, caller, parent, reconnect, testLane{}, testMesh{}) }()
	t.Cleanup(func() {
		client.Close()
		if e := <-done; e != nil {
			t.Error(e)
		}
	})
	return &rpc{t, client, bufio.NewReader(client)}
}

func (c *rpc) request(method, params string) map[string]any {
	c.t.Helper()
	_, err := fmt.Fprintf(c.conn, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":%q,\"params\":%s}\n", method, params)
	if err != nil {
		c.t.Fatal(err)
	}
	for {
		b, e := c.reader.ReadBytes('\n')
		if e != nil {
			c.t.Fatal(e)
		}
		var reply struct {
			ID     int             `json:"id"`
			Result map[string]any  `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if e = json.Unmarshal(b, &reply); e != nil {
			c.t.Fatal(e)
		}
		if reply.Error != nil {
			c.t.Fatalf("%s: %s", method, reply.Error)
		}
		if reply.ID == 1 {
			return reply.Result
		}
	}
}

func (c *rpc) call(name, args string) map[string]any {
	c.t.Helper()
	return c.request("tools/call", fmt.Sprintf(`{"name":%q,"arguments":%s}`, name, args))
}

func payload(t *testing.T, result map[string]any) map[string]any {
	t.Helper()
	text := result["content"].([]any)
	if len(text) != 1 || result["isError"] != false {
		t.Fatal(result)
	}
	item := text[0].(map[string]any)
	var decoded map[string]any
	if json.Unmarshal([]byte(item["text"].(string)), &decoded) != nil || item["type"] != "text" || !reflect.DeepEqual(decoded, result["structuredContent"]) {
		t.Fatal(result)
	}
	return decoded
}

func TestPublishedContract(t *testing.T) {
	data, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Info struct {
			Version string `yaml:"version"`
		} `yaml:"info"`
		Components struct {
			Schemas map[string]map[string]any `yaml:"schemas"`
		} `yaml:"components"`
		MCP struct {
			Servers map[string]struct {
				Info  map[string]any `yaml:"serverInfo"`
				Modes map[string]struct {
					Instructions string `yaml:"instructions"`
					Tools        []struct {
						Name        string            `yaml:"name"`
						Description string            `yaml:"description"`
						Input       map[string]string `yaml:"inputSchema"`
						Annotations map[string]any    `yaml:"annotations"`
					} `yaml:"tools"`
				} `yaml:"modes"`
			} `yaml:"servers"`
		} `yaml:"x-amesh-mcp"`
	}
	if err = yaml.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	def := spec.MCP.Servers["amesh"]
	if len(def.Modes) != 2 {
		t.Fatal("missing modes")
	}
	for name, mode := range def.Modes {
		t.Run(name, func(t *testing.T) {
			parent := ""
			if name == "lane" {
				parent = "parent"
			}
			client := newRPC(t, parent, false)
			init := client.request("initialize", `{"protocolVersion":"2025-11-25","clientInfo":{"name":"proof","version":"1"},"capabilities":{}}`)
			if !reflect.DeepEqual(init["serverInfo"], def.Info) || init["instructions"] != mode.Instructions {
				t.Fatal(init)
			}
			_, err := fmt.Fprintln(client.conn, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
			if err != nil {
				t.Fatal(err)
			}
			list := client.request("tools/list", "{}")["tools"].([]any)
			if len(list) != len(mode.Tools) {
				t.Fatal(list)
			}
			for i, tool := range mode.Tools {
				got := list[i].(map[string]any)
				schema := spec.Components.Schemas[strings.TrimPrefix(tool.Input["$ref"], "#/components/schemas/")]
				// The hub lane owns the snapshot refresh. A1 removes just this argument.
				if spec.Info.Version == "1.0.0-draft.2026-11-19" && tool.Name == "send_message" {
					delete(schema["properties"].(map[string]any), "notify_when_idle")
				}
				want, _ := json.Marshal(schema)
				actual, _ := json.Marshal(got["inputSchema"])
				if got["name"] != tool.Name || got["description"] != tool.Description || string(want) != string(actual) {
					t.Fatalf("%s: %s want %s", tool.Name, actual, want)
				}
				if len(tool.Annotations) > 0 && !reflect.DeepEqual(got["annotations"], tool.Annotations) {
					t.Fatal(got)
				}
			}
			if parent != "" {
				got := payload(t, client.call("send_to_parent", `{"message":"hi"}`))
				if got["accepted"] != false || got["reason"] != "caller1 refused parent hi" {
					t.Fatal(got)
				}
			} else {
				got := payload(t, client.call("list_peers", `{"filter":"@pond"}`))["you"].(map[string]any)
				if got["name"] != "caller1" || got["project"] != "@pond" {
					t.Fatal(got)
				}
				got = payload(t, client.call("send_message", `{"to":"peer","message":"hi"}`))
				if got["reason"] != "caller2 refused peer" {
					t.Fatal(got)
				}
				removed := client.call("send_message", `{"to":"peer","message":"hi","notify_when_idle":true}`)
				if removed["isError"] != true {
					t.Fatal(removed)
				}
				bad := client.call("send_message", `{"to":"peer","unknown":1}`)
				if bad["isError"] != true {
					t.Fatal(bad)
				}
			}
		})
	}
}

func TestResumedSessionAcceptsFirstToolCall(t *testing.T) {
	c := newRPC(t, "parent", true)
	got := payload(t, c.call("send_to_parent", `{"message":"after restart"}`))
	if got["reason"] != "caller1 refused parent after restart" {
		t.Fatal(got)
	}
}
