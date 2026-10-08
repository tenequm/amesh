package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestPaths(t *testing.T) {
	for _, name := range []string{"a", "team/docs/brief.md", "a..b", "**/*.md"} {
		if err := validatePath(name); err != nil {
			t.Errorf("valid path %q: %v", name, err)
		}
	}
	for _, name := range []string{"", ".", "..", "../a", "a/../b", "/a", "a//b", "a/", "a/./b", `C:/a`, `C:\a`, `a\b`, "a\x00b", "a\nb"} {
		if err := validatePath(name); !errors.Is(err, errPath) {
			t.Errorf("unsafe path %q accepted", name)
		}
	}
}

func TestGlobMatch(t *testing.T) {
	for _, test := range []struct {
		pattern string
		name    string
		want    bool
	}{
		{"**", "brief.md", true},
		{"**", "a/b/brief.md", true},
		{"**/*.md", "brief.md", true},
		{"**/*.md", "a/b/brief.md", true},
		{"*.md", "a/brief.md", false},
		{"docs/**/*.md", "docs/brief.md", true},
		{"docs/**/*.md", "docs/a/b/brief.md", true},
		{"docs/**/*.md", "other/brief.md", false},
		{"a/**/b/**/c", "a/b/c", true},
		{"a/**/b/**/c", "a/x/b/y/z/c", true},
		{"a/**/b", "a/b/x", false},
		{"**/x/**/x", "x/x", true},
		{"**/x/**/x", "x", false},
		{"docs/?.[mt][dx]", "docs/a.md", true},
		{"docs/?.[mt][dx]", "docs/ab.md", false},
		{"**/**", "a/b", true},
		{"[!a]*", "brief.md", false},
		{"[^a]*", "brief.md", true},
	} {
		t.Run(test.pattern+"/"+test.name, func(t *testing.T) {
			got := globMatch(strings.Split(test.pattern, "/"), strings.Split(test.name, "/"))
			if got != test.want {
				t.Errorf("match = %v, want %v", got, test.want)
			}
		})
	}
}

func TestLines(t *testing.T) {
	for _, test := range []struct {
		content string
		want    []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{"a\n", []string{"a"}},
		{"\n", []string{""}},
		{"a\n\n", []string{"a", ""}},
		{"a\r\nb", []string{"a\r", "b"}},
	} {
		if got := lines(test.content); !reflect.DeepEqual(got, test.want) {
			t.Errorf("lines(%q) = %q, want %q", test.content, got, test.want)
		}
	}
}

func TestWriteSizeLimit(t *testing.T) {
	store := &storage{}
	_, err := store.write(t.Context(), writeInput{Path: "oversized", Content: strings.Repeat("a", maxObjectBytes+1)})
	if !errors.Is(err, errObjectSize) {
		t.Fatalf("oversized write: %v", err)
	}
}

func TestHTTPGuards(t *testing.T) {
	handler := httpHandler(&storage{}, "unit-test-only")
	for _, test := range []struct {
		method string
		url    string
		auth   string
		origin string
		want   int
	}{
		{"POST", "/mcp", "", "", 401},
		{"GET", "/mcp", "Bearer wrong", "", 401},
		{"DELETE", "/mcp", "", "", 401},
		{"GET", "/missing", "", "", 401},
		{"POST", "/mcp", "Bearer unit-test-only", "https://evil.example", 403},
		{"GET", "/mcp", "Bearer unit-test-only", "null", 403},
		{"GET", "/mcp", "Bearer unit-test-only", "http://example.com", 405},
		{"GET", "/mcp", "Bearer unit-test-only", "", 405},
		{"DELETE", "/mcp", "Bearer unit-test-only", "", 405},
		{"GET", "/missing", "Bearer unit-test-only", "", 404},
	} {
		request := httptest.NewRequestWithContext(t.Context(), test.method, "http://example.com"+test.url, nil)
		request.Header.Set("Authorization", test.auth)
		request.Header.Set("Origin", test.origin)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.want {
			t.Errorf("%s %s: status %d, want %d", test.method, test.url, response.Code, test.want)
		}
	}
}

func TestConfigAndCLI(t *testing.T) {
	t.Setenv("AMESH_TOKEN", "")
	if err := run(t.Context(), []string{"serve"}); !errors.Is(err, errToken) {
		t.Fatalf("tokenless serve: %v", err)
	}
	for _, args := range [][]string{nil, {"unknown"}, {"stdio", "extra"}, {"serve", "extra"}} {
		if err := run(t.Context(), args); !errors.Is(err, errUsage) {
			t.Errorf("args %q: %v", args, err)
		}
	}
	t.Setenv("AMESH_S3_BUCKET", "amesh")
	for _, endpoint := range []string{"", "ftp://localhost", "http://", "http://user:throwaway@localhost", "http://localhost?secret=throwaway"} {
		t.Setenv("AMESH_S3_ENDPOINT", endpoint)
		if _, err := newStorage(t.Context()); !errors.Is(err, errConfig) {
			t.Errorf("bad endpoint accepted")
		}
	}
	t.Setenv("AMESH_S3_ENDPOINT", "http://127.0.0.1:9000")
	for _, prefix := range []string{"/", "../x", "/x", "x//y"} {
		t.Setenv("AMESH_S3_PREFIX", prefix)
		if _, err := newStorage(t.Context()); !errors.Is(err, errPath) {
			t.Errorf("bad prefix %q accepted", prefix)
		}
	}
}

type smokeClient struct {
	t        *testing.T
	endpoint string
	token    string
	modern   bool
	id       atomic.Int64
}

type rpcReply struct {
	ID     int64           `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

type smokeResult struct {
	Content []struct {
		Text string `json:"text"`
	} `json:"content"`
	IsError bool `json:"isError"`
}

func (client *smokeClient) rpc(method string, params map[string]any) json.RawMessage {
	client.t.Helper()
	version := "2025-11-25"
	if client.modern {
		version = "2026-07-28"
		params["_meta"] = map[string]any{
			"io.modelcontextprotocol/protocolVersion":    version,
			"io.modelcontextprotocol/clientCapabilities": map[string]any{},
			"io.modelcontextprotocol/clientInfo":         map[string]string{"name": "amesh-smoke", "version": "1"},
		}
	}
	id := client.id.Add(1)
	envelope := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
	notification := strings.HasPrefix(method, "notifications/")
	if !notification {
		envelope["id"] = id
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		client.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(client.t.Context(), http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		client.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("MCP-Protocol-Version", version)
	request.Header.Set("Mcp-Method", method)
	if name, ok := params["name"].(string); ok {
		request.Header.Set("Mcp-Name", name)
	}
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(request)
	if err != nil {
		client.t.Fatal(err)
	}
	defer response.Body.Close()
	if notification {
		if response.StatusCode != http.StatusAccepted {
			client.t.Fatalf("%s: status %d", method, response.StatusCode)
		}
		return nil
	}
	var reply rpcReply
	if err := json.NewDecoder(response.Body).Decode(&reply); err != nil {
		client.t.Fatalf("%s: status %d, decode error %v", method, response.StatusCode, err)
	}
	if response.StatusCode != http.StatusOK || len(reply.Error) > 0 || reply.ID != id {
		client.t.Fatalf("%s: status %d, RPC error %s, id %d", method, response.StatusCode, reply.Error, reply.ID)
	}
	return reply.Result
}

func (client *smokeClient) call(name string, args map[string]any, wantError bool) string {
	client.t.Helper()
	var result smokeResult
	if err := json.Unmarshal(client.rpc("tools/call", map[string]any{"name": name, "arguments": args}), &result); err != nil {
		client.t.Fatal(err)
	}
	if result.IsError != wantError || len(result.Content) != 1 {
		client.t.Fatalf("%s: unexpected result %+v", name, result)
	}
	return result.Content[0].Text
}

func startProcess(t *testing.T, binary string, args, env []string) (*exec.Cmd, io.WriteCloser, io.ReadCloser) {
	t.Helper()
	command := exec.CommandContext(t.Context(), binary, args...)
	command.Env = env
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = input.Close()
		_ = command.Process.Signal(os.Interrupt)
		if err := command.Wait(); err != nil && t.Context().Err() == nil {
			t.Errorf("process exit: %v; stderr: %s", err, stderr.String())
		}
	})
	return command, input, output
}

func TestSmoke(t *testing.T) {
	binary := os.Getenv("AMESH_SMOKE_BINARY")
	if binary == "" {
		t.Skip("run just smoke for the local MinIO e2e")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	store, err := newStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, headErr := store.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(store.bucket)}); headErr != nil {
		if _, createErr := store.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(store.bucket)}); createErr != nil {
			t.Fatal("create local test bucket failed; start MinIO with just s3-local")
		}
	}
	store.prefix += "run-" + rand.Text() + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		objects, listErr := store.list(cleanupCtx, "")
		if listErr != nil {
			t.Error(listErr)
			return
		}
		for _, object := range objects {
			if _, deleteErr := store.client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(store.prefix + aws.ToString(object.Key))}); deleteErr != nil {
				t.Error("delete smoke object failed")
			}
		}
	})
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "AMESH_TOKEN=") && !strings.HasPrefix(entry, "AMESH_S3_PREFIX=") {
			env = append(env, entry)
		}
	}
	env = append(env, "AMESH_S3_PREFIX="+strings.TrimSuffix(store.prefix, "/"))
	token := rand.Text()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bind := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, _ = startProcess(t, binary, []string{"serve", "--bind", bind}, append(slices.Clone(env), "AMESH_TOKEN="+token))
	endpoint := "http://" + bind + "/mcp"
	ready := false
	for range 100 {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := (&http.Client{Timeout: time.Second}).Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthenticated status: %d", response.StatusCode)
			}
			ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatal("serve did not start")
	}
	t.Log("HTTP without bearer token: 401")
	for _, modern := range []bool{false, true} {
		era := "legacy"
		if modern {
			era = "modern"
		}
		t.Run(era, func(t *testing.T) {
			client := &smokeClient{t: t, endpoint: endpoint, token: token, modern: modern}
			if modern {
				discover := string(client.rpc("server/discover", map[string]any{}))
				if !strings.Contains(discover, "2026-07-28") {
					t.Fatal("modern discovery missing version")
				}
				t.Log("server/discover: 2026-07-28 supported")
			} else {
				init := string(client.rpc("initialize", map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "amesh-smoke", "version": "1"}}))
				if !strings.Contains(init, "amesh") {
					t.Fatal("initialize missing server identity")
				}
				t.Log("initialize: amesh 0.1.0, protocol 2025-11-25")
				client.rpc("notifications/initialized", map[string]any{})
			}
			var listed struct {
				Tools []struct {
					Name        string         `json:"name"`
					Description string         `json:"description"`
					InputSchema map[string]any `json:"inputSchema"`
				} `json:"tools"`
			}
			if err := json.Unmarshal(client.rpc("tools/list", map[string]any{}), &listed); err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, tool := range listed.Tools {
				names = append(names, tool.Name)
				if !strings.Contains(tool.Description, "SHARED") || tool.InputSchema["type"] != "object" || tool.InputSchema["additionalProperties"] != false {
					t.Fatalf("invalid published tool %s", tool.Name)
				}
			}
			wantNames := []string{"shared_edit", "shared_glob", "shared_grep", "shared_read", "shared_write"}
			if !slices.Equal(names, wantNames) {
				t.Fatalf("tools/list: %v", names)
			}
			t.Logf("tools/list: %s", strings.Join(names, ", "))
			name := era + "/docs/brief.md"
			text := client.call("shared_write", map[string]any{"path": name, "content": "Alpha\nbeta beta\nGamma\n"}, false)
			t.Log(text)
			if got := client.call("shared_read", map[string]any{"path": name, "offset": 2, "limit": 1}, false); got != "     2\tbeta beta\n" {
				t.Fatalf("read window: %q", got)
			}
			t.Log("shared_read(offset=2, limit=1): 2 beta beta")
			if got := client.call("shared_edit", map[string]any{"path": name, "old_string": "beta", "new_string": "delta"}, true); !strings.Contains(got, "not unique") {
				t.Fatal(got)
			}
			t.Log("shared_edit non-unique: isError=true, old_string is not unique")
			t.Log(client.call("shared_edit", map[string]any{"path": name, "old_string": "beta", "new_string": "delta", "replace_all": true}, false))
			for _, pattern := range []string{era + "/**", era + "/**/*.md", era + "/docs/*.md"} {
				if got := client.call("shared_glob", map[string]any{"pattern": pattern}, false); got != name {
					t.Fatalf("glob %s: %q", pattern, got)
				}
			}
			t.Log("shared_glob: " + name)
			if got := client.call("shared_grep", map[string]any{"pattern": "ALPHA|delta", "path": era, "case_insensitive": true}, false); got != name+":1:Alpha\n"+name+":2:delta delta\n" {
				t.Fatalf("grep: %q", got)
			}
			t.Log("shared_grep: " + name + ":1:Alpha; " + name + ":2:delta delta")
			for tool, args := range map[string]map[string]any{
				"shared_read":  {"path": "../escape"},
				"shared_write": {"path": "docs/../escape", "content": "bad"},
				"shared_edit":  {"path": "../escape", "old_string": "x", "new_string": "y"},
				"shared_glob":  {"pattern": "../**"},
				"shared_grep":  {"pattern": ".", "path": "../escape"},
			} {
				if got := client.call(tool, args, true); !strings.Contains(got, "relative") {
					t.Fatalf("%s traversal: %q", tool, got)
				}
			}
			t.Log("all five tools reject '..' paths with isError=true")
			if got := client.call("shared_read", map[string]any{"path": name, "offset": 0}, true); !strings.Contains(got, "positive") {
				t.Fatal(got)
			}
			client.call("shared_read", map[string]any{"path": name, "limit": -1}, true)
			client.call("shared_write", map[string]any{"path": name}, true)
			client.call("shared_glob", map[string]any{"pattern": "["}, true)
			client.call("shared_grep", map[string]any{"pattern": "["}, true)
			client.call("shared_edit", map[string]any{"path": name, "old_string": "missing", "new_string": "x"}, true)
			client.call("shared_edit", map[string]any{"path": name, "old_string": "", "new_string": "x"}, true)
			client.call("shared_read", map[string]any{"path": era + "/missing"}, true)
			client.call("shared_write", map[string]any{"path": era + "/empty", "content": ""}, false)
			if got := client.call("shared_read", map[string]any{"path": era + "/empty"}, false); got != "" {
				t.Fatal("empty file got phantom line")
			}
			if got := client.call("shared_read", map[string]any{"path": name, "offset": 100}, false); got != "" {
				t.Fatal("read beyond EOF not empty")
			}
			client.call("shared_write", map[string]any{"path": era + "/overlap", "content": "aaa"}, false)
			client.call("shared_edit", map[string]any{"path": era + "/overlap", "old_string": "aa", "new_string": "b"}, true)
			client.call("shared_write", map[string]any{"path": era + "-outside/file", "content": "delta"}, false)
			if got := client.call("shared_grep", map[string]any{"pattern": "delta", "path": era + "/docs/"}, false); got != name+":2:delta delta\n" {
				t.Fatalf("prefix boundary: %q", got)
			}
			t.Log("empty file, EOF window, invalid regex/glob/window, absent/empty/overlapping edit, directory prefix: PASS")
			target := era + "/concurrent"
			client.call("shared_write", map[string]any{"path": target, "content": "BASE"}, false)
			var group sync.WaitGroup
			for i := range 12 {
				group.Go(func() {
					client.call("shared_edit", map[string]any{"path": target, "old_string": "BASE", "new_string": fmt.Sprintf("BASE\nentry-%02d", i)}, false)
				})
			}
			group.Wait()
			content, err := store.get(ctx, target)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 12 {
				if strings.Count(content, fmt.Sprintf("entry-%02d", i)) != 1 {
					t.Fatal("concurrent edit lost an update")
				}
			}
			t.Log("12 concurrent shared_edit calls: no lost updates")
			client.call("shared_write", map[string]any{"path": era + "/expansion", "content": strings.Repeat("a", 1<<16)}, false)
			if got := client.call("shared_edit", map[string]any{"path": era + "/expansion", "old_string": "a", "new_string": strings.Repeat("b", 1<<16), "replace_all": true}, true); !strings.Contains(got, "16 MiB") {
				t.Fatalf("edit expansion guard: %q", got)
			}
			if got, err := store.get(ctx, era+"/expansion"); err != nil || got != strings.Repeat("a", 1<<16) {
				t.Fatal("rejected expansion changed the file")
			}
			t.Log("4 GiB projected edit: rejected before allocation; original unchanged")
			client.call("shared_write", map[string]any{"path": era + "/order/a", "content": "a"}, false)
			time.Sleep(1100 * time.Millisecond)
			client.call("shared_write", map[string]any{"path": era + "/order/b", "content": "b"}, false)
			if got := client.call("shared_glob", map[string]any{"pattern": era + "/order/*"}, false); got != era+"/order/b\n"+era+"/order/a" {
				t.Fatalf("newest-first glob: %q", got)
			}
			t.Log("shared_glob last-modified descending order: PASS")
		})
	}
	t.Run("pagination_and_prefix", func(t *testing.T) {
		client := &smokeClient{t: t, endpoint: endpoint, token: token, modern: true}
		var group sync.WaitGroup
		concurrency := make(chan struct{}, 8)
		for i := range 1001 {
			concurrency <- struct{}{}
			group.Go(func() {
				defer func() { <-concurrency }()
				if err := store.put(ctx, fmt.Sprintf("pages/%04d", i), "x"); err != nil {
					t.Error(err)
				}
			})
		}
		group.Wait()
		if got := client.call("shared_glob", map[string]any{"pattern": "pages/*"}, false); len(strings.Split(got, "\n")) != 1001 {
			t.Fatal("glob omitted objects after first S3 page")
		}
		outsideKey := strings.TrimSuffix(store.prefix, "/") + "-outside/escaped-prefix-sentinel"
		if _, err := store.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(outsideKey), Body: strings.NewReader("OUTSIDE_SENTINEL")}); err != nil {
			t.Fatal("seed prefix boundary fixture failed")
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if _, err := store.client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(outsideKey)}); err != nil {
				t.Error("delete prefix boundary fixture failed")
			}
		})
		if got := client.call("shared_grep", map[string]any{"pattern": "OUTSIDE_SENTINEL"}, false); got != "" {
			t.Fatal("grep escaped configured bucket prefix")
		}
		if got := client.call("shared_glob", map[string]any{"pattern": "**"}, false); strings.Contains(got, "escaped-prefix-sentinel") {
			t.Fatal("glob escaped configured bucket prefix")
		}
		t.Log("1001 matching objects across S3 pages and configured prefix isolation: PASS")
	})
	t.Run("stdio", func(t *testing.T) {
		_, input, output := startProcess(t, binary, []string{"stdio"}, env)
		scanner := bufio.NewScanner(output)
		for i, message := range []string{
			`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}`,
			`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
		} {
			if _, err := io.WriteString(input, message+"\n"); err != nil {
				t.Fatal(err)
			}
			if !scanner.Scan() {
				t.Fatal("stdio returned no JSON-RPC response")
			}
			var reply rpcReply
			if err := json.Unmarshal(scanner.Bytes(), &reply); err != nil {
				t.Fatalf("stdio non-protocol stdout: %v", err)
			}
			if reply.ID != int64(i+1) || len(reply.Error) > 0 {
				t.Fatalf("stdio error: %s", scanner.Text())
			}
			if i == 1 && strings.Count(string(reply.Result), `"name":"shared_`) != 5 {
				t.Fatal("stdio missing tools")
			}
			if i == 0 {
				if _, err := io.WriteString(input, `{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`+"\n"); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Log("piped JSON-RPC initialize + tools/list: amesh, all five tools")
	})
}
