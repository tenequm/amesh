package hub

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
)

type Client struct {
	cfg      config.Config
	http     *http.Client
	key      func() (identity, error)
	mu       sync.Mutex
	helloMu  sync.RWMutex
	started  bool
	hello    contract.HelloRequest
	current  contract.HelloResponse
	offset   time.Duration
	halt     error
	stopped  chan struct{}
	queueMu  sync.Mutex
	draining chan struct{}
	wake     chan struct{}
	waiters  map[string][]chan SendOutcome
	progress
	streamRunning bool
	usedHello     uint64
	logChanged    chan struct{}
	instance      string
	helloCount    uint64
	verifying     bool
	presenceMu    sync.Mutex
	bindingMu     sync.Mutex
}

func New(cfg config.Config) *Client { return NewWithTransport(cfg, nil) }

func NewWithTransport(cfg config.Config, transport http.RoundTripper) *Client {
	var n [16]byte
	rand.Read(n[:])
	return &Client{
		instance:   encoding.EncodeToString(n[:]),
		logChanged: make(chan struct{}),
		cfg:        cfg,
		http:       &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		key:        sync.OnceValues(func() (identity, error) { return loadKey(filepath.Join(cfg.StateDir, "machine.key")) }),
		current:    contract.HelloResponse{Settings: contract.DefaultSettings(), Limits: contract.Limits{MaxTextBytes: 32768, QueueTTLMs: 86400000}},
		stopped:    make(chan struct{}),
		draining:   make(chan struct{}, 1),
		wake:       make(chan struct{}, 1),
		waiters:    make(map[string][]chan SendOutcome),
	}
}

func (c *Client) Connection() (contract.HelloResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, c.halt
}

type HTTPError struct {
	Status     int
	Body       contract.ErrorBody
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("peer HTTP %d (%s)", e.Status, e.Body.Error)
}

func (e *HTTPError) transient() bool {
	return e.Body.Error == "" || e.Status >= 500 || e.Status == 429 || e.Status == 408 || (e.Status == 409 && e.Body.Error == "machine_in_use") || (e.Status == 401 && e.Body.Error == "stale_timestamp")
}

func (c *Client) stop(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.halt == nil {
		c.halt = err
		close(c.stopped)
	}
	return c.halt
}

func (c *Client) open(ctx context.Context, method, route string, body []byte, media string) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		if _, err := c.Connection(); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, c.cfg.HubURL+route, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(req.URL.Path, "/peer/") || req.URL.Path != path.Clean(req.URL.Path) || req.URL.RawPath != "" || req.URL.User != nil {
			return nil, errors.New("invalid peer API target")
		}
		req.Header.Set("Accept", media)
		req.Header.Set("Accept-Encoding", "identity")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if method != http.MethodGet || route != "/hello" {
			id, keyErr := c.key()
			if keyErr != nil {
				return nil, c.stop(keyErr)
			}
			c.mu.Lock()
			ts := time.Now().Add(c.offset).Unix()
			c.mu.Unlock()
			auth, signErr := id.sign(method, req.URL.RequestURI(), ts, body)
			if signErr != nil {
				return nil, signErr
			}
			req.Header.Set("Authorization", auth)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		e := &HTTPError{Status: resp.StatusCode}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<18)).Decode(&e.Body)
		_ = resp.Body.Close()
		if seconds, parseErr := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 32); parseErr == nil && seconds > 0 {
			e.RetryAfter = time.Duration(seconds) * time.Second
		}
		switch {
		case e.Body.Error == "" && (route == "/hello" || strings.HasPrefix(route, "/events?")):
			return nil, e
		case e.Status < 400:
			return nil, c.stop(e)
		case e.Status == 401 && (e.Body.Error == "bad_signature" || e.Body.Error == "replayed"):
			if attempt == 0 {
				continue
			}
			return nil, c.stop(e)
		case e.Status == 401 && e.Body.Error == "stale_timestamp":
			if attempt == 0 && e.Body.ServerTime != nil {
				c.mu.Lock()
				c.offset = time.Until(time.UnixMilli(*e.Body.ServerTime))
				c.mu.Unlock()
				continue
			}
		case e.Status == 401 && e.Body.Error == "unknown_machine", e.Status == 403 && e.Body.Error == "machine_revoked", e.Status == 426:
			return nil, c.stop(e)
		}
		return nil, e
	}
}

func rpc[T any](ctx context.Context, c *Client, method, route string, request any) (T, error) {
	var result T
	var body []byte
	var err error
	if request != nil {
		body, err = json.Marshal(request)
		if err != nil {
			return result, err
		}
	}
	conn, _ := c.Connection()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(conn.Settings.RequestTimeoutMs)*time.Millisecond)
	defer cancel()
	resp, err := c.open(ctx, method, route, body, "application/json")
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err == nil {
		err = json.Unmarshal(raw, &result)
	}
	return result, err
}

func (c *Client) Info(ctx context.Context) (contract.HelloInfo, error) {
	return rpc[contract.HelloInfo](ctx, c, http.MethodGet, "/hello", nil)
}

func (c *Client) Hello(ctx context.Context, req contract.HelloRequest) (contract.HelloResponse, error) {
	c.helloMu.Lock()
	defer c.helloMu.Unlock()
	return c.helloLocked(ctx, req)
}

func hasCode(err error, status int, code string) bool {
	h, ok := errors.AsType[*HTTPError](err)
	return ok && h.Status == status && h.Body.Error == code
}

func (c *Client) confirmOwner(ctx context.Context, revision uint64) error {
	c.helloMu.Lock()
	defer c.helloMu.Unlock()
	if c.helloCount != revision {
		return nil
	}
	c.verifying = true
	_, err := c.helloLocked(ctx, c.hello)
	return err
}

var logID = regexp.MustCompile(`^lg_[A-Za-z0-9_-]{8,64}$`)

func (c *Client) helloLocked(ctx context.Context, req contract.HelloRequest) (contract.HelloResponse, error) {
	if err := c.loadCursor(); err != nil {
		return contract.HelloResponse{}, c.stop(err)
	}
	c.mu.Lock()
	req.Instance = c.instance
	req.Takeover = !c.started
	req.ResumeEpoch = nil
	if c.current.Epoch > 0 {
		epoch := c.current.Epoch
		req.ResumeEpoch = &epoch
	}
	c.started = true
	c.hello = req
	c.mu.Unlock()
	// Decode settings independently: even a wrong JSON type falls back as one object.
	type response struct {
		contract.HelloResponse
		Settings json.RawMessage `json:"settings"`
	}
	wire, err := rpc[response](ctx, c, http.MethodPost, "/hello", req)
	if err != nil {
		if c.verifying && hasCode(err, 409, "machine_in_use") {
			return contract.HelloResponse{}, c.stop(&HTTPError{Status: 409, Body: contract.ErrorBody{Error: "superseded", Message: "Another daemon owns this machine."}})
		}
		return contract.HelloResponse{}, err
	}
	v := wire.HelloResponse
	if v.Limits.MaxTextBytes <= 0 || v.Limits.QueueTTLMs <= 0 || v.Limits.QueueTTLMs > int64((1<<63-1)/time.Millisecond) {
		return v, errors.New("invalid hub send limits")
	}
	if v.Epoch < 1 {
		return v, errors.New("hub hello returned an invalid epoch")
	}
	var required struct {
		SendWaitMs *int64 `json:"sendWaitMs"`
	}
	if json.Unmarshal(wire.Settings, &required) != nil || required.SendWaitMs == nil || json.Unmarshal(wire.Settings, &v.Settings) != nil || !v.Settings.Valid() {
		v.Settings = contract.DefaultSettings()
		slog.Warn("invalid peer timing settings, using defaults")
	}
	if !logID.MatchString(v.Log) {
		return v, errors.New("invalid hub event log id")
	}
	c.bindingMu.Lock()
	defer c.bindingMu.Unlock()
	if err = c.bindOutbox(v.Log); err != nil {
		return v, err
	}
	if err = c.replaceLog(v.Log); err != nil {
		return v, err
	}
	c.helloCount++
	c.verifying = false
	c.mu.Lock()
	c.current = v
	c.mu.Unlock()
	c.signalOutbox()
	return v, nil
}

var sessionKey = regexp.MustCompile("^[1-9][0-9]{0,9}-[0-9a-f]{8}$")

func sessionRPC[T any](ctx context.Context, c *Client, method, key, suffix string, req any) (T, error) {
	if !sessionKey.MatchString(key) {
		var zero T
		return zero, errors.New("invalid peer session key")
	}
	return rpc[T](ctx, c, method, "/sessions/"+key+suffix, req)
}

func (c *Client) Peers(ctx context.Context) (contract.PeersResponse, error) {
	return c.PeersFiltered(ctx, "", "")
}

var projectLabel = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,39}$`)

func (c *Client) PeersFiltered(ctx context.Context, project, machine string) (contract.PeersResponse, error) {
	route := "/peers"
	if project != "" {
		if !projectLabel.MatchString(project) {
			return contract.PeersResponse{}, errors.New("invalid peer project filter")
		}
		route += "?project=" + project
	}
	if machine != "" {
		if project == "" || !machineName.MatchString(machine) {
			return contract.PeersResponse{}, errors.New("invalid peer machine filter")
		}
		route += "&machine=" + machine
	}
	return rpc[contract.PeersResponse](ctx, c, http.MethodGet, route, nil)
}

func retryWrite[T any](ctx context.Context, c *Client, retry bool, send func(epoch int64) (T, error)) (T, error) {
	var backoff time.Duration
	for {
		c.helloMu.RLock()
		conn, halt := c.Connection()
		if halt != nil {
			c.helloMu.RUnlock()
			var zero T
			return zero, halt
		}
		rev := c.helloCount
		value, err := send(conn.Epoch)
		c.helloMu.RUnlock()
		if err == nil {
			return value, nil
		}
		if hasCode(err, 409, "superseded") {
			if err = c.confirmOwner(ctx, rev); err == nil {
				continue
			}
		}
		if !retry {
			return value, err
		}
		if h, ok := errors.AsType[*HTTPError](err); ok && !h.transient() {
			return value, err
		}
		if _, halt = c.Connection(); halt != nil {
			return value, halt
		}
		if backoff == 0 {
			backoff = time.Duration(conn.Settings.BackoffInitialMs) * time.Millisecond
		}
		if waitErr := c.pause(ctx, retryDelay(err, backoff)); waitErr != nil {
			return value, waitErr
		}
		backoff = min(2*backoff, time.Duration(conn.Settings.BackoffMaxMs)*time.Millisecond)
	}
}

func (c *Client) Register(ctx context.Context, key string, req contract.SessionRegisterRequest) (contract.SessionRegisterResponse, error) {
	if !sessionKey.MatchString(key) {
		return contract.SessionRegisterResponse{}, errors.New("invalid peer session key")
	}
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	return retryWrite(ctx, c, true, func(epoch int64) (contract.SessionRegisterResponse, error) {
		req.Epoch = epoch
		return sessionRPC[contract.SessionRegisterResponse](ctx, c, http.MethodPut, key, "", req)
	})
}

func (c *Client) HumanTurn(ctx context.Context, key string, req contract.HumanTurnRequest) (contract.HumanTurnResponse, error) {
	return retryWrite(ctx, c, false, func(epoch int64) (contract.HumanTurnResponse, error) {
		req.Epoch = epoch
		return sessionRPC[contract.HumanTurnResponse](ctx, c, http.MethodPost, key, "/human", req)
	})
}

func (c *Client) End(ctx context.Context, key string, req contract.SessionEndRequest) (contract.SessionEndResponse, error) {
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	return retryWrite(ctx, c, false, func(epoch int64) (contract.SessionEndResponse, error) {
		req.Epoch = epoch
		v, err := sessionRPC[contract.SessionEndResponse](ctx, c, http.MethodPost, key, "/end", req)
		if hasCode(err, 404, "unknown_session") {
			v.Session.Key = key
			v.Session.Status = "ended"
			return v, nil
		}
		return v, err
	})
}

func (c *Client) Heartbeat(ctx context.Context, req contract.HeartbeatRequest) (contract.HeartbeatResponse, error) {
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	return retryWrite(ctx, c, false, func(epoch int64) (contract.HeartbeatResponse, error) {
		req.Epoch = epoch
		c.mu.Lock()
		req.Cursor = c.cursor
		c.mu.Unlock()
		if req.Sessions == nil {
			req.Sessions = []contract.HeartbeatRequestSessionsItem{}
		}
		return rpc[contract.HeartbeatResponse](ctx, c, http.MethodPost, "/heartbeat", req)
	})
}

func (c *Client) Report(ctx context.Context, req contract.ReportsRequest) (contract.ReportsResponse, error) {
	return retryWrite(ctx, c, true, func(epoch int64) (contract.ReportsResponse, error) {
		req.Epoch = epoch
		return rpc[contract.ReportsResponse](ctx, c, http.MethodPost, "/reports", req)
	})
}
