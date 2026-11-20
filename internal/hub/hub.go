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
	"os"
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
	streamConnected bool
	statusSince     time.Time
	streamRunning   bool
	usedHello       uint64
	logChanged      chan struct{}
	instance        string
	helloCount      uint64
	lastHelloErr    error
	verifying       bool
	presenceMu      sync.Mutex
	bindingMu       sync.Mutex
}

// ErrNoHub halts a client whose configuration names no hub; lane mode works without one.
var ErrNoHub = errors.New("no hub URL is configured (set AMESH_HUB_URL)")

func New(cfg config.Config) *Client { return NewWithTransport(cfg, nil) }

func NewWithTransport(cfg config.Config, transport http.RoundTripper) *Client {
	var n [16]byte
	rand.Read(n[:])
	c := &Client{
		statusSince: time.Now(),
		instance:    encoding.EncodeToString(n[:]),
		logChanged:  make(chan struct{}),
		cfg:         cfg,
		http:        &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		key:         sync.OnceValues(func() (identity, error) { return loadKey(filepath.Join(cfg.StateDir, "machine.key")) }),
		current:     contract.HelloResponse{Settings: contract.DefaultSettings(), Limits: contract.Limits{MaxTextBytes: 32768}},
		stopped:     make(chan struct{}),
		draining:    make(chan struct{}, 1),
		wake:        make(chan struct{}, 1),
		waiters:     make(map[string][]chan SendOutcome),
	}
	if cfg.HubURL == "" {
		c.halt = ErrNoHub
		close(c.stopped)
	}
	return c
}

func (c *Client) Connection() (contract.HelloResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, c.halt
}

type HTTPError struct {
	Status     int                `json:"status"`
	Body       contract.ErrorBody `json:"body"`
	RetryAfter time.Duration      `json:"retryAfter,omitempty"`
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("peer HTTP %d (%s)", e.Status, e.Body.Error)
}

func (e *HTTPError) transient() bool {
	return e.Body.Error == "" || e.Status >= 500 || e.Status == 429 || e.Status == 408 || (e.Status == 409 && e.Body.Error == contract.ErrorCodeMachineInUse) || (e.Status == 401 && e.Body.Error == contract.ErrorCodeStaleTimestamp)
}

func (c *Client) stop(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.halt == nil {
		c.halt = err
		c.statusSince = time.Now()
		close(c.stopped)
	}
	return c.halt
}

func (c *Client) open(ctx context.Context, method, route string, payload any, media string) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		if _, err := c.Connection(); err != nil {
			return nil, err
		}
		var body []byte
		if payload != nil {
			if hello, ok := payload.(contract.HelloRequest); ok && attempt > 0 {
				hello.Takeover = false
				payload = hello
			}
			var err error
			body, err = json.Marshal(payload)
			if err != nil {
				return nil, err
			}
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
		var wire struct {
			contract.ErrorBody
			Message *string `json:"message"`
		}
		const maxErrorBody = 1 << 18
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
		if readErr == nil && len(raw) <= maxErrorBody && json.Unmarshal(raw, &wire) == nil && wire.Message != nil {
			e.Body = wire.ErrorBody
			e.Body.Message = *wire.Message
		}
		_ = resp.Body.Close()
		if seconds, parseErr := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 32); parseErr == nil && seconds > 0 {
			e.RetryAfter = time.Duration(seconds) * time.Second
		}
		switch {
		case e.Body.Error == "" && (route == "/hello" || strings.HasPrefix(route, "/events?")):
			return nil, e
		case e.Status < 400:
			return nil, c.stop(e)
		case e.Status == 401 && (e.Body.Error == contract.ErrorCodeBadSignature || e.Body.Error == contract.ErrorCodeReplayed):
			if attempt == 0 {
				continue
			}
			return nil, c.stop(e)
		case e.Status == 401 && e.Body.Error == contract.ErrorCodeStaleTimestamp:
			if attempt == 0 && e.Body.ServerTime != nil {
				c.mu.Lock()
				c.offset = time.Until(time.UnixMilli(*e.Body.ServerTime))
				c.mu.Unlock()
				continue
			}
		case e.Status == 401 && e.Body.Error == contract.ErrorCodeUnknownMachine, e.Status == 403 && e.Body.Error == contract.ErrorCodeMachineRevoked, e.Status == 426:
			return nil, c.stop(e)
		}
		return nil, e
	}
}

func rpc[T any](ctx context.Context, c *Client, method, route string, request any) (T, error) {
	var result T
	conn, _ := c.Connection()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(conn.Settings.RequestTimeoutMs)*time.Millisecond)
	defer cancel()
	resp, err := c.open(ctx, method, route, request, "application/json")
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

func (c *Client) Hello(ctx context.Context, req contract.HelloRequest) (contract.HelloResponse, error) {
	c.helloMu.Lock()
	defer c.helloMu.Unlock()
	v, err := c.helloLocked(ctx, req)
	c.lastHelloErr = err
	return v, err
}

func hasCode(err error, status int, code contract.ErrorCode) bool {
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
		Limits   json.RawMessage `json:"limits"`
		Settings json.RawMessage `json:"settings"`
	}
	wire, err := rpc[response](ctx, c, http.MethodPost, "/hello", req)
	if err != nil {
		if c.verifying && hasCode(err, 409, contract.ErrorCodeMachineInUse) {
			return contract.HelloResponse{}, c.stop(&HTTPError{Status: 409, Body: contract.ErrorBody{Error: contract.ErrorCodeSuperseded, Message: "Another daemon owns this machine."}})
		}
		return contract.HelloResponse{}, err
	}
	v := wire.HelloResponse
	var limits struct {
		MaxTextBytes int64 `json:"maxTextBytes"`
	}
	if json.Unmarshal(wire.Limits, &limits) != nil || limits.MaxTextBytes <= 0 {
		limits.MaxTextBytes = 32768
	}
	v.Limits.MaxTextBytes = limits.MaxTextBytes
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
	return rpc[contract.PeersResponse](ctx, c, http.MethodGet, "/peers", nil)
}

func retryWrite[T any](ctx context.Context, c *Client, log string, retry bool, send func(epoch int64) (T, error)) (T, error) {
	var backoff time.Duration
	for {
		c.helloMu.RLock()
		conn, halt := c.Connection()
		if halt != nil {
			c.helloMu.RUnlock()
			var zero T
			return zero, halt
		}
		if log != "" && conn.Log != log {
			c.helloMu.RUnlock()
			var zero T
			return zero, errors.New("delivery report belongs to a previous hub log")
		}
		rev := c.helloCount
		value, err := send(conn.Epoch)
		c.helloMu.RUnlock()
		if err == nil {
			return value, nil
		}
		if hasCode(err, 409, contract.ErrorCodeSuperseded) {
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
	return retryWrite(ctx, c, "", true, func(epoch int64) (contract.SessionRegisterResponse, error) {
		req.Epoch = epoch
		return sessionRPC[contract.SessionRegisterResponse](ctx, c, http.MethodPut, key, "", req)
	})
}

func (c *Client) HumanTurn(ctx context.Context, key string, req contract.HumanTurnRequest) (contract.HumanTurnResponse, error) {
	return retryWrite(ctx, c, "", false, func(epoch int64) (contract.HumanTurnResponse, error) {
		req.Epoch = epoch
		return sessionRPC[contract.HumanTurnResponse](ctx, c, http.MethodPost, key, "/human", req)
	})
}

func (c *Client) End(ctx context.Context, key string, req contract.SessionEndRequest) (contract.SessionEndResponse, error) {
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	return retryWrite(ctx, c, "", false, func(epoch int64) (contract.SessionEndResponse, error) {
		req.Epoch = epoch
		v, err := sessionRPC[contract.SessionEndResponse](ctx, c, http.MethodPost, key, "/end", req)
		if hasCode(err, 404, contract.ErrorCodeUnknownSession) {
			v.Session.Key = key
			v.Session.Status = string(contract.SessionInfoStatusEnded)
			return v, nil
		}
		return v, err
	})
}

func (c *Client) Heartbeat(ctx context.Context, req contract.HeartbeatRequest) (contract.HeartbeatResponse, error) {
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	return retryWrite(ctx, c, "", false, func(epoch int64) (contract.HeartbeatResponse, error) {
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

func (c *Client) Report(ctx context.Context, log string, req contract.ReportsRequest) (contract.ReportsResponse, error) {
	return retryWrite(ctx, c, log, true, func(epoch int64) (contract.ReportsResponse, error) {
		req.Epoch = epoch
		return rpc[contract.ReportsResponse](ctx, c, http.MethodPost, "/reports", req)
	})
}

func (c *Client) Status(s *contract.DaemonStatus) error {
	c.mu.Lock()
	conn := c.current
	s.Hub = contract.DaemonStatusHub{State: contract.DaemonStatusHubStateConnecting, Since: c.statusSince.UnixMilli(), Cursor: c.cursor}
	if conn.Machine.ID != "" {
		s.Machine = &contract.DaemonStatusMachine{ID: conn.Machine.ID, Name: conn.Machine.Name}
	}
	if c.halt != nil {
		s.Hub.State = contract.DaemonStatusHubStateHalted
		h, ok := errors.AsType[*HTTPError](c.halt)
		if ok {
			body := h.Body
			s.Hub.Halt = &body
		} else {
			s.Hub.Halt = &contract.ErrorBody{Error: contract.ErrorCodeInternal, Message: c.halt.Error()}
			if errors.Is(c.halt, os.ErrNotExist) {
				s.Hub.Halt = &contract.ErrorBody{Error: contract.ErrorCodeUnknownMachine, Message: "This machine has no enrollment key."}
			}
		}
	} else if c.streamConnected {
		s.Hub.State = contract.DaemonStatusHubStateConnected
		s.Hub.Epoch = &conn.Epoch
	}
	c.mu.Unlock()
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	q, err := c.readQueue()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.Outbox.Count = int64(len(q))
	if len(q) > 0 {
		s.Outbox.OldestAt = &q[0].At
	}
	return nil
}
