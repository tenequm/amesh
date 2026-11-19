package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
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
	cursor   int64
}

func New(cfg config.Config) *Client {
	return &Client{
		cfg: cfg, http: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		key:     sync.OnceValues(func() (identity, error) { return loadKey(filepath.Join(cfg.StateDir, "machine.key")) }),
		current: contract.HelloResponse{Settings: contract.DefaultSettings(), Limits: contract.Limits{MaxTextBytes: 32768, QueueTTLMs: 86400000}},
		stopped: make(chan struct{}), draining: make(chan struct{}, 1), wake: make(chan struct{}, 1), waiters: make(map[string][]chan SendOutcome),
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
	return e.Status >= 500 || e.Status == 429 || e.Status == 408 || e.Body.Error == "machine_in_use" || e.Body.Error == "stale_timestamp"
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
		switch e.Body.Error {
		case "bad_signature", "replayed":
			if attempt == 0 {
				continue
			}
			return nil, c.stop(e)
		case "stale_timestamp":
			if attempt == 0 && e.Body.ServerTime != nil {
				c.mu.Lock()
				c.offset = time.Until(time.UnixMilli(*e.Body.ServerTime))
				c.mu.Unlock()
				continue
			}
		case "unknown_machine", "machine_revoked", "superseded", "upgrade_required", "protocol":
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
	c.mu.Lock()
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
		return contract.HelloResponse{}, err
	}
	v := wire.HelloResponse
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

func retryWrite[T any](ctx context.Context, c *Client, send func(epoch int64) (T, error)) (T, error) {
	var backoff time.Duration
	for {
		c.helloMu.RLock()
		conn, halt := c.Connection()
		if halt != nil {
			c.helloMu.RUnlock()
			var zero T
			return zero, halt
		}
		value, err := send(conn.Epoch)
		c.helloMu.RUnlock()
		if err == nil {
			return value, nil
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
	return retryWrite(ctx, c, func(epoch int64) (contract.SessionRegisterResponse, error) {
		req.Epoch = epoch
		return sessionRPC[contract.SessionRegisterResponse](ctx, c, http.MethodPut, key, "", req)
	})
}

func (c *Client) HumanTurn(ctx context.Context, key string, req contract.HumanTurnRequest) (contract.HumanTurnResponse, error) {
	c.helloMu.RLock()
	defer c.helloMu.RUnlock()
	conn, _ := c.Connection()
	req.Epoch = conn.Epoch
	return sessionRPC[contract.HumanTurnResponse](ctx, c, http.MethodPost, key, "/human", req)
}

func (c *Client) End(ctx context.Context, key string, req contract.SessionEndRequest) (contract.SessionEndResponse, error) {
	c.helloMu.RLock()
	defer c.helloMu.RUnlock()
	conn, _ := c.Connection()
	req.Epoch = conn.Epoch
	return sessionRPC[contract.SessionEndResponse](ctx, c, http.MethodPost, key, "/end", req)
}

func (c *Client) Heartbeat(ctx context.Context, req contract.HeartbeatRequest) (contract.HeartbeatResponse, error) {
	c.helloMu.RLock()
	defer c.helloMu.RUnlock()
	c.mu.Lock()
	req.Epoch = c.current.Epoch
	req.Cursor = c.cursor
	c.mu.Unlock()
	if req.Sessions == nil {
		req.Sessions = []contract.HeartbeatRequestSessionsItem{}
	}
	return rpc[contract.HeartbeatResponse](ctx, c, http.MethodPost, "/heartbeat", req)
}

func (c *Client) Report(ctx context.Context, req contract.ReportsRequest) (contract.ReportsResponse, error) {
	return retryWrite(ctx, c, func(epoch int64) (contract.ReportsResponse, error) {
		req.Epoch = epoch
		return rpc[contract.ReportsResponse](ctx, c, http.MethodPost, "/reports", req)
	})
}

func (*Client) Stream(ctx context.Context, handle func(context.Context, contract.EventStreamItem) error) error {
	return errors.ErrUnsupported
}
func (*Client) Ack(ctx context.Context, seq int64) error { return errors.ErrUnsupported }
