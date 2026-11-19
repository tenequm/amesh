package hub

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tenequm/amesh/internal/contract"
)

func parseStream(r io.Reader, handle func(contract.EventStreamItem) error) error {
	scanner := bufio.NewScanner(r)
	const maxFrame = 1 << 20
	scanner.Buffer(make([]byte, 4096), maxFrame)
	var item contract.EventStreamItem
	var data strings.Builder
	size := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if data.Len() > 0 {
				item.Data = strings.TrimSuffix(data.String(), "\n")
				if err := handle(item); err != nil {
					return err
				}
			}
			item = contract.EventStreamItem{}
			data.Reset()
			size = 0
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		size += len(line) + 1
		if size > maxFrame {
			return errors.New("oversized peer event")
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			item.Event = value
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
		case "id":
			item.ID = &value
		case "retry":
			if ms, err := strconv.ParseInt(value, 10, 64); err == nil && ms >= 0 {
				item.Retry = &ms
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}

func (c *Client) event(ctx context.Context, item contract.EventStreamItem, handle func(context.Context, contract.EventStreamItem) error) error {
	var meta struct {
		Kind string          `json:"kind"`
		Seq  json.RawMessage `json:"seq"`
		ID   json.RawMessage `json:"id"`
	}
	if json.Unmarshal([]byte(item.Data), &meta) != nil || meta.Kind == "" || meta.Kind != item.Event {
		return errors.New("invalid peer event metadata")
	}
	var dataSeq *int64
	logEvent := false
	switch item.Event {
	case "revoked", "superseded", "status", "synchronized":
		if item.ID != nil {
			return errors.New("unexpected id on unnumbered peer event")
		}
	case "endpoint":
		if item.ID == nil {
			return handle(ctx, item)
		}
		logEvent = true
	case "message", "delivery", "limit", "idle":
		if item.ID == nil {
			return errors.New("missing peer event id")
		}
		logEvent = true
	}
	if item.ID != nil {
		seq, err := strconv.ParseInt(*item.ID, 10, 64)
		if err != nil || seq <= 0 || strconv.FormatInt(seq, 10) != *item.ID || logEvent && (json.Unmarshal(meta.Seq, &dataSeq) != nil || dataSeq == nil || seq != *dataSeq) {
			return errors.New("invalid peer event sequence")
		}
		id := ""
		if item.Event == "message" {
			if json.Unmarshal(meta.ID, &id) != nil || id == "" {
				return errors.New("missing peer message id")
			}
		}
		dispatch, err := c.observe(ctx, seq, id)
		if err != nil || !dispatch {
			return err
		}
		if !logEvent {
			return c.Ack(ctx, seq)
		}
		if err = handle(ctx, item); err != nil {
			c.mu.Lock()
			if p, ok := c.pending[seq]; ok && !c.stale(ctx) {
				p.handed = false
				c.pending[seq] = p
			}
			c.mu.Unlock()
		}
		return err
	}
	switch item.Event {
	case "revoked":
		return c.stop(&HTTPError{Status: 403, Body: contract.ErrorBody{Error: "machine_revoked", Message: "Machine enrollment was revoked."}})
	case "superseded":
		return &HTTPError{Status: 409, Body: contract.ErrorBody{Error: "superseded", Message: "Another daemon owns this machine."}}
	case "synchronized":
		if json.Unmarshal(meta.Seq, &dataSeq) != nil || dataSeq == nil || *dataSeq < 0 {
			return errors.New("missing or invalid synchronization sequence")
		}
		if err := c.synchronize(ctx, *dataSeq); err != nil {
			return err
		}
		return handle(ctx, item)
	case "status":
		return handle(ctx, item)
	default:
		return nil
	}
}

type activeReader struct {
	r    io.Reader
	last *atomic.Int64
}

func (r activeReader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	if n > 0 {
		r.last.Store(time.Now().UnixNano())
	}
	return n, err
}

var errSlept = errors.New("machine slept during peer connection")

// Wall time includes suspend on hosts whose monotonic clock stops while asleep.
func slept(start time.Time, delay time.Duration) bool {
	return time.Now().UnixNano()-start.UnixNano() > int64(delay+time.Minute)
}

func (c *Client) readStream(ctx context.Context, conn contract.HelloResponse, handle func(context.Context, contract.EventStreamItem) error) (time.Duration, error) {
	requestCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	c.mu.Lock()
	cursor, gen, changed := c.cursor, c.generation, c.logChanged
	currentLog := c.log
	c.mu.Unlock()
	if currentLog != conn.Log {
		return 0, errors.New("event log changed before stream opened")
	}
	done := make(chan struct{})
	silence := time.Duration(conn.Settings.StreamSilenceMs) * time.Millisecond
	go func() {
		defer close(done)
		wait := silence
		for {
			start := time.Now()
			timer := time.NewTimer(wait)
			select {
			case <-changed:
				timer.Stop()
				cancel(errors.New("event log replaced"))
				return
			case <-requestCtx.Done():
				timer.Stop()
				return
			case <-c.stopped:
				timer.Stop()
				_, err := c.Connection()
				cancel(err)
				return
			case <-timer.C:
				if slept(start, wait) {
					cancel(errSlept)
					return
				}
				wait = silence - time.Duration(time.Now().UnixNano()-last.Load())
				if wait <= 0 {
					cancel(errors.New("peer stream silence timeout"))
					return
				}
				wait = min(wait, silence)
			}
		}
	}()
	defer func() { cancel(nil); <-done }()
	route := fmt.Sprintf("/events?after=%d&epoch=%d", cursor, conn.Epoch)
	resp, err := c.open(requestCtx, http.MethodGet, route, nil, "text/event-stream")
	if err != nil {
		if cause := context.Cause(requestCtx); cause != nil {
			return 0, cause
		}
		return 0, err
	}
	defer resp.Body.Close()
	media, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if media != "text/event-stream" {
		return 0, errors.New("invalid peer stream content type")
	}
	start := time.Now()
	last.Store(start.UnixNano())
	eventCtx := context.WithValue(ctx, generationKey{}, gen)
	err = parseStream(activeReader{resp.Body, &last}, func(item contract.EventStreamItem) error {
		if cause := context.Cause(requestCtx); cause != nil {
			return cause
		}
		return c.event(eventCtx, item, handle)
	})
	if cause := context.Cause(requestCtx); cause != nil {
		err = cause
	}
	return time.Since(start), err
}

// Stream follows the Hello negotiated by the caller and repeats it before each reconnect.
// Handle may queue work per session. Ack each numbered event when its effect is complete.
func (c *Client) Stream(ctx context.Context, handle func(context.Context, contract.EventStreamItem) error) error {
	c.mu.Lock()
	if c.streamRunning || !c.started || handle == nil {
		c.mu.Unlock()
		return errors.New("Stream requires Hello, one reader and a handler")
	}
	c.streamRunning = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.streamRunning = false
		// A new Stream has a new consumer context. Redeliver unfinished work to it.
		c.head = c.cursor
		c.pending = make(map[int64]pendingEvent)
		c.order = nil
		c.generation++
	}()
	if err := c.loadCursor(); err != nil {
		return err
	}
	c.helloMu.RLock()
	conn, err := c.Connection()
	needHello := c.usedHello == c.helloCount
	if conn.Epoch == 0 && c.lastHelloErr != nil && err == nil {
		// Apply the first failed hello's backoff before reconnecting.
		err = c.lastHelloErr
		needHello = false
	}
	c.helloMu.RUnlock()
	delay := time.Duration(conn.Settings.BackoffInitialMs) * time.Millisecond
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, halt := c.Connection(); halt != nil {
			return halt
		}
		if needHello {
			c.mu.Lock()
			hello := c.hello
			c.mu.Unlock()
			_, err = c.Hello(ctx, hello)
		}
		needHello = true
		var connected time.Duration
		if err == nil {
			c.helloMu.RLock()
			revision := c.helloCount
			c.usedHello = revision
			conn, _ = c.Connection()
			c.helloMu.RUnlock()
			connected, err = c.readStream(ctx, conn, handle)
			if h, ok := errors.AsType[*HTTPError](err); ok && h.Status == 400 && h.Body.Error == "bad_cursor" && h.Body.Seq != nil {
				slog.Warn("resetting peer cursor to hub head", "seq", *h.Body.Seq)
				if resetErr := c.resetCursor(conn.Log, *h.Body.Seq); resetErr != nil {
					return resetErr
				}
				continue
			}
			if hasCode(err, 409, "superseded") {
				err = c.confirmOwner(ctx, revision)
				if err == nil {
					needHello = false
					continue
				}
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, halt := c.Connection(); halt != nil {
			return halt
		}
		conn, _ = c.Connection()
		initial := time.Duration(conn.Settings.BackoffInitialMs) * time.Millisecond
		if connected >= time.Duration(conn.Settings.BackoffResetAfterMs)*time.Millisecond {
			delay = initial
		}
		if errors.Is(err, errSlept) {
			delay = initial
			continue
		}
		if h, ok := errors.AsType[*HTTPError](err); ok {
			if h.Body.Error != "" && !h.transient() {
				return err
			}
		}
		wait := retryDelay(err, delay)
		start := time.Now()
		if err = c.pause(ctx, wait); err != nil {
			return err
		}
		if slept(start, wait) {
			delay = initial
		} else {
			delay = min(2*delay, time.Duration(conn.Settings.BackoffMaxMs)*time.Millisecond)
		}
	}
}
