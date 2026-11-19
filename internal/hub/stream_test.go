package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub/hubtest"
)

func TestStreamReplayAtEveryByte(t *testing.T) {
	raw := bytes.ReplaceAll(hubtest.Example(t, "streamReplay"), []byte("Rerun done"), []byte("Résumé ✓"))
	var seq []string
	status := 0
	err := parseStream(iotest.OneByteReader(bytes.NewReader(raw)), func(ev contract.EventStreamItem) error {
		if !json.Valid([]byte(ev.Data)) {
			t.Fatal("corrupt event")
		}
		if ev.ID != nil {
			seq = append(seq, *ev.ID)
		}
		if ev.Event == "status" {
			status++
		}
		if ev.Event == "message" && !strings.Contains(ev.Data, "Résumé ✓") {
			t.Fatal("split UTF-8 lost")
		}
		return nil
	})
	if !errors.Is(err, io.EOF) || !slices.Equal(seq, []string{"118", "119", "121", "122", "123"}) || status != 1 {
		t.Fatal("replay mismatch")
	}
	err = parseStream(strings.NewReader("event: message\ndata: {}\n"), func(contract.EventStreamItem) error { t.Fatal("partial event emitted"); return nil })
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestLowWaterMarkSurvivesReplayAndRestart(t *testing.T) {
	c := enrolled(t, "http://amesh-hub.test")
	if err := c.loadCursor(); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	hold := func(_ context.Context, item contract.EventStreamItem) error {
		if item.ID == nil || *item.ID == "119" {
			return nil
		}
		seq, _ := strconv.ParseInt(*item.ID, 10, 64)
		return c.Ack(ctx, seq)
	}
	replay := hubtest.Example(t, "streamReplay")
	err := parseStream(bytes.NewReader(replay), func(item contract.EventStreamItem) error { return c.event(ctx, item, hold) })
	if !errors.Is(err, io.EOF) || c.cursor != 118 {
		t.Fatalf("cursor crossed pending 119: %d", c.cursor)
	}
	noDuplicate := func(_ context.Context, item contract.EventStreamItem) error {
		if item.ID != nil {
			t.Fatal("numbered event dispatched twice")
		}
		return nil
	}
	err = parseStream(bytes.NewReader(replay), func(item contract.EventStreamItem) error { return c.event(ctx, item, noDuplicate) })
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	restarted := New(c.cfg)
	if err = restarted.loadCursor(); err != nil || restarted.cursor != 118 {
		t.Fatal("restart crossed unacknowledged message")
	}
	message := example[contract.MessageEvent](t, "eventMessage")
	message.Seq = 125
	data, _ := json.Marshal(message)
	id := "125"
	if err = c.event(ctx, contract.EventStreamItem{ID: &id, Event: "message", Data: string(data)}, noDuplicate); err != nil {
		t.Fatal(err)
	}
	if err = c.Ack(ctx, 119); err != nil || c.cursor != 125 {
		t.Fatal("ack did not release message and duplicate")
	}
	message.Seq = 126
	data, _ = json.Marshal(message)
	id = "126"
	if err = c.event(ctx, contract.EventStreamItem{ID: &id, Event: "message", Data: string(data)}, noDuplicate); err != nil || c.cursor != 126 {
		t.Fatal("recent-id replay")
	}
	id = "129"
	if err = c.event(ctx, contract.EventStreamItem{ID: &id, Event: "future", Data: `{"kind":"future"}`}, noDuplicate); err != nil || c.cursor != 129 {
		t.Fatal("unknown event cursor")
	}
	if n, ackErr := c.cursor, c.Ack(ctx, 150); ackErr == nil || n != c.cursor {
		t.Fatal("ack beyond log accepted")
	}
	if err = restarted.resetCursor(4); err != nil || restarted.cursor != 4 {
		t.Fatal("log reset failed")
	}
}

func eventReply(kind string, raw []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, raw)
	}
}

func TestSilenceReconnectAndFencing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		hello := example[contract.HelloResponse](t, "helloResponse")

		second := func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(hello) }
		ping := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			for range 3 {
				io.WriteString(w, ": partial")
				w.(http.Flusher).Flush()
				time.Sleep(15 * time.Second)
			}
			<-r.Context().Done()
		}
		s := hubtest.MemoryScript(t,
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
			hubtest.Step{Method: "GET", Target: "/peer/events?after=0&epoch=7", Reply: ping},
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: second},
			hubtest.Step{Method: "GET", Target: "/peer/events?after=0&epoch=7", Reply: eventReply("superseded", hubtest.Example(t, "eventSuperseded"))}, hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "errMachineInUse")})
		c := enrolled(t, "http://amesh-hub.test")
		c = NewWithTransport(c.cfg, s.Client().Transport)
		if _, err := c.Hello(t.Context(), example[contract.HelloRequest](t, "helloRequest")); err != nil {
			t.Fatal(err)
		}
		err := c.Stream(t.Context(), func(context.Context, contract.EventStreamItem) error { t.Fatal("unexpected event"); return nil })
		h, ok := errors.AsType[*HTTPError](err)
		if !ok || h.Body.Error != "superseded" {
			t.Fatalf("missing halt: %v", err)
		}
		if d := time.Since(start); d < 75*time.Second || d >= 76*time.Second {
			t.Errorf("keepalive/reconnect took %v", d)
		}
		if _, err = c.Peers(t.Context()); err == nil {
			t.Fatal("RPC after fencing")
		}
		calls := s.Calls()
		for i, index := range []int{0, 2} {
			var req contract.HelloRequest
			if err = json.Unmarshal(calls[index].Body, &req); err != nil {
				t.Fatal(err)
			}
			if i == 0 && (!req.Takeover || req.ResumeEpoch != nil) || i == 1 && (req.Takeover || req.ResumeEpoch == nil || *req.ResumeEpoch != 7) {
				t.Fatal("invalid takeover/resume")
			}
		}
	})
}

func TestCanceledReplayAndFatalCodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := hubtest.MemoryScript(t,
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
			hubtest.Step{Method: "GET", Target: "/peer/events?after=0&epoch=7", Reply: hubtest.Reply(t, "streamReplay")})
		c := enrolled(t, "http://amesh-hub.test")
		c.http.Transport = s.Client().Transport
		if _, err := c.Hello(t.Context(), contract.HelloRequest{}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		err := c.Stream(ctx, func(context.Context, contract.EventStreamItem) error { calls++; cancel(); return nil })
		if !errors.Is(err, context.Canceled) || calls != 1 || c.cursor != 0 {
			t.Fatal("cancellation did not preserve pending event")
		}
	})
	for _, name := range []string{"errUnknownMachine", "errMachineRevoked", "errUpgradeRequired", "errProtocol"} {
		t.Run(name, func(t *testing.T) {
			s := hubtest.Script(t, hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, name)})
			c := enrolled(t, s.URL)
			_, err := c.Hello(t.Context(), contract.HelloRequest{})
			_, halt := c.Connection()
			if err == nil || !errors.Is(halt, err) {
				t.Fatal("fatal error did not halt")
			}
		})
	}
}
