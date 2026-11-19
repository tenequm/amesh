package hub

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub/hubtest"
)

func example[T any](t *testing.T, name string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(hubtest.Example(t, name), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func enrolled(t *testing.T, base string) *Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "amesh-hub-*") //nolint:usetesting // The shared host requires lane-named temporary directories.
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	cfg := config.Config{MachineName: "mac", StateDir: dir, HubURL: base + "/peer"}
	if err = Enroll(t.Context(), cfg, io.Discard); err != nil {
		t.Fatal(err)
	}
	return New(cfg)
}

func freshTransport(t *testing.T, c *Client) {
	t.Helper()
	tr := &http.Transport{DisableKeepAlives: true}
	c.http.Transport = tr
	t.Cleanup(tr.CloseIdleConnections)
}

func TestEnrollmentAndSigning(t *testing.T) {
	c := enrolled(t, "")
	var pub bytes.Buffer
	if err := Enroll(t.Context(), c.cfg, &pub); err != nil {
		t.Fatal(err)
	}
	id, err := c.key()
	if err != nil {
		t.Fatal(err)
	}
	if pub.String() != "ncl peers add --name mac --public-key "+id.spki+"\n" {
		t.Fatal("unexpected enrollment command")
	}
	path := filepath.Join(c.cfg.StateDir, "machine.key")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("incorrect key permissions")
	}
	if err = os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err = New(c.cfg).key(); err == nil {
		t.Fatal("group-readable key accepted")
	}

	key := example[contract.TestKey](t, "testKey")
	spki, _ := encoding.DecodeString(key.PublicKeySPKI)
	k, err := x509.ParsePKIXPublicKey(spki)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(spki)
	for _, name := range []string{"sigPostHello", "sigEventsConnect", "sigGetPeersFilter", "sigPostMessage", "sigBadChangedBody"} {
		v := example[contract.SignatureVector](t, name)
		if "pm_"+encoding.EncodeToString(digest[:16]) != v.MachineID {
			t.Fatal("machine id mismatch")
		}
		text, bh := canonical(v.Method, v.Target, v.MachineID, v.Ts, v.Nonce, []byte(v.Body))
		if bh != v.BodyHash || text != v.Canonical {
			t.Fatalf("%s canonical mismatch", name)
		}
		sig, _ := encoding.DecodeString(v.Signature)
		hash := sha256.Sum256([]byte(text))
		if ecdsa.VerifyASN1(k.(*ecdsa.PublicKey), hash[:], sig) != v.SignatureValid {
			t.Fatalf("%s signature mismatch", name)
		}
	}
}

func checkSignature(t *testing.T, c *Client, call hubtest.Call) {
	t.Helper()
	id, err := c.key()
	if err != nil {
		t.Fatal(err)
	}
	auth, ok := strings.CutPrefix(call.Header.Get("Authorization"), "HeyDan-Sig v=1,p=1,")
	if !ok {
		t.Fatal("missing signature")
	}
	fields := map[string]string{}
	for part := range strings.SplitSeq(auth, ",") {
		k, v, _ := strings.Cut(part, "=")
		fields[k] = v
	}
	ts, err := strconv.ParseInt(fields["t"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	text, bh := canonical(call.Method, call.Target, id.id, ts, fields["n"], call.Body)
	sig, _ := encoding.DecodeString(fields["s"])
	hash := sha256.Sum256([]byte(text))
	n, _ := encoding.DecodeString(fields["n"])
	if fields["d"] != id.id || fields["bh"] != bh || len(n) != 16 || !ecdsa.VerifyASN1(&id.key.PublicKey, hash[:], sig) {
		t.Fatal("invalid signed request")
	}
}

func TestRequestsAndLostResponses(t *testing.T) {
	const key = "15355-4f6c8c61"
	step := func(method, route, ex string) hubtest.Step {
		return hubtest.Step{Method: method, Target: "/peer/" + route, Reply: hubtest.Reply(t, ex)}
	}
	lost := func(method, route string) hubtest.Step {
		return hubtest.Step{Method: method, Target: "/peer/" + route, Reply: hubtest.LostResponse}
	}
	s := hubtest.Script(t, step("GET", "hello", "helloInfo"), step("POST", "hello", "helloResponse"), lost("PUT", "sessions/"+key), step("PUT", "sessions/"+key, "sessionRegisterResponse"), step("POST", "heartbeat", "heartbeatResponse"), step("POST", "sessions/"+key+"/human", "humanTurnResponse"), step("GET", "peers", "peersResponse"), lost("POST", "reports"), step("POST", "reports", "reportsResponse"), step("POST", "sessions/"+key+"/end", "sessionEndResponse"))
	var c *Client
	synctest.Test(t, func(t *testing.T) {
		c = enrolled(t, s.URL)
		freshTransport(t, c)
		ctx := t.Context()
		if _, err := c.Info(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Hello(ctx, example[contract.HelloRequest](t, "helloRequest")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Register(ctx, key, example[contract.SessionRegisterRequest](t, "sessionRegisterRequest")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Heartbeat(ctx, contract.HeartbeatRequest{}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.HumanTurn(ctx, key, example[contract.HumanTurnRequest](t, "humanTurnRequest")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Peers(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Report(ctx, example[contract.ReportsRequest](t, "reportsRequest")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.End(ctx, key, example[contract.SessionEndRequest](t, "sessionEndRequest")); err != nil {
			t.Fatal(err)
		}
	})
	calls := s.Calls()
	if calls[0].Header.Get("Authorization") != "" {
		t.Fatal("probe was signed")
	}
	for _, call := range calls[1:] {
		checkSignature(t, c, call)
	}
	for _, i := range []int{2, 7} {
		if !bytes.Equal(calls[i].Body, calls[i+1].Body) || calls[i].Header.Get("Authorization") == calls[i+1].Header.Get("Authorization") {
			t.Fatal("invalid retry")
		}
	}
	if !bytes.Contains(calls[4].Body, []byte(`"sessions":[]`)) {
		t.Fatal("empty heartbeat omitted complete list")
	}
}

func TestOutboxRestartAndRetry(t *testing.T) {
	s := hubtest.Script(t, hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")}, hubtest.Step{Method: "POST", Target: "/peer/messages", Reply: hubtest.LostResponse}, hubtest.Step{Method: "POST", Target: "/peer/messages", Reply: hubtest.Reply(t, "sendResponse")})
	var c *Client
	synctest.Test(t, func(t *testing.T) {
		c = enrolled(t, s.URL)
		req := example[contract.SendRequest](t, "sendStartRequest")
		begin := time.Now()
		if _, err := c.Send(t.Context(), req); !errors.Is(err, ErrQueued) {
			t.Fatal(err)
		}
		if time.Since(begin) != 5*time.Second {
			t.Fatal("incorrect send wait")
		}
		if count, err := c.Pending(req.From.Session); err != nil || count != 1 {
			t.Fatal("entry was not durable")
		}
		c = New(c.cfg)
		freshTransport(t, c)
		if _, err := c.Hello(t.Context(), example[contract.HelloRequest](t, "helloRequest")); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		err := c.RunOutbox(ctx, func(out SendOutcome) {
			if out.Err != nil || out.Response.Message.ID != req.ID {
				t.Error("wrong outcome")
			}
			cancel()
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if n, pendingErr := c.Pending(""); pendingErr != nil || n != 0 {
			t.Fatal("acknowledged entry on disk")
		}
	})
	calls := s.Calls()
	a, b := calls[1], calls[2]
	if !bytes.Equal(a.Body, b.Body) || a.Header.Get("Authorization") == b.Header.Get("Authorization") {
		t.Fatal("retry changed body or reused signature")
	}
	checkSignature(t, c, a)
	checkSignature(t, c, b)
}

func TestSettingsAndAuthenticationRecovery(t *testing.T) {
	for _, mutation := range []string{"missing", "type", "bounds", "zero"} {
		t.Run(mutation, func(t *testing.T) {
			body := example[map[string]any](t, "helloResponse")
			settings := body["settings"].(map[string]any)
			settings["heartbeatMs"] = 6000
			switch mutation {
			case "missing":
				delete(settings, "sendWaitMs")
			case "type":
				settings["heartbeatMs"] = "bad"
			case "bounds":
				settings["requestTimeoutMs"] = -1
			case "zero":
				settings["sendWaitMs"] = 0
			}
			reply := func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(body) }
			s := hubtest.Script(t, hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "errStaleTimestamp")}, hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: reply})
			c := enrolled(t, s.URL)
			resp, err := c.Hello(t.Context(), contract.HelloRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if mutation == "zero" {
				if resp.Settings.SendWaitMs != 0 || resp.Settings.HeartbeatMs != 6000 {
					t.Fatal("valid settings lost")
				}
			} else if resp.Settings != contract.DefaultSettings() {
				t.Fatal("incomplete settings fallback")
			}
			calls := s.Calls()
			checkSignature(t, c, calls[1])
			server := example[contract.ErrorBody](t, "errStaleTimestamp")
			if !strings.Contains(calls[1].Header.Get("Authorization"), ",t="+strconv.FormatInt(*server.ServerTime/1000, 10)+",") {
				t.Fatal("clock correction missing")
			}
		})
	}
}

func TestOutboxExpiryWithoutHello(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := enrolled(t, "http://amesh-hub.test")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		outcomes := make(chan SendOutcome, 1)
		done := make(chan error, 1)
		go func() { done <- c.RunOutbox(ctx, func(out SendOutcome) { outcomes <- out; cancel() }) }()
		req := example[contract.SendRequest](t, "sendStartRequest")
		if _, err := c.Send(ctx, req); !errors.Is(err, ErrQueued) {
			t.Fatal(err)
		}
		start := time.Now()
		out := <-outcomes
		h, ok := errors.AsType[*HTTPError](out.Err)
		if !ok || h.Body.Error != "queue_expired" || time.Since(start) != 24*time.Hour-5*time.Second {
			t.Fatal("offline expiry failed")
		}
		if !errors.Is(<-done, context.Canceled) {
			t.Fatal("uploader did not stop")
		}
	})
}

func TestRetryAcrossEpochsAndReplacedLog(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(strconv.FormatBool(replace), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				hello := example[contract.HelloResponse](t, "helloResponse")
				hello.Epoch++
				if replace {
					hello.Log = "lg_replaced123"
				}
				reply := func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(hello) }
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				steps := []hubtest.Step{
					{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
					{Method: "POST", Target: "/peer/messages", Reply: func(http.ResponseWriter, *http.Request) { cancel(); panic(http.ErrAbortHandler) }},
					{Method: "POST", Target: "/peer/hello", Reply: reply},
				}
				if !replace {
					steps = append(steps, hubtest.Step{Method: "POST", Target: "/peer/messages", Reply: hubtest.Reply(t, "sendResponse")})
				}
				s := hubtest.MemoryScript(t, steps...)
				c := enrolled(t, "http://amesh-hub.test")
				c.http.Transport = s.Client().Transport
				hr := example[contract.HelloRequest](t, "helloRequest")
				if _, err := c.Hello(ctx, hr); err != nil {
					t.Fatal(err)
				}
				if err := c.event(ctx, contract.EventStreamItem{Event: "synchronized", Data: `{"kind":"synchronized","seq":41}`}, func(context.Context, contract.EventStreamItem) error { return nil }); err != nil {
					t.Fatal(err)
				}
				req := example[contract.SendRequest](t, "sendStartRequest")
				if _, err := c.Send(ctx, req); !errors.Is(err, ErrQueued) {
					t.Fatal(err)
				}
				same := req
				same.Origin = "human"
				same.Epoch = 88
				same.FromMode = nil
				if _, err := c.Send(ctx, same); !errors.Is(err, ErrQueued) {
					t.Fatal("semantic duplicate refused")
				}
				different := req
				different.Text += "!"
				if _, err := c.Send(ctx, different); !hasCode(err, 409, "id_conflict") {
					t.Fatal("conflicting contents accepted")
				}
				if err := c.RunOutbox(ctx, nil); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				restarted := NewWithTransport(c.cfg, s.Client().Transport)
				if _, err := restarted.Hello(t.Context(), hr); err != nil {
					t.Fatal(err)
				}
				if replace && restarted.cursor != 0 || !replace && restarted.cursor != 41 {
					t.Fatal("wrong cursor after reconnect")
				}
				run, stop := context.WithCancel(t.Context())
				defer stop()
				err := restarted.RunOutbox(run, func(out SendOutcome) {
					defer stop()
					if replace && !hasCode(out.Err, 410, "log_replaced") || !replace && out.Err != nil {
						t.Errorf("retry outcome %v", out.Err)
					}
				})
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				calls := s.Calls()
				if !replace {
					var before, after contract.SendRequest
					if json.Unmarshal(calls[1].Body, &before) != nil || json.Unmarshal(calls[3].Body, &after) != nil || before.Epoch != 7 || after.Epoch != 8 || before.ID != after.ID || before.Text != after.Text || before.From != after.From || !sameTarget(before.To.Address, after.To.Address) {
						t.Fatal("send identity changed across epochs")
					}
				}
				if c.instance == restarted.instance {
					t.Fatal("new process reused instance")
				}
			})
		})
	}
}

func TestLostHelloAndConfirmedTakeover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := hubtest.MemoryScript(t,
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.LostResponse},
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
			hubtest.Step{Method: "GET", Target: "/peer/events?after=0&epoch=7", Reply: hubtest.Reply(t, "errSuperseded")},
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
			hubtest.Step{Method: "GET", Target: "/peer/events?after=0&epoch=7", Reply: eventReply("revoked", hubtest.Example(t, "eventRevoked"))})
		c := enrolled(t, "http://amesh-hub.test")
		c.http.Transport = s.Client().Transport
		if _, err := c.Hello(t.Context(), example[contract.HelloRequest](t, "helloRequest")); err == nil {
			t.Fatal("expected lost hello")
		}
		err := c.Stream(t.Context(), func(context.Context, contract.EventStreamItem) error { return nil })
		if !hasCode(err, 403, "machine_revoked") {
			t.Fatal(err)
		}
		calls := s.Calls()
		for i, index := range []int{0, 1, 3} {
			var req contract.HelloRequest
			if json.Unmarshal(calls[index].Body, &req) != nil || req.Instance != c.instance || req.Takeover != (i == 0) {
				t.Fatal("lost process identity or repeated takeover")
			}
		}
	})
}

func TestOutboxRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := hubtest.MemoryScript(t, hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")}, hubtest.Step{Method: "POST", Target: "/peer/messages", Reply: hubtest.Reply(t, "errRateLimited")}, hubtest.Step{Method: "POST", Target: "/peer/messages", Reply: hubtest.Reply(t, "sendResponse")})
		c := enrolled(t, "http://amesh-hub.test")
		c.http.Transport = s.Client().Transport
		if _, err := c.Hello(t.Context(), contract.HelloRequest{}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		start := time.Now()
		go func() {
			done <- c.RunOutbox(ctx, func(out SendOutcome) {
				if out.Err != nil {
					t.Error(out.Err)
				}
				cancel()
			})
		}()
		if _, err := c.Send(ctx, example[contract.SendRequest](t, "sendStartRequest")); !errors.Is(err, ErrQueued) || !hasCode(err, 429, "rate_limited") {
			t.Fatal("missing pacing result")
		}
		if n, err := c.Pending(""); err != nil || n != 1 {
			t.Fatal("paced entry missing")
		}
		if !errors.Is(<-done, context.Canceled) {
			t.Fatal("uploader did not stop")
		}
		if d := time.Since(start); d < 42*time.Second || d >= 43*time.Second {
			t.Fatalf("wrong Retry-After delay %v", d)
		}
	})
}

func TestSleepOverrun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		time.Sleep(70 * time.Second)
		if slept(start, 10*time.Second) {
			t.Fatal("overrun boundary")
		}
		time.Sleep(time.Millisecond)
		if !slept(start, 10*time.Second) {
			t.Fatal("missed sleep")
		}
	})
}

func TestWriteRecoversEpochAndCompleteMembership(t *testing.T) {
	const key = "15355-4f6c8c61"
	hello := example[contract.HelloResponse](t, "helloResponse")
	hello.Epoch++
	s := hubtest.Script(t,
		hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
		hubtest.Step{Method: "POST", Target: "/peer/heartbeat", Reply: hubtest.Reply(t, "errSuperseded")},
		hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(hello) }},
		hubtest.Step{Method: "POST", Target: "/peer/heartbeat", Reply: hubtest.Reply(t, "heartbeatResponse")},
		hubtest.Step{Method: "POST", Target: "/peer/sessions/" + key + "/end", Reply: hubtest.Reply(t, "errUnknownSession")})
	c := enrolled(t, s.URL)
	if _, err := c.Hello(t.Context(), contract.HelloRequest{}); err != nil {
		t.Fatal(err)
	}
	req := example[contract.HeartbeatRequest](t, "heartbeatRequest")
	if _, err := c.Heartbeat(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	end, err := c.End(t.Context(), key, contract.SessionEndRequest{Reason: "exited"})
	if err != nil || end.Session.Status != "ended" || end.Session.Key != key {
		t.Fatal("already ended key")
	}
	var replay contract.HeartbeatRequest
	if json.Unmarshal(s.Calls()[3].Body, &replay) != nil || replay.Epoch != 8 || !slices.Equal(replay.Sessions, req.Sessions) {
		t.Fatal("membership changed across retry")
	}
}

func TestProxyErrorAllowsRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		denied := func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "proxy denied", http.StatusForbidden) }
		s := hubtest.MemoryScript(t,
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: denied},
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
			hubtest.Step{Method: "GET", Target: "/peer/events?after=0&epoch=7", Reply: denied},
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
			hubtest.Step{Method: "GET", Target: "/peer/events?after=0&epoch=7", Reply: eventReply("revoked", hubtest.Example(t, "eventRevoked"))})
		c := enrolled(t, "http://amesh-hub.test")
		c.http.Transport = s.Client().Transport
		if _, err := c.Hello(t.Context(), contract.HelloRequest{}); err == nil {
			t.Fatal("expected a proxy error")
		}
		err := c.Stream(t.Context(), func(context.Context, contract.EventStreamItem) error { return nil })
		if !hasCode(err, 403, "machine_revoked") {
			t.Fatal(err)
		}
	})
}

func TestTwoDaemonsWithOneEnrollment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hello := example[contract.HelloResponse](t, "helloResponse")
		hello.Epoch = 8
		newOwner := func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(hello) }
		s := hubtest.MemoryScript(t,
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "helloResponse")},
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: newOwner},
			hubtest.Step{Method: "POST", Target: "/peer/heartbeat", Reply: hubtest.Reply(t, "errSuperseded")},
			hubtest.Step{Method: "POST", Target: "/peer/hello", Reply: hubtest.Reply(t, "errMachineInUse")},
			hubtest.Step{Method: "GET", Target: "/peer/peers", Reply: hubtest.Reply(t, "peersResponse")})
		first := enrolled(t, "http://amesh-hub.test")
		second := enrolled(t, "http://amesh-hub.test")
		key, err := os.ReadFile(filepath.Join(first.cfg.StateDir, "machine.key"))
		if err != nil {
			t.Fatal(err)
		}
		if err = saveFile(filepath.Join(second.cfg.StateDir, "machine.key"), key, false); err != nil {
			t.Fatal(err)
		}
		first.http.Transport = s.Client().Transport
		second.http.Transport = s.Client().Transport
		req := example[contract.HelloRequest](t, "helloRequest")
		if _, err = first.Hello(t.Context(), req); err != nil {
			t.Fatal(err)
		}
		if _, err = second.Hello(t.Context(), req); err != nil {
			t.Fatal(err)
		}
		if _, err = first.Heartbeat(t.Context(), contract.HeartbeatRequest{}); !hasCode(err, 409, "superseded") {
			t.Fatal("unconfirmed takeover")
		}
		if _, err = first.Peers(t.Context()); !hasCode(err, 409, "superseded") {
			t.Fatal("old owner resumed")
		}
		if _, err = second.Peers(t.Context()); err != nil {
			t.Fatal(err)
		}
		calls := s.Calls()
		var opened, takeover, probe contract.HelloRequest
		if json.Unmarshal(calls[0].Body, &opened) != nil || json.Unmarshal(calls[1].Body, &takeover) != nil || json.Unmarshal(calls[3].Body, &probe) != nil {
			t.Fatal("invalid hello")
		}
		if opened.Instance == takeover.Instance || probe.Instance != opened.Instance || !takeover.Takeover || probe.Takeover {
			t.Fatal("owner identity changed or repeated takeover")
		}
		for _, call := range calls {
			checkSignature(t, first, call)
		}
	})
}
