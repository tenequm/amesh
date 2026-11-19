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
