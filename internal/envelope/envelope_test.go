package envelope

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestContractEnvelopes(t *testing.T) {
	if err := SelfCheck(); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	examples := strings.Split(string(source), "\n  examples:\n")
	if len(examples) != 2 {
		t.Fatal("missing contract example section")
	}
	blocks := regexp.MustCompile(`(?m)^    (\w+):\n`).Split(examples[1], -1)
	for _, block := range blocks {
		if !strings.Contains(block, "x-amesh-schema: NativeUserFrame\n") && !strings.Contains(block, "x-amesh-schema: EnvelopeVector\n") {
			continue
		}
		t.Run(strings.SplitN(block, "\n", 2)[0], func(t *testing.T) {
			_, rest, ok := strings.Cut(block, "content: |-\n")
			if !ok {
				t.Fatal("missing content")
			}
			lines := strings.Split(rest, "\n")
			indent := lines[0][:len(lines[0])-len(strings.TrimLeft(lines[0], " "))]
			var body []string
			for _, line := range lines {
				value, ok := strings.CutPrefix(line, indent)
				if !ok {
					break
				}
				body = append(body, value)
			}
			checkRoundTrip(t, strings.Join(body, "\n"))
		})
	}
	for _, v := range vectors {
		if !strings.Contains(strings.ReplaceAll(string(source), "\n          ", "\n"), v.Content) && !strings.Contains(strings.ReplaceAll(string(source), "\n            ", "\n"), v.Content) {
			t.Fatalf("startup vector drift: %s", v.Name)
		}
	}
}

func checkRoundTrip(t *testing.T, wire string) {
	t.Helper()
	m, err := Parse(wire)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Render(m)
	if err != nil || got != wire {
		t.Fatalf("round trip: %q, %v", got, err)
	}
}

func TestCapturedFrames(t *testing.T) {
	checkRoundTrip(t, `<cross-session-message from="uds:/tmp/cc-socks/a5e1f0c0de01.sock" from-session="amesh-dan-01" from-name="dan" from-mode="prompting">
BODY
</cross-session-message>`)
	checkRoundTrip(t, `<cross-session-message from="uds:/tmp/cc-socks/16951.sock" hop-chain="c4eb2881cd514ae75a89e6ac" from-name="snat-t1" from-mode="prompting">
Spike test reply: (1) sender name "dan", (2) reply address "uds:/tmp/cc-socks/a5e1f0c0de01.sock", (3) cwd basename "t-int".
</cross-session-message>`)
}

func TestCanonicalization(t *testing.T) {
	if got := Address("/tmp/é %"); got != "uds:/tmp/%C3%A9%20%25" {
		t.Fatal(got)
	}
	long := strings.Repeat("🐚", 64)
	wire, err := Render(Message{FromName: " " + long + "x ", Body: "a<\u2002/\tCROSS-tail"})
	if err != nil {
		t.Fatal(err)
	}
	m, err := Parse(wire)
	if err != nil || m.FromName != long+"…" || m.Body != "a&lt;\u2002/\tCROSS-tail" {
		t.Fatalf("%+v, %v", m, err)
	}
	for _, input := range []string{
		` from-name="trailing "`, ` from-mode="bypass" from-name="wrong order"`, ` hop-chain="forged"`, ` from-session="🙂"`, ` from-name="x"  from-mode="bypass"`,
	} {
		if _, err := Parse("<cross-session-message" + input + ">\nbody\n</cross-session-message>"); err == nil {
			t.Error(input)
		}
	}
}
