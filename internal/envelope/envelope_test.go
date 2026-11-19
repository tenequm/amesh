package envelope

import (
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestContractEnvelopes(t *testing.T) {
	if err := SelfCheck(); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Components struct {
			Examples map[string]yaml.Node `yaml:"examples"`
		} `yaml:"components"`
	}
	if err = yaml.Unmarshal(source, &spec); err != nil {
		t.Fatal(err)
	}
	checked := make(map[string]string)
	for name, node := range spec.Components.Examples {
		var header struct {
			Schema string `yaml:"x-amesh-schema"`
		}
		if err := node.Decode(&header); err != nil {
			t.Fatal(err)
		}
		if header.Schema != "NativeUserFrame" && header.Schema != "EnvelopeVector" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			type input struct {
				Body        string   `yaml:"body"`
				From        string   `yaml:"from"`
				FromSession string   `yaml:"fromSession"`
				FromName    string   `yaml:"fromName"`
				FromMode    string   `yaml:"fromMode"`
				HopChain    []string `yaml:"hopChain"`
			}
			var example struct {
				Envelope input `yaml:"x-amesh-envelope"`
				Value    struct {
					Input   input  `yaml:"input"`
					Content string `yaml:"content"`
					From    string `yaml:"from"`
					Message struct {
						Content string `yaml:"content"`
					} `yaml:"message"`
				} `yaml:"dataValue"`
			}
			if err := node.Decode(&example); err != nil {
				t.Fatal(err)
			}
			in, wire := example.Envelope, example.Value.Message.Content
			if header.Schema == "EnvelopeVector" {
				in, wire = example.Value.Input, example.Value.Content
			} else if in.From != example.Value.From {
				t.Fatal("envelope and frame addresses differ")
			}
			got, err := Render(Message{From: in.From, FromSession: in.FromSession, FromName: in.FromName, FromMode: in.FromMode, HopChain: in.HopChain, Body: in.Body})
			if err != nil || got != wire {
				t.Fatalf("render from contract input: %q, %v", got, err)
			}
			checkRoundTrip(t, wire)
			checked[name] = wire
		})
	}
	for _, v := range vectors {
		if checked[v.Name] != v.Content {
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
