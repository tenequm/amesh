package contract

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
)

func closeExampleObjects(node any) {
	switch s := node.(type) {
	case map[string]any:
		if s["type"] == "object" && s["properties"] != nil {
			s["additionalProperties"] = false
		}
		for _, child := range s {
			closeExampleObjects(child)
		}
	case []any:
		for _, child := range s {
			closeExampleObjects(child)
		}
	}
}

func TestContractExamples(t *testing.T) {
	raw, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Components struct {
			Schemas  map[string]any
			Examples map[string]struct {
				Schema string `yaml:"x-amesh-schema"`
				Value  any    `yaml:"dataValue"`
				Stream string `yaml:"serializedValue"`
			}
		}
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	closeExampleObjects(doc.Components.Schemas)
	const base = "urn:amesh:contract"
	if err := compiler.AddResource(base, map[string]any{"components": map[string]any{"schemas": doc.Components.Schemas}}); err != nil {
		t.Fatal(err)
	}
	check := func(t *testing.T, name string, value any) {
		t.Helper()
		schema, err := compiler.Compile(base + "#/components/schemas/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if err = schema.Validate(value); err != nil {
			t.Fatalf("schema mismatch for %s (%T)", name, err)
		}
		discriminator, _ := doc.Components.Schemas[name].(map[string]any)["discriminator"].(map[string]any)
		if discriminator != nil {
			field := discriminator["propertyName"].(string)
			key := value.(map[string]any)[field].(string)
			ref := discriminator["mapping"].(map[string]any)[key].(string)
			name = strings.TrimPrefix(ref, "#/components/schemas/")
		}
		dto := reflect.New(reflect.TypeOf(wireTypes[name]).Elem()).Interface()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(raw, dto); err != nil {
			t.Fatalf("decode %s: %T", name, err)
		}
		roundtrip, err := json.Marshal(dto)
		if err != nil {
			t.Fatal(err)
		}
		var before, after any
		if json.Unmarshal(raw, &before) != nil || json.Unmarshal(roundtrip, &after) != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("%s wire fields changed on round trip", name)
		}
	}
	if len(doc.Components.Examples) == 0 {
		t.Fatal("missing examples")
	}
	for name, ex := range doc.Components.Examples {
		t.Run(name, func(t *testing.T) {
			if ex.Stream == "" {
				check(t, ex.Schema, ex.Value)
				return
			}
			for block := range strings.SplitSeq(strings.TrimSpace(ex.Stream), "\n\n") {
				if strings.HasPrefix(block, ":") {
					continue
				}
				item := map[string]any{}
				for line := range strings.SplitSeq(block, "\n") {
					k, v, ok := strings.Cut(line, ": ")
					if !ok {
						t.Fatal("malformed SSE example")
					}
					item[k] = v
				}
				check(t, ex.Schema, item)
				var data any
				dec := json.NewDecoder(bytes.NewBufferString(item["data"].(string)))
				dec.UseNumber()
				if err := dec.Decode(&data); err != nil {
					t.Fatal(err)
				}
				check(t, "PeerEvent", data)
			}
		})
	}
}

func TestOpenEnumValuesAndReasonUnion(t *testing.T) {
	for _, reason := range []string{string(DropReasonRouteFailed), string(ErrorCodeNotMember), "future_reason", ""} {
		var want *string
		if reason != "" {
			want = &reason
		}
		raw, err := json.Marshal(map[string]any{"state": "future_state", "reason": want})
		if err != nil {
			t.Fatal(err)
		}
		var event DeliveryEvent
		if err = json.Unmarshal(raw, &event); err != nil || event.State != "future_state" || !reflect.DeepEqual(event.Reason, want) {
			t.Fatal(event, err)
		}
	}
}
