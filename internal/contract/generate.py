# /// script
# dependencies = ["pyyaml==6.0.3"]
# ///
"""Generate wire DTOs from the JSON Schema components, independent of OpenAPI tooling."""

import json
import re
import subprocess
from pathlib import Path

import yaml

schemas = yaml.safe_load(Path("../../api/openapi.yaml").read_bytes())["components"]["schemas"]
declarations = {}
constants = {}
initialisms = {"id": "ID", "ids": "IDs", "pid": "PID", "os": "OS", "uuid": "UUID",
               "spki": "SPKI", "pem": "PEM", "pkcs8": "PKCS8", "ttl": "TTL"}


def ident(name):
    words = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).split("_")
    return "".join(initialisms.get(w.lower(), w[:1].upper() + w[1:]) for w in words)


def go_type(name, schema):
    if "$ref" in schema:
        return ident(schema["$ref"].split("/")[-1])
    kind = schema.get("type")
    if isinstance(kind, list) and "null" in kind:
        return "*" + go_type(name, schema | {"type": next(k for k in kind if k != "null")})
    if "oneOf" in schema and "properties" not in schema:
        choices = schema["oneOf"]
        if len(choices) == 2 and choices[1].get("type") == "null":
            return "*" + go_type(name, choices[0])
        if all("properties" in c for c in choices):
            props = {k: v for c in choices for k, v in c["properties"].items()}
            required = set.intersection(*(set(c["required"]) for c in choices))
            return go_type(name, {"type": "object", "properties": props, "required": required})
        # Discriminated unions keep the original bytes for decoding by kind.
        assert "discriminator" in schema, name
        return "json.RawMessage"
    if "anyOf" in schema:
        choices = schema["anyOf"]
        assert choices[-1] == {"type": "null"} and all(
            schemas[c["$ref"].split("/")[-1]]["type"] == "string" for c in choices[:-1])
        return "*string"
    if schema.get("x-amesh-open") or name in ("SessionEndRequestReason", "SendRequestOrigin"):
        declarations[name] = "string"
        for value in schema["enum"]:
            constants[name + ident(value.replace("-", "_"))] = (name, value)
        return name
    if kind == "array":
        return "[]" + go_type(name + "Item", schema["items"])
    if kind != "object":
        return {"string": "string", "boolean": "bool", "integer": "int64"}[kind]
    if "properties" not in schema:
        return "map[string]any"
    fields = []
    for wire, prop in schema["properties"].items():
        field = ident(wire)
        typ = go_type(name + field, prop)
        tag = wire
        if wire not in schema.get("required", []):
            tag += ",omitempty"
            if not typ.startswith("*"):
                typ = "*" + typ
        fields.append(f'{field} {typ} `json:"{tag}"`')
    declarations[name] = "struct {\n" + "\n".join(fields) + "\n}"
    return name


header = "// Code generated from api/openapi.yaml by generate.py. DO NOT EDIT.\npackage contract\n"
for wire, schema in schemas.items():
    name = ident(wire)
    typ = go_type(name, schema)
    if typ != name:
        declarations[name] = "= " + typ

types = "\n".join(f"type {name} {body}" for name, body in declarations.items())
types += "\nconst (\n" + "\n".join(f"{key} {typ} = {json.dumps(value)}" for key, (typ, value) in constants.items()) + "\n)\n"
defaults = []
bounds = []
for field, rule in schemas["Settings"]["properties"].items():
    name = ident(field)
    defaults.append(f'{name}: {rule["default"]},')
    bounds.append(f's.{name} >= {rule["minimum"]} && s.{name} <= {rule["maximum"]}')
types += "\nfunc DefaultSettings() Settings { return Settings{" + "".join(defaults) + "} }\n"
types += "func (s Settings) Valid() bool { return " + " &&\n".join(bounds) + " }\n"
registry = "\n".join(f'"{name}": new({ident(name)}),' for name in schemas)
for path, content in [("types.gen.go", 'import "encoding/json"\n' + types),
                      ("types_gen_test.go", "var wireTypes = map[string]any{\n" + registry + "\n}")]:
    Path(path).write_bytes(subprocess.run(["gofmt"], input=(header + content).encode(),
                                         capture_output=True, check=True).stdout)
