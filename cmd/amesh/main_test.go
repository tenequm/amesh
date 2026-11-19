package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"testing"

	"github.com/tenequm/amesh/internal/version"
)

func TestVersionBeforeConfiguration(t *testing.T) {
	t.Setenv("AMESH_STATE_DIR", "relative")
	var out bytes.Buffer
	err := run(t.Context(), []string{"-version"}, nil, &out, io.Discard)
	if err != nil || out.String() != version.Value+"\n" {
		t.Fatalf("version output %q, error %v", out.String(), err)
	}
}

func TestHelpKeepsEnvironmentPrivate(t *testing.T) {
	t.Setenv("AMESH_HUB_URL", "private-value")
	var help bytes.Buffer
	err := run(t.Context(), []string{"-help"}, nil, io.Discard, &help)
	if !errors.Is(err, flag.ErrHelp) || bytes.Contains(help.Bytes(), []byte("private-value")) {
		t.Fatal("expected help using built-in defaults")
	}
}
