package config

import (
	"flag"
	"os"
	"slices"
	"strings"
	"testing"
)

func flags(t *testing.T, args ...string) *Flags {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := Bind(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return f
}

func cleanEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"AMESH_STATE_DIR", "AMESH_SOCKET_PATH", "AMESH_CLAUDE_CONFIG_DIRS", "AMESH_CODEX_HOMES", "CODEX_HOME", "AMESH_EXCLUDE", "AMESH_MACHINE_NAME", "AMESH_HUB_URL"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDefaults(t *testing.T) {
	cleanEnv(t)
	t.Setenv("HOME", "/home/test")
	f := flags(t)

	c, err := f.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if c.StateDir != "/home/test/.local/state/amesh" || c.SocketPath != c.StateDir+"/amesh.sock" ||
		!slices.Equal(c.ClaudeConfigDirs, []string{"/home/test/.claude"}) || !slices.Equal(c.CodexHomes, []string{"/home/test/.codex"}) || len(c.ExcludeRoots) != 0 || c.HubURL != "" {
		t.Fatalf("unexpected paths: %+v", c)
	}
}

func TestEnvironmentAndFlagPrecedence(t *testing.T) {
	t.Setenv("HOME", "/home/test")
	t.Setenv("AMESH_STATE_DIR", "/state")
	t.Setenv("AMESH_SOCKET_PATH", "/run/custom.sock")
	t.Setenv("AMESH_MACHINE_NAME", "mac")
	t.Setenv("AMESH_CLAUDE_CONFIG_DIRS", "~/.claude:~/work/.claude-config")
	t.Setenv("AMESH_EXCLUDE", "/one:/two")
	t.Setenv("AMESH_HUB_URL", "http://localhost:3001/peer/")
	c, err := flags(t, "-machine-name", "server").Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if c.MachineName != "server" || c.StateDir != "/state" || c.SocketPath != "/run/custom.sock" || c.HubURL != "http://localhost:3001/peer" ||
		!slices.Equal(c.ClaudeConfigDirs, []string{"/home/test/.claude", "/home/test/work/.claude-config"}) || !slices.Equal(c.ExcludeRoots, []string{"/one", "/two"}) {
		t.Fatalf("unexpected config: %+v", c)
	}
}

func TestCodexHomes(t *testing.T) {
	cleanEnv(t)
	t.Setenv("HOME", "/home/test")
	t.Setenv("CODEX_HOME", "/one:account")
	check := func(want []string, args ...string) {
		t.Helper()
		c, err := flags(t, args...).Resolve()
		if err != nil || !slices.Equal(c.CodexHomes, want) {
			t.Fatalf("homes = %v, want %v: %v", c.CodexHomes, want, err)
		}
	}
	check([]string{"/one:account"})
	t.Setenv("AMESH_CODEX_HOMES", "~/.codex:/two")
	check([]string{"/home/test/.codex", "/two"})
	check([]string{"/three"}, "-codex-homes", "/three")
}

func TestEmptyExclude(t *testing.T) {
	cleanEnv(t)
	for _, tc := range []struct {
		name string
		env  string
		args []string
	}{
		{name: "empty environment"},
		{name: "empty flag overrides environment", env: "/one:/two", args: []string{"--exclude", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AMESH_EXCLUDE", tc.env)
			c, err := flags(t, tc.args...).Resolve()
			if err != nil {
				t.Fatal(err)
			}
			if len(c.ExcludeRoots) != 0 {
				t.Fatalf("unexpected exclusion roots: %v", c.ExcludeRoots)
			}
		})
	}
}

func TestMalformedConfig(t *testing.T) {
	cleanEnv(t)
	for name, value := range map[string]string{
		"AMESH_MACHINE_NAME":       "bad.name",
		"AMESH_HUB_URL":            "https://user:sample@example.invalid",
		"AMESH_STATE_DIR":          "relative",
		"AMESH_SOCKET_PATH":        "relative",
		"AMESH_CLAUDE_CONFIG_DIRS": "~/one::~/two",
		"AMESH_EXCLUDE":            "/a::/b",
		"AMESH_CODEX_HOMES":        "/valid:relative",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			_, err := flags(t).Resolve()
			if err == nil || strings.Contains(err.Error(), "sample") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
