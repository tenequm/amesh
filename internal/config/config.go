package config

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Config struct {
	StateDir         string
	SocketPath       string
	ClaudeConfigDirs []string
	ExcludeRoots     []string
	MachineName      string
	HubURL           string
}

type Flags struct {
	stateDir, socketPath, configDirs, exclude, machineName, hubURL string
}

func Bind(fs *flag.FlagSet) *Flags {
	f := new(Flags)
	bind := func(target *string, name, env, fallback, usage string) {
		value, ok := os.LookupEnv(env)
		if !ok {
			value = fallback
		}
		fs.StringVar(target, name, value, usage)
		fs.Lookup(name).DefValue = fallback // Keep environment values out of help and flag errors.
	}
	bind(&f.stateDir, "state-dir", "AMESH_STATE_DIR", "~/.local/state/amesh", "daemon state directory")
	bind(&f.socketPath, "socket-path", "AMESH_SOCKET_PATH", "", "Unix socket (default: amesh.sock in state directory)")
	bind(&f.configDirs, "claude-config-dirs", "AMESH_CLAUDE_CONFIG_DIRS", "~/.claude", "colon-separated Claude config directories")
	bind(&f.exclude, "exclude", "AMESH_EXCLUDE", "", "colon-separated mesh exclusion roots (empty for none)")
	bind(&f.machineName, "machine-name", "AMESH_MACHINE_NAME", "", "enrolled machine name")
	bind(&f.hubURL, "hub-url", "AMESH_HUB_URL", "https://nanoclaw.opx.one/peer", "hub API base including /peer")
	return f
}

func absolute(s string) (string, error) {
	if s == "~" || strings.HasPrefix(s, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		s = filepath.Join(home, strings.TrimPrefix(strings.TrimPrefix(s, "~"), "/"))
	}
	if !filepath.IsAbs(s) || strings.ContainsRune(s, 0) {
		return "", errors.New("expected an absolute path or ~/ path")
	}
	return filepath.Clean(s), nil
}

func paths(s string) ([]string, error) {
	values := filepath.SplitList(s)
	if len(values) == 0 {
		return nil, errors.New("empty directory list")
	}
	for i, value := range values {
		p, err := absolute(value)
		if err != nil {
			return nil, err
		}
		values[i] = p
	}
	return values, nil
}

func (f *Flags) SocketPath() (string, error) {
	if f.socketPath != "" {
		return absolute(f.socketPath)
	}
	dir, err := absolute(f.stateDir)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "amesh.sock"), nil
}

var machineName = regexp.MustCompile("^[a-z0-9][a-z0-9-]{0,15}$")

func (f *Flags) Resolve() (Config, error) {
	c := Config{MachineName: f.machineName, HubURL: f.hubURL}
	var err error
	if c.StateDir, err = absolute(f.stateDir); err != nil {
		return Config{}, fmt.Errorf("state-dir: %w", err)
	}
	if c.SocketPath, err = f.SocketPath(); err != nil {
		return Config{}, fmt.Errorf("socket-path: %w", err)
	}
	if c.ClaudeConfigDirs, err = paths(f.configDirs); err != nil {
		return Config{}, fmt.Errorf("claude-config-dirs: %w", err)
	}
	if f.exclude != "" {
		if c.ExcludeRoots, err = paths(f.exclude); err != nil {
			return Config{}, fmt.Errorf("exclude: %w", err)
		}
	}
	if c.MachineName != "" && !machineName.MatchString(c.MachineName) {
		return Config{}, errors.New("invalid machine-name")
	}
	u, err := url.Parse(c.HubURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return Config{}, errors.New("hub-url must be an HTTP(S) base URL without credentials, query or fragment")
	}
	c.HubURL = strings.TrimRight(c.HubURL, "/")
	return c, nil
}
