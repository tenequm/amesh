package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/tenequm/amesh/internal/config"
	"github.com/tenequm/amesh/internal/control"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/lane"
	"github.com/tenequm/amesh/internal/mesh"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
	"github.com/tenequm/amesh/internal/state"
	"github.com/tenequm/amesh/internal/version"
	"golang.org/x/term"
)

func main() {
	handler := slog.Handler(slog.NewJSONHandler(os.Stderr, nil))
	if term.IsTerminal(int(os.Stderr.Fd())) {
		handler = slog.NewTextHandler(os.Stderr, nil)
	}
	slog.SetDefault(slog.New(handler))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		slog.Error("amesh", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, in io.Reader, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("amesh", flag.ContinueOnError)
	fs.SetOutput(stderr)
	flags := config.Bind(fs)
	showVersion := fs.Bool("version", false, "print the build version")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: amesh [flags] [daemon | mcp [--parent ID] | hook | status | enroll]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	command, rest := "daemon", fs.Args()
	if len(rest) > 0 {
		command, rest = rest[0], rest[1:]
	}
	if command != "mcp" {
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("unexpected trailing argument")
		}
	}
	if *showVersion {
		_, err := fmt.Fprintln(out, version.Value)
		return err
	}
	switch command {
	case "mcp", "hook", "status":
		socket, err := flags.SocketPath()
		if command == "hook" {
			if err == nil {
				_ = control.Hook(ctx, socket, in, out)
			}
			return nil
		}
		if err != nil {
			return err
		}
		if command == "mcp" {
			return control.Pipe(ctx, socket, rest, in, out)
		}
		return control.Status(ctx, socket, out)
	case "daemon", "enroll":
		cfg, err := flags.Resolve()
		if err != nil {
			return err
		}
		if command == "enroll" {
			return hub.Enroll(ctx, cfg, out)
		}
		slog.Info("starting daemon", "version", version.Value)
		return daemon(ctx, cfg)
	default:
		return errors.New("unknown command (use -help)")
	}
}

func daemon(ctx context.Context, cfg config.Config) error {
	if err := envelope.SelfCheck(); err != nil {
		return fmt.Errorf("native envelope startup check: %w", err)
	}
	store := state.New(cfg.StateDir)
	if err := store.Lock(); err != nil {
		return err
	}
	defer store.Close()
	sessions := registry.New(cfg.ClaudeConfigDirs)
	runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = os.Getenv("CLAUDE_CODE_TMPDIR")
	}
	if runtimeDir == "" {
		runtimeDir = "/tmp"
	}
	relays := relay.New(filepath.Join(runtimeDir, "cc-socks"), store)
	ctx, cancel := context.WithCancel(ctx)
	laneBackend := lane.New(ctx, sessions, relays, cfg.StateDir)
	defer func() { cancel(); laneBackend.Wait() }()
	meshBackend := mesh.New(cfg, sessions, relays, hub.New(cfg))
	return control.Serve(ctx, cfg, sessions, laneBackend, meshBackend, relays.OwnsRelay)
}
