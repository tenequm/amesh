package relaytest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

// Inbox returns a short Unix socket path and one NDJSON line (without LF) per connection.
func Inbox(tb testing.TB) (string, <-chan []byte, error) {
	tb.Helper()
	dir, err := os.MkdirTemp("/tmp", "amesh-wire-a1-") //nolint:usetesting // macOS limits the whole Unix socket path to 104 bytes.
	if err != nil {
		return "", nil, err
	}
	tb.Cleanup(func() {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			tb.Error(removeErr)
		}
	})
	path := filepath.Join(dir, "amesh-wire-a1.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return "", nil, err
	}
	ctx, cancel := context.WithCancel(tb.Context())
	lines := make(chan []byte, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(lines)
		for {
			conn, err := ln.AcceptUnix()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					tb.Error(err)
				}
				return
			}
			stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
			data, err := io.ReadAll(io.LimitReader(conn, 4<<20))
			stop()
			_ = conn.Close()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				tb.Error(err)
				continue
			}
			if len(data) == 0 {
				continue
			}
			line, ok := bytes.CutSuffix(data, []byte("\n"))
			if !ok || bytes.ContainsRune(line, '\n') {
				tb.Error("expected one NDJSON line per connection")
				continue
			}
			select {
			case lines <- line:
			case <-ctx.Done():
				return
			}
		}
	}()
	tb.Cleanup(func() { cancel(); _ = ln.Close(); <-done })
	return path, lines, nil
}
