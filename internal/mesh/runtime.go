package mesh

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/state"
	"github.com/tenequm/amesh/internal/version"
)

func (b *Backend) Start(parent context.Context, store *state.Store, socketDir string) (func() error, error) {
	if err := store.Lock(); err != nil {
		return nil, err
	}
	if err := b.sessions.SelfCheck(parent); err != nil {
		return nil, errors.Join(fmt.Errorf("registry startup check: %w", err), store.Close())
	}
	if err := envelope.SelfCheck(); err != nil {
		return nil, errors.Join(fmt.Errorf("native envelope startup check: %w", err), store.Close())
	}
	info, err := os.Lstat(socketDir)
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	if !info.IsDir() || int(info.Sys().(*syscall.Stat_t).Uid) != os.Geteuid() {
		return nil, errors.Join(errors.New("native socket directory has unexpected type or owner"), store.Close())
	}
	b.store = store
	b.socketDir = socketDir
	if err = b.restore(parent); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	live, err := b.sessions.List(parent)
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	versions := map[string]bool{}
	for _, s := range live {
		if s.Version != "" && b.eligible(s) {
			versions[s.Version] = true
		}
	}
	hello := contract.HelloRequest{
		Amesh:        contract.HelloRequestAmesh{Version: version.Value, OS: runtime.GOOS, Arch: runtime.GOARCH},
		ClaudeCode:   contract.HelloRequestClaudeCode{Versions: slices.Sorted(maps.Keys(versions)), PeerProtocol: 1, NativeCheck: "ok"},
		Capabilities: []string{"receipts", "idle", "human-turn"},
	}
	if hello.ClaudeCode.Versions == nil {
		hello.ClaudeCode.Versions = []string{}
	}
	ctx, cancel := context.WithCancel(parent)
	b.ctx = ctx
	var workers sync.WaitGroup
	workers.Go(func() {
		_, helloErr := b.client.Hello(ctx, hello)
		b.connectivity(helloErr)
		workers.Go(func() { b.runPresence(ctx) })
		workers.Go(func() { b.runUploader(ctx) })
		if streamErr := b.stream(ctx); streamErr != nil && ctx.Err() == nil {
			b.connectivity(streamErr)
			slog.Warn("mesh stream stopped", "error", streamErr)
		}
	})
	return sync.OnceValue(func() error { cancel(); workers.Wait(); b.closeEndpoints(); b.retries.Wait(); return store.Close() }), nil
}

func (b *Backend) runUploader(ctx context.Context) {
	for ctx.Err() == nil {
		conn, halt := b.client.Connection()
		if halt != nil {
			return
		}
		if conn.Log != "" {
			if err := b.prepareLog(ctx, conn); err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Warn("mesh outbox awaits delivery recovery", "error", err)
			} else {
				if err = b.client.RunOutbox(ctx, b.completed); err != nil && ctx.Err() == nil {
					slog.Warn("mesh outbox stopped", "error", err)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
