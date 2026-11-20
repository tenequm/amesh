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
	"github.com/tenequm/amesh/internal/hub"
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
	if err := os.Mkdir(socketDir, 0o700); err != nil && !errors.Is(err, os.ErrExist) { // #nosec G703 -- socketDir comes from daemon startup configuration.
		return nil, errors.Join(err, store.Close())
	}
	info, err := os.Lstat(socketDir) // #nosec G703 -- the daemon supplies its native socket directory before accepting requests.
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
	live, err := b.sessions.Snapshot(parent)
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	versions := map[string]bool{}
	for _, s := range live.Sessions {
		if s.Version != "" && b.eligible(s) {
			versions[s.Version] = true
		}
	}
	hello := contract.HelloRequest{
		Amesh:        contract.HelloRequestAmesh{Version: version.Value, OS: runtime.GOOS, Arch: runtime.GOARCH},
		ClaudeCode:   contract.HelloRequestClaudeCode{Versions: slices.Sorted(maps.Keys(versions)), PeerProtocol: 1, NativeCheck: "ok"},
		Capabilities: []string{"receipts", "human-turn"},
	}
	if hello.ClaudeCode.Versions == nil {
		hello.ClaudeCode.Versions = []string{}
	}
	ctx, cancel := context.WithCancel(parent)
	b.ctx = ctx
	var workers sync.WaitGroup
	if watcher, ok := b.threads.(interface{ Watch(context.Context) }); ok {
		workers.Go(func() { watcher.Watch(ctx) })
	}
	pendingByLog := store.PendingRewrites()
	b.resumeRewrites = func(log string) {
		pending := pendingByLog[log]
		if len(pending) == 0 {
			return
		}
		workers.Go(func() {
			b.relays.ResumeRewrites(ctx, pending, func(ctx context.Context, key string, pid int, path string) bool {
				s, ok, err := b.liveSession(ctx, key)
				return err == nil && ok && s.PID == pid && s.SocketPath == path
			})
		})
	}
	// Lane rewrites have no hub log and must work offline.
	b.resumeRewrites("")
	workers.Go(func() {
		if _, halt := b.client.Connection(); errors.Is(halt, hub.ErrNoHub) {
			b.runPresence(ctx)
			return
		}
		_, helloErr := b.client.Hello(ctx, hello)
		b.connectivity(helloErr)
		workers.Go(func() { b.runPresence(ctx) })
		workers.Go(func() { b.runUploader(ctx) })
		if streamErr := b.stream(ctx); streamErr != nil && ctx.Err() == nil {
			b.connectivity(streamErr)
			slog.Warn("mesh stream stopped", "error", streamErr)
		}
	})
	return sync.OnceValue(func() error { cancel(); workers.Wait(); b.closeEndpoints(); return store.Close() }), nil
}

// Expiry notices still need the public machine id when a restarted daemon
// cannot complete hello. The outbox and the delivered-id store own their log ids.
func (b *Backend) rememberMachine(id string) error {
	if id == "" {
		return errors.New("missing hub machine id")
	}
	root, err := os.OpenRoot(b.cfg.StateDir)
	if err != nil {
		return err
	}
	defer root.Close()
	const path = "mesh-machine"
	const pending = "mesh-machine.pending"
	prior, err := root.ReadFile(path)
	if err == nil && string(prior) == id {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := root.OpenFile(pending, os.O_RDWR|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(pending) }()
	_, err = f.WriteString(id)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	return root.Rename(pending, path)
}

func (b *Backend) runUploader(ctx context.Context) {
	for ctx.Err() == nil {
		conn, halt := b.client.Connection()
		var err error
		if conn.Log != "" && halt == nil {
			err = b.prepareLog(ctx, conn)
		}
		if err == nil {
			err = b.client.Drain(ctx, b.reregister, b.completed)
		}
		if ctx.Err() != nil {
			return
		}
		if _, halt = b.client.Connection(); halt != nil {
			return
		}
		slog.Warn("mesh outbox stopped", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
