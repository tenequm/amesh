package mesh

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/state"
)

type member struct {
	session       registry.Session
	address       string
	pipes         int
	hosted, dirty bool
	last          state.Registration
}

func sessionKey(k registry.Key) string {
	digest := sha256.Sum256([]byte(k.ProcStart))
	return fmt.Sprintf("%d-%x", k.PID, digest[:4])
}

func (b *Backend) eligible(s registry.Session) bool {
	return !s.MeshExcluded(b.cfg.ClaudeConfigDirs, b.cfg.ExcludeRoots)
}

func (b *Backend) member(key string) (member, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.members[key]
	return m, ok && m.hosted
}

func (b *Backend) IsMember(key registry.Key) bool { _, ok := b.member(sessionKey(key)); return ok }

func (b *Backend) persist(key string, m member) error {
	return b.store.PutMember(state.Member{Key: key, PID: m.session.PID, ProcStart: m.session.ProcStart, Address: m.address, Registration: m.last, ConfigDir: m.session.ConfigDir})
}

func (b *Backend) restore(ctx context.Context) error {
	saved, err := b.store.Members()
	if err != nil {
		return err
	}
	live, err := b.sessions.List(ctx)
	if err != nil {
		return err
	}
	byKey := map[registry.Key]registry.Session{}
	for _, s := range live {
		byKey[s.Key] = s
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, row := range saved {
		key := registry.Key{PID: row.PID, ProcStart: row.ProcStart}
		if row.Key != sessionKey(key) {
			return errors.New("invalid persisted mesh session key")
		}
		s, ok := byKey[key]
		if !ok {
			s.Key = key
			s.ConfigDir = row.ConfigDir
		}
		b.members[row.Key] = member{session: s, address: row.Address, last: row.Registration, hosted: ok && b.eligible(s)}
	}
	return nil
}

type pipe struct{ close func() }

func (p pipe) Close() error { p.close(); return nil }

func (b *Backend) changed() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *Backend) Connect(ctx context.Context, s registry.Session) (io.Closer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !b.eligible(s) || b.store == nil {
		return nil, errors.New("mesh unavailable for session")
	}
	key := sessionKey(s.Key)
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.members[key]
	m.session = s
	m.hosted = true
	m.dirty = true
	m.pipes++
	if err := b.persist(key, m); err != nil {
		return nil, err
	}
	b.members[key] = m
	b.changed()
	return pipe{sync.OnceFunc(func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		v, ok := b.members[key]
		if !ok || v.pipes == 0 {
			return
		}
		v.pipes--
		b.members[key] = v
		b.changed()
	})}, nil
}

func status(s registry.Session) string {
	if s.Status == "idle" {
		return "idle"
	}
	return "busy"
}

func registration(s registry.Session) (contract.SessionRegisterRequest, error) {
	req := contract.SessionRegisterRequest{SessionID: s.SessionID, Name: s.Name, Project: s.Project, Status: status(s), ClaudeCode: s.Version}
	// These registration attributes are absent from the identity package's process view.
	if s.ConfigDir != "" {
		raw, err := os.ReadFile(filepath.Join(s.ConfigDir, "sessions", strconv.Itoa(s.PID)+".json")) // #nosec G304 -- configured registry and numeric PID
		if err != nil {
			return req, err
		}
		var meta struct {
			PID        int    `json:"pid"`
			ProcStart  string `json:"procStart"`
			SessionID  string `json:"sessionId"`
			NameSource string `json:"nameSource"`
			Entrypoint string `json:"entrypoint"`
			StartedAt  int64  `json:"startedAt"`
		}
		if err = json.Unmarshal(raw, &meta); err != nil {
			return req, err
		}
		if meta.PID != s.PID || meta.ProcStart != s.ProcStart || meta.SessionID != s.SessionID {
			return req, errors.New("session changed during registration")
		}
		req.NameSource, req.Entrypoint, req.StartedAt = meta.NameSource, meta.Entrypoint, meta.StartedAt
	}
	return req, nil
}

func registeredFields(log string, req contract.SessionRegisterRequest) state.Registration {
	return state.Registration{Log: log, SessionID: req.SessionID, Name: req.Name, NameSource: req.NameSource, Project: req.Project}
}

func (b *Backend) register(ctx context.Context, key string) error {
	m, ok := b.member(key)
	if !ok {
		return nil
	}
	req, err := registration(m.session)
	if err != nil {
		return err
	}
	conn, _ := b.client.Connection()
	resp, err := b.client.Register(ctx, key, req)
	if err != nil {
		return err
	}
	if resp.Session.Key != key || resp.Session.Address == "" {
		return errors.New("invalid hub registration")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.members[key]
	current.address = resp.Session.Address
	current.last = registeredFields(conn.Log, req)
	current.dirty = false
	if err = b.persist(key, current); err != nil {
		return err
	}
	b.members[key] = current
	return nil
}

func (b *Backend) end(ctx context.Context, key string, reason contract.SessionEndRequestReason) error {
	n, err := b.client.Pending(key)
	if err != nil || n > 0 {
		return err
	}
	_, err = b.client.End(ctx, key, contract.SessionEndRequest{Reason: reason})
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if m := b.members[key]; m.hosted {
		return nil
	}
	if err = b.store.DeleteMember(key); err == nil {
		delete(b.members, key)
	}
	return err
}

func (b *Backend) reconcile(ctx context.Context) error {
	b.presence.Lock()
	defer b.presence.Unlock()
	conn, err := b.client.Connection()
	if err != nil || conn.Epoch == 0 {
		return err
	}
	live, err := b.sessions.List(ctx)
	if err != nil {
		return err
	}
	byKey := map[registry.Key]registry.Session{}
	for _, s := range live {
		byKey[s.Key] = s
	}
	b.mu.RLock()
	keys := slices.Sorted(maps.Keys(b.members))
	b.mu.RUnlock()
	changed := b.presenceLog != conn.Log || b.heartbeatDue
	for _, key := range keys {
		b.mu.Lock()
		m := b.members[key]
		s, found := byKey[m.session.Key]
		m.hosted = found && b.eligible(s) || !found && m.pipes > 0
		if found {
			changed = changed || status(m.session) != status(s)
			m.session = s
		}
		b.members[key] = m
		b.mu.Unlock()
		if !m.hosted {
			reason := contract.SessionEndRequestReasonExited
			if found || m.session.ConfigDir != "" && !b.eligible(m.session) {
				reason = contract.SessionEndRequestReasonExcluded
			}
			if err = b.end(ctx, key, reason); err != nil {
				return err
			}
			changed = true
			continue
		}
		if !found {
			continue
		}
		req, readErr := registration(s)
		if readErr != nil {
			return readErr
		}
		if m.dirty || registeredFields(conn.Log, req) != m.last {
			if err = b.register(ctx, key); err != nil {
				return err
			}
			changed = true
		}
	}
	b.heartbeatDue = changed
	now := time.Now()
	if !b.lastHeartbeat.IsZero() && time.Since(b.lastHeartbeat) < time.Duration(conn.Settings.HeartbeatMs)*time.Millisecond && (!changed || time.Since(b.lastHeartbeat) < time.Second) {
		return nil
	}
	b.mu.RLock()
	req := contract.HeartbeatRequest{}
	for _, key := range slices.Sorted(maps.Keys(b.members)) {
		req.Sessions = append(req.Sessions, contract.HeartbeatRequestSessionsItem{Key: key, Status: status(b.members[key].session)})
	}
	b.mu.RUnlock()
	res, err := b.client.Heartbeat(ctx, req)
	b.connectivity(err)
	if err != nil {
		return err
	}
	b.heartbeatDue = false
	b.lastHeartbeat = now
	b.presenceLog = conn.Log
	for _, key := range append(res.Unknown, res.Ended...) {
		if err = b.register(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) runPresence(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for ctx.Err() == nil {
		if err := b.reconcile(ctx); err != nil && ctx.Err() == nil {
			if _, halt := b.client.Connection(); halt != nil {
				return
			}
			slog.Warn("mesh presence update failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-b.wake:
		}
	}
}

func (b *Backend) Status(s *contract.DaemonStatus) error {
	if err := b.client.Status(s); err != nil {
		return err
	}
	open, err := b.store.OpenEntries("")
	if err != nil {
		return err
	}
	for _, e := range open {
		if e.Phase == state.Written {
			s.Open.Count++
		}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, key := range slices.Sorted(maps.Keys(b.members)) {
		m := b.members[key]
		if m.hosted {
			s.Sessions = append(s.Sessions, contract.DaemonStatusSessionsItem{Key: key, Address: m.address, Status: contract.PeerStatus(status(m.session)), Role: "member"})
		}
	}
	return nil
}
