package mesh

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/state"
)

type member struct {
	humanTurn     string
	session       registry.Session
	address       string
	pipes         int
	hosted, dirty bool
	last          state.Registration
}

func (b *Backend) eligible(s registry.Session) bool {
	return !s.MeshExcluded(b.cfg.ExcludeRoots)
}

func (b *Backend) member(key string) (member, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.members[key]
	return m, ok && m.hosted
}

func (b *Backend) IsMember(key registry.Key) bool { _, ok := b.member(key.MeshKey()); return ok }

func (b *Backend) persist(key string, m member) error {
	thread := ""
	if m.session.Codex {
		thread = m.session.SessionID
	}
	return b.store.PutMember(state.Member{ThreadID: thread, Key: key, PID: m.session.PID, ProcStart: m.session.ProcStart, Address: m.address, Registration: m.last, ConfigDir: m.session.ConfigDir})
}

func (b *Backend) restore(ctx context.Context) error {
	saved, err := b.store.Members()
	if err != nil {
		return err
	}
	live, err := b.sessions.Snapshot(ctx)
	if err != nil {
		return err
	}
	byKey := map[registry.Key]registry.Session{}
	for _, s := range live.Sessions {
		byKey[s.Key] = s
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, row := range saved {
		key := registry.Key{PID: row.PID, ProcStart: row.ProcStart}
		s, ok := byKey[key]
		if row.ThreadID != "" {
			// A Codex thread is hosted again only once its pipe reconnects and proves the thread.
			s = registry.Session{Key: key, Codex: true, SessionID: row.ThreadID}
			ok = false
		}
		if !ok {
			s.Key = key
			s.ConfigDir = row.ConfigDir
		}
		if row.Key != s.MeshKey() {
			return errors.New("invalid persisted mesh session key")
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
	key := s.MeshKey()
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.members[key]
	if m.session.Codex && m.session.SessionID != s.SessionID {
		return nil, errors.New("thread key collision")
	}
	if m.session.Key != s.Key {
		m.pipes = 0
	}
	m.session = s
	if _, err := b.client.Connection(); errors.Is(err, hub.ErrNoHub) && m.address == "" {
		m.address = b.localAddress(s)
	}
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
		v, ok := b.members[key]
		if !ok || v.session.Key != s.Key || v.pipes == 0 {
			b.mu.Unlock()
			return
		}
		v.pipes--
		if s.Codex && v.pipes == 0 {
			v.hosted = false
		}
		b.members[key] = v
		b.mu.Unlock()
		if s.Codex && v.pipes == 0 {
			b.endpointMu.Lock()
			_, hosted := b.member(key)
			ep, exists := b.endpoints[key]
			if !hosted && exists {
				delete(b.endpoints, key)
			}
			b.endpointMu.Unlock()
			if !hosted && exists {
				_ = ep.listener.Close()
			}
		}
		b.changed()
	})}, nil
}

func (b *Backend) localAddress(s registry.Session) string {
	machine := b.machineName(contract.HelloResponse{})
	base := addressLabel(s.Name)
	suffix := "@" + addressLabel(s.Project) + ":" + machine
	occupied := map[string]bool{}
	for _, m := range b.members {
		if m.hosted && m.session.MeshKey() != s.MeshKey() {
			occupied[m.address] = true
		}
	}
	addr := base + suffix
	for i := 2; occupied[addr]; i++ {
		n := "-" + strconv.Itoa(i)
		addr = base[:min(len(base), maxLabel-len(n))] + n + suffix
	}
	return addr
}

func status(s registry.Session) contract.PeerStatus {
	if s.Status == string(contract.PeerStatusIdle) {
		return contract.PeerStatusIdle
	}
	return contract.PeerStatusBusy
}

func registration(s registry.Session) contract.SessionRegisterRequest {
	return contract.SessionRegisterRequest{SessionID: s.SessionID, Name: s.Name, NameSource: s.NameSource, Entrypoint: s.Entrypoint, StartedAt: s.StartedAt, Project: s.Project, Status: string(status(s)), ClaudeCode: s.Version}
}

func registeredFields(log string, req contract.SessionRegisterRequest) state.Registration {
	return state.Registration{Log: log, SessionID: req.SessionID, Name: req.Name, NameSource: req.NameSource, Project: req.Project}
}

func (b *Backend) register(ctx context.Context, key string, req contract.SessionRegisterRequest) error {
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
	m := b.members[key]
	if m.hosted {
		return nil
	}
	// A Codex thread can drop out of one scan while its pipe stays open (a newer sibling, a failed probe).
	// Keep it with no registration, so the next scan that sees it registers it again.
	if m.session.Codex && m.pipes > 0 {
		m.last = state.Registration{}
		b.members[key] = m
		return b.persist(key, m)
	}
	if err = b.store.DeleteMember(key); err == nil {
		delete(b.members, key)
		b.forgetLocalHops(key)
	}
	return err
}

func (b *Backend) reconcile(ctx context.Context) error {
	conn, err := b.client.Connection()
	local := errors.Is(err, hub.ErrNoHub)
	if !local && (err != nil || conn.Epoch == 0) {
		return err
	}
	if local {
		b.mu.RLock()
		empty := len(b.members) == 0
		b.mu.RUnlock()
		if empty {
			return nil
		}
	}
	live, err := b.sessions.Snapshot(ctx)
	if err != nil {
		return err
	}
	byKey := map[registry.Key]registry.Session{}
	for _, s := range live.Sessions {
		byKey[s.Key] = s
	}
	for _, s := range live.Sessions {
		if s.Codex {
			if e := b.HumanTurn(ctx, s); e != nil {
				slog.Warn("mesh human turn update failed", "error", e)
			}
		}
	}
	b.presence.Lock()
	defer b.presence.Unlock()
	b.mu.RLock()
	keys := slices.Sorted(maps.Keys(b.members))
	b.mu.RUnlock()
	if local {
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, key := range keys {
			m := b.members[key]
			s, ok := byKey[m.session.Key]
			if !ok && live.Unavailable(m.session) {
				continue
			}
			if ok && s.MeshKey() == key && b.eligible(s) {
				if m.session.Name != s.Name || m.session.Project != s.Project || m.address == "" {
					m.address = b.localAddress(s)
				}
				m.session = s
				m.hosted = true
				b.members[key] = m
			} else if m.pipes == 0 {
				if e := b.store.DeleteMember(key); e != nil {
					return e
				}
				delete(b.members, key)
				b.forgetLocalHops(key)
			} else {
				m.hosted = !ok && !m.session.Codex
				b.members[key] = m
			}
		}
		return nil
	}
	changed := b.presenceLog != conn.Log || b.heartbeatDue
	for _, key := range keys {
		b.mu.Lock()
		m := b.members[key]
		s, found := byKey[m.session.Key]
		found = found && s.MeshKey() == key
		if !found && live.Unavailable(m.session) {
			b.mu.Unlock()
			continue
		}
		m.hosted = found && b.eligible(s) || !found && !m.session.Codex && m.pipes > 0
		if found {
			changed = changed || status(m.session) != status(s)
			m.session = s
		}
		b.members[key] = m
		b.mu.Unlock()
		if !m.hosted && m.session.Codex && m.pipes > 0 && m.last == (state.Registration{}) {
			continue
		}
		if !m.hosted {
			reason := contract.SessionEndRequestReasonExited
			if found || m.session.CWD != "" && !b.eligible(m.session) {
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
		req := registration(s)
		if m.dirty || registeredFields(conn.Log, req) != m.last {
			if err = b.register(ctx, key, req); err != nil {
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
		req.Sessions = append(req.Sessions, contract.HeartbeatRequestSessionsItem{Key: key, Status: string(status(b.members[key].session))})
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
		m, hosted := b.member(key)
		s, found := byKey[m.session.Key]
		if !hosted || !found {
			continue
		}
		if err = b.register(ctx, key, registration(s)); err != nil {
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
			if _, halt := b.client.Connection(); halt != nil && !errors.Is(halt, hub.ErrNoHub) {
				return
			}
			slog.Warn("mesh presence update failed", "error", err)
		}
		b.flushThreadReplies(ctx)
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
			s.Sessions = append(s.Sessions, contract.DaemonStatusSessionsItem{Key: key, Address: m.address, Status: status(m.session), Role: "member"})
		}
	}
	return nil
}
