package mesh

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/hub"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
)

const label = "[a-z0-9][a-z0-9._-]{0,39}"

var address = regexp.MustCompile("^" + label + "(@" + label + "(:[a-z0-9][a-z0-9-]{0,15})?)?$")

func value(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (b *Backend) connectivity(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.offlineSince = time.Time{}
	} else if b.offlineSince.IsZero() {
		b.offlineSince = time.Now()
	}
}

func (b *Backend) directory(ctx context.Context) []contract.Peer {
	b.mu.RLock()
	cached := slices.Clone(b.peers)
	revision := b.directoryVersion
	b.mu.RUnlock()
	if len(cached) > 0 {
		return cached
	}
	res, err := b.client.Peers(ctx)
	b.connectivity(err)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil && revision == b.directoryVersion {
		b.peers = res.Peers
		b.directoryVersion++
	}
	return slices.Clone(b.peers)
}

func haltReason(err error) string {
	reason := err.Error()
	if h, ok := errors.AsType[*hub.HTTPError](err); ok && h.Body.Message != "" {
		reason = h.Body.Message
		if h.Body.Error == "machine_revoked" {
			reason = "this machine was removed from the mesh."
		}
	}
	return "amesh on this machine is not connected to the hub: " + reason
}

func (b *Backend) ListPeers(ctx context.Context, s registry.Session, filter string) (mcpserver.Directory, error) {
	conn, halt := b.client.Connection()
	machine := conn.Machine.Name
	if machine == "" {
		machine = b.cfg.MachineName
	}
	m, _ := b.member(sessionKey(s.Key))
	name := m.address
	if name == "" {
		name = s.Name + "@" + s.Project + ":" + machine
	}
	out := mcpserver.Directory{You: mcpserver.Identity{Name: name, Project: s.Project, Machine: machine}, Peers: []mcpserver.Peer{}}
	project, host, _ := strings.Cut(strings.TrimPrefix(filter, "@"), ":")
	for _, p := range b.directory(ctx) {
		if p.Kind == "session" && value(p.Machine) == machine || filter != "" && (value(p.Project) != project || host != "" && value(p.Machine) != host) {
			continue
		}
		row := mcpserver.Peer{Name: p.Address, Kind: p.Kind, Machine: p.Machine, Project: p.Project, Status: p.Status}
		if p.LastSeen != nil {
			stamp := time.UnixMilli(*p.LastSeen).UTC().Format(time.RFC3339)
			row.LastSeen = &stamp
		}
		out.Peers = append(out.Peers, row)
	}
	b.mu.RLock()
	since := b.offlineSince
	b.mu.RUnlock()
	if halt != nil {
		out.Stale = haltReason(halt)
	} else if !since.IsZero() {
		out.Stale = "The hub has been unreachable since " + since.UTC().Format("15:04") + " UTC; statuses may be old."
	}
	return out, nil
}

func refused(reason string, known []string) mcpserver.SendResult {
	if known == nil {
		known = []string{}
	}
	return mcpserver.SendResult{Reason: reason, Known: &known}
}

func matches(input, full string) bool {
	base, _, qualified := strings.Cut(full, ":")
	return input == full || qualified && input == base
}

func (b *Backend) reregister(ctx context.Context, key string) error {
	b.presence.Lock()
	defer b.presence.Unlock()
	if _, ok := b.member(key); !ok {
		return errors.New("session is no longer a mesh member")
	}
	return b.register(ctx, key)
}

func unknownSession(err error) bool {
	h, ok := errors.AsType[*hub.HTTPError](err)
	return ok && h.Status == 404 && h.Body.Error == "unknown_session"
}

func (b *Backend) send(ctx context.Context, req contract.SendRequest) (contract.SendResponse, error) {
	response, err := b.client.Send(ctx, req)
	if unknownSession(err) {
		if err = b.reregister(ctx, req.From.Session); err == nil {
			return b.client.Send(ctx, req)
		}
	}
	return response, err
}

func (b *Backend) localTarget(to string) (member, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, m := range b.members {
		if m.hosted && m.address != "" && matches(to, m.address) {
			return m, true
		}
	}
	return member{}, false
}

func (b *Backend) liveSession(ctx context.Context, key string) (registry.Session, bool, error) {
	m, ok := b.member(key)
	if !ok {
		return registry.Session{}, false, nil
	}
	s, err := b.sessions.Resolve(ctx, m.session.PID)
	if errors.Is(err, os.ErrNotExist) {
		return registry.Session{}, false, nil
	}
	if err != nil {
		return registry.Session{}, false, err
	}
	return s, s.Key == m.session.Key && b.eligible(s), nil
}

func (b *Backend) sendLocal(ctx context.Context, s registry.Session, target member, message string) (mcpserver.SendResult, error) {
	receiver, ok, err := b.liveSession(ctx, sessionKey(target.session.Key))
	if err != nil {
		return mcpserver.SendResult{}, err
	}
	if !ok {
		return refused(target.address+" is not running anymore.", nil), nil
	}
	id := uuid.NewString()
	err = b.relays.Write(ctx, relay.Delivery{ID: id, Inbox: receiver.SocketPath, Message: envelope.Message{From: envelope.Address(s.SocketPath), FromName: s.Name, FromMode: s.FromMode, Body: message}})
	if err != nil {
		return refused("The message could not be written to "+target.address+".", nil), nil //nolint:nilerr // MCP delivery refusals are ordinary tool results.
	}
	return mcpserver.SendResult{Accepted: true, ID: id, To: target.address, Note: "Sent. A reply, if any, arrives later as a message from " + target.address + "."}, nil
}

func (b *Backend) SendMessage(ctx context.Context, s registry.Session, args mcpserver.SendArgs) (mcpserver.SendResult, error) {
	conn, halt := b.client.Connection()
	if halt != nil {
		return refused(haltReason(halt), nil), nil
	}
	key := sessionKey(s.Key)
	m, ok := b.member(key)
	if !ok {
		return refused("This session is not a mesh member.", nil), nil
	}
	to := args.To
	text := strings.TrimSpace(args.Message)
	switch {
	case !address.MatchString(to) || matches(to, m.address):
		return refused("Choose a peer address other than this session.", nil), nil
	case text == "":
		return refused("Enter a message.", nil), nil
	case strings.HasPrefix(text, "/"):
		return refused("Slash commands cannot be sent.", nil), nil
	case int64(len(args.Message)) > conn.Limits.MaxTextBytes:
		limit := fmt.Sprintf("%d bytes", conn.Limits.MaxTextBytes)
		if conn.Limits.MaxTextBytes%1024 == 0 {
			limit = fmt.Sprintf("%d KiB", conn.Limits.MaxTextBytes/1024)
		}
		return refused("The message is longer than "+limit+". Shorten it or split it.", nil), nil
	}
	if target, found := b.localTarget(to); found {
		return b.sendLocal(ctx, s, target, args.Message)
	}
	peers, err := b.ListPeers(ctx, s, "")
	if err != nil {
		return mcpserver.SendResult{}, err
	}
	var known, candidates []string
	for _, p := range peers.Peers {
		known = append(known, p.Name)
		if matches(to, p.Name) {
			candidates = append(candidates, p.Name)
		}
	}
	switch len(candidates) {
	case 0:
		return refused(fmt.Sprintf("No peer named %q.", to), known), nil
	case 1:
	default:
		return refused(fmt.Sprintf("%q matches more than one session. Use the full address.", to), candidates), nil
	}
	id := uuid.NewString()
	req := contract.SendRequest{ID: id, From: contract.SendRequestFrom{Session: key}, To: contract.SendRequestTo{Address: &to}, Text: args.Message, FromMode: optional(s.FromMode), Origin: "unknown"}
	res, err := b.send(ctx, req)
	out := mcpserver.SendResult{Accepted: true, ID: id, To: candidates[0]}
	if errors.Is(err, hub.ErrQueued) {
		out.Note = "Sent from this machine. It reaches the hub as soon as the connection is back, within 24 hours."
		if h, ok := errors.AsType[*hub.HTTPError](err); ok && h.Status == 429 {
			out.Note = "Sending is paced. This machine will retry automatically."
		}
		return out, nil
	}
	if err != nil {
		if _, halt = b.client.Connection(); halt != nil {
			return refused(haltReason(halt), nil), nil
		}
		if h, ok := errors.AsType[*hub.HTTPError](err); ok {
			var options []string
			if h.Body.Error == "not_found" {
				options = known
			} else if h.Body.Candidates != nil {
				options = *h.Body.Candidates
			}
			return refused(h.Body.Message, options), nil
		}
		return refused("The message could not be queued on this machine.", nil), nil
	}
	out.To = res.Message.To.Address
	out.Note = "Sent. A reply, if any, arrives later as a message from " + out.To + "."
	if res.Message.To.Status == "offline" {
		out.Note = out.To + " is offline. The message waits up to 24 hours and is delivered when it is back."
	}
	return out, nil
}
