package mesh

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"time"

	"github.com/tenequm/amesh/internal/codex"
	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/envelope"
	"github.com/tenequm/amesh/internal/mcpserver"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/relay"
)

type threadDelivery interface {
	Status(string) string
	Watch(context.Context)
	Deliver(context.Context, string, string, string, func() bool) (bool, error)
}

type replyLink struct {
	sender, receiver, address, endpoint string
	expires                             time.Time
	digest                              [32]byte
	answer                              string
}

// fallbackReply marks a send that carries a Codex turn's final answer back to its sender.
type fallbackReply struct{}

// linkReply records where an answer to d goes. When armed, the receiving thread's final answer is sent back
// if its turn ends without send_message; a fallback answer is never armed, so two threads cannot keep answering.
func (b *Backend) linkReply(d relay.Delivery, receiver, sender string, armed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for id, l := range b.links {
		if !now.Before(l.expires) {
			delete(b.links, id)
		}
	}
	l := replyLink{sender: sender, receiver: receiver, address: d.ReplyAddress, endpoint: d.ReplyTo, expires: now.Add(24 * time.Hour)}
	if armed {
		content, _ := d.ThreadText()
		l.digest = sha256.Sum256([]byte(content))
	}
	b.links[d.ID] = l
}

func (b *Backend) reply(ctx context.Context, s registry.Session, args mcpserver.SendArgs) (replyLink, bool) {
	id := value(args.ReplyTo)
	metaID := mcpserver.CodexCall(ctx).Message
	b.mu.Lock()
	defer b.mu.Unlock()
	if s.Codex && metaID != "" && (id == "" || b.links[metaID].endpoint == id) {
		id = metaID
	}
	l, ok := b.links[id]
	if !ok && id != "" {
		// A hub delivery's trailer names the conversation endpoint, so match its newest armed link.
		for linkID, candidate := range b.links {
			if candidate.receiver == s.MeshKey() && candidate.endpoint == id && candidate.digest != ([32]byte{}) && (!ok || candidate.expires.After(l.expires)) {
				l, ok, id = candidate, true, linkID
			}
		}
	}
	if !ok || l.receiver != s.MeshKey() || !matches(normalizeAddress(args.To), l.address) || !time.Now().Before(l.expires) {
		return replyLink{}, false
	}
	l.digest = [32]byte{}
	l.answer = ""
	b.links[id] = l
	return l, true
}

func (b *Backend) completedThread(thread string, input [32]byte, answer string) {
	key := (registry.Session{Codex: true, SessionID: thread}).MeshKey()
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, l := range b.links {
		if l.receiver == key && l.digest == input && time.Now().Before(l.expires) {
			l.answer = answer
			l.digest = [32]byte{}
			b.links[id] = l
			b.changed()
			return
		}
	}
}

func (b *Backend) flushThreadReplies(ctx context.Context) {
	b.mu.Lock()
	replies := map[string]replyLink{}
	for id, l := range b.links {
		if l.answer != "" {
			replies[id] = l
			l.answer = ""
			b.links[id] = l
		}
	}
	b.mu.Unlock()
	for id, l := range replies {
		s, live, err := b.liveSession(ctx, l.receiver)
		if err == nil && live {
			_, _ = b.SendMessage(context.WithValue(ctx, fallbackReply{}, true), s, mcpserver.SendArgs{To: l.address, Message: l.answer, ReplyTo: &id})
		}
	}
}

func (b *Backend) writeThread(ctx context.Context, s registry.Session, d relay.Delivery) (bool, error) {
	content, err := d.ThreadText()
	if err != nil {
		return false, err
	}
	kind := "message"
	if d.Notice {
		kind = "notice"
	}
	app := b.threads[s.ConfigDir]
	if app == nil {
		return false, errors.New("unknown Codex home")
	}
	claimed, err := b.store.BeginWrite(d.Log, d.ID, d.Session, time.Now().Add(relay.Retention), kind)
	if !claimed || err != nil {
		return false, err
	}
	queued, err := app.Deliver(ctx, s.SessionID, d.ID, content, func() bool {
		current, ok, e := b.liveSession(ctx, s.MeshKey())
		return ok && e == nil && current.Key == s.Key && current.ConfigDir == s.ConfigDir
	})
	if err != nil {
		if !errors.Is(err, codex.ErrUnconfirmed) {
			err = errors.Join(err, b.store.ForgetWrite(d.Log, d.ID))
		}
		return false, err
	}
	if err = b.store.MarkWritten(d.Log, d.ID); err != nil {
		return queued, errors.Join(codex.ErrUnconfirmed, err)
	}
	return queued, nil
}

func (b *Backend) nativeThreadAddress(s registry.Session) (string, error) {
	key := s.MeshKey()
	b.endpointMu.Lock()
	defer b.endpointMu.Unlock()
	if _, live := b.member(key); !live {
		return "", os.ErrNotExist
	}
	if ep, ok := b.endpoints[key]; ok {
		return ep.from, nil
	}
	// A thread's native relay must exist without a hub, so the state directory stands in for the machine ID.
	path, err := relay.Path(b.socketDir, "amesh-relay\n"+b.cfg.StateDir, key)
	if err != nil {
		return "", err
	}
	listener, err := b.relays.Listen(b.ctx, path, func(ctx context.Context, e relay.Event) error {
		if e.Action != "user" {
			return nil
		}
		sender, resolveErr := b.sessions.Resolve(ctx, e.Peer.PID)
		if resolveErr != nil {
			return resolveErr
		}
		target, ok := b.member(key)
		if !ok || sender.PID != e.Peer.PID || sender.Codex || sender.SocketPath == "" || envelope.Address(sender.SocketPath) != e.From {
			return os.ErrPermission
		}
		result, sendErr := b.SendMessage(ctx, sender, mcpserver.SendArgs{To: target.address, Message: e.Message.Body})
		if sendErr == nil && !result.Accepted {
			return errors.New("native Codex reply refused")
		}
		return sendErr
	})
	if err != nil {
		return "", err
	}
	from := envelope.Address(path)
	b.endpoints[key] = endpoint{ref: contract.EndpointRef{ID: key}, from: from, listener: listener}
	return from, nil
}
