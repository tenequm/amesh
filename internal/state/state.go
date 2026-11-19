package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	recordFile = "delivered.json"
	maxClaims  = 32768
	maxID      = 128
)

type Phase string

const (
	Writing  Phase = "writing"
	Written  Phase = "written"
	Reported Phase = "reported"
)

type Member struct {
	Key       string `json:"key"`
	PID       int    `json:"pid"`
	ProcStart string `json:"procStart"`
	Address   string `json:"address"`
}

type Entry struct {
	Session string `json:"session,omitempty"`
	Phase   Phase  `json:"phase"`
	Expires int64  `json:"expires"`
}

type snapshot struct {
	Version int               `json:"version"`
	Log     string            `json:"log"`
	IDs     map[string]Entry  `json:"ids"`
	Members map[string]Member `json:"members"`
}

type Store struct {
	dir  string
	mu   sync.Mutex
	root *os.Root
	lock *os.File
	data snapshot
}

func New(dir string) *Store { return &Store{dir: dir} }

func (s *Store) Lock() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock != nil {
		return errors.New("state lock already held")
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return err
	}
	file, err := root.OpenFile("daemon.lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return errors.Join(err, root.Close())
	}
	if err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errors.Join(fmt.Errorf("lock daemon state: %w", err), file.Close(), root.Close())
	}
	next := snapshot{Version: 1, IDs: make(map[string]Entry), Members: make(map[string]Member)}
	data, err := root.ReadFile(recordFile)
	if err == nil {
		next, err = readSnapshot(data)
	} else if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return errors.Join(err, file.Close(), root.Close())
	}
	s.root, s.lock, s.data = root, file, next
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	err := errors.Join(s.lock.Close(), s.root.Close())
	s.root, s.lock, s.data = nil, nil, snapshot{}
	return err
}

func readSnapshot(raw []byte) (snapshot, error) {
	var data snapshot
	if err := json.Unmarshal(raw, &data); err != nil {
		return data, err
	}
	if data.Version == 0 {
		// Legacy claims contain no session key. Keep replay protection until they expire.
		var legacy map[string]int64
		if err := json.Unmarshal(raw, &legacy); err != nil || legacy == nil {
			return data, errors.New("invalid legacy delivered-id record")
		}
		data = snapshot{Version: 1, IDs: make(map[string]Entry, len(legacy)), Members: make(map[string]Member)}
		for id, expiry := range legacy {
			data.IDs[id] = Entry{Phase: Writing, Expires: expiry}
		}
	}
	if data.Version != 1 || data.IDs == nil || data.Members == nil || len(data.IDs) > maxClaims {
		return data, errors.New("invalid delivered-id record")
	}
	for id, entry := range data.IDs {
		if id == "" || len(id) > maxID || entry.Expires <= 0 || (entry.Phase != Writing && entry.Phase != Written && entry.Phase != Reported) {
			return data, errors.New("invalid delivered-id entry")
		}
	}
	for key, member := range data.Members {
		if key != member.Key || !member.valid() {
			return data, errors.New("invalid persisted member")
		}
	}
	return data, nil
}

func (s *Store) check(log string) error {
	if s.lock == nil {
		return errors.New("daemon state is not locked")
	}
	if log != "" && log != s.data.Log {
		return errors.New("delivered-id record belongs to another hub log")
	}
	return nil
}

func (s *Store) SetLog(log string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(""); err != nil {
		return err
	}
	if log == "" {
		return errors.New("empty hub log")
	}
	if log == s.data.Log {
		return nil
	}
	next := s.data
	next.Log = log
	next.IDs = make(map[string]Entry)
	return s.persist(next)
}

func (s *Store) Claim(id string, expiresAt time.Time) (bool, error) {
	return s.BeginWrite("", id, "", expiresAt, false)
}

func (s *Store) Contains(log, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(log); err != nil {
		return false, err
	}
	entry, ok := s.data.IDs[id]
	return ok && entry.Expires > time.Now().UnixNano(), nil
}

// BeginWrite atomically replaces a confirmed drop's claim on retry. A final report survives the rewrite.
func (s *Store) BeginWrite(log, id, session string, expiresAt time.Time, retry bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(log); err != nil {
		return false, err
	}
	now := time.Now()
	if id == "" || len(id) > maxID || !expiresAt.After(now) || (session != "" && log == "") {
		return false, errors.New("invalid delivered-id claim")
	}
	prior, exists := s.data.IDs[id]
	exists = exists && prior.Expires > now.UnixNano()
	if exists && !retry {
		return false, nil
	}
	if retry && (!exists || prior.Session != session) {
		return false, errors.New("retry has no matching claim")
	}
	next := s.data
	next.IDs = maps.Clone(next.IDs)
	maps.DeleteFunc(next.IDs, func(_ string, entry Entry) bool { return entry.Expires <= now.UnixNano() })
	if !exists && len(next.IDs) >= maxClaims {
		return false, errors.New("delivered-id record full")
	}
	phase := Writing
	if retry && prior.Phase == Reported {
		phase = Reported
	}
	next.IDs[id] = Entry{Session: session, Phase: phase, Expires: expiresAt.UnixNano()}
	if err := s.persist(next); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) MarkWritten(log, id string) error { return s.mark(log, id, Written) }

// MarkReported is called only after the hub applies a final report, including an interrupted write.
func (s *Store) MarkReported(log, id string) error { return s.mark(log, id, Reported) }

func (s *Store) mark(log, id string, phase Phase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(log); err != nil {
		return err
	}
	entry, ok := s.data.IDs[id]
	if !ok {
		return errors.New("missing delivered-id entry")
	}
	if entry.Phase == Reported || entry.Phase == phase {
		return nil
	}
	entry.Phase = phase
	next := s.data
	next.IDs = maps.Clone(next.IDs)
	next.IDs[id] = entry
	return s.persist(next)
}

// OpenEntries returns reportable deliveries from this log, indexed by hub message ID.
func (s *Store) OpenEntries(log string) (map[string]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(log); err != nil {
		return nil, err
	}
	entries := make(map[string]Entry)
	now := time.Now().UnixNano()
	for id, entry := range s.data.IDs {
		if entry.Session != "" && entry.Phase != Reported && entry.Expires > now {
			entries[id] = entry
		}
	}
	return entries, nil
}

func (s *Store) Forget(id string) error { return s.ForgetWrite("", id) }
func (s *Store) ForgetWrite(log, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(log); err != nil {
		return err
	}
	if _, ok := s.data.IDs[id]; !ok {
		return nil
	}
	next := s.data
	next.IDs = maps.Clone(next.IDs)
	delete(next.IDs, id)
	return s.persist(next)
}

func (m Member) valid() bool { return m.Key != "" && m.PID > 0 && m.ProcStart != "" }

func (s *Store) PutMember(m Member) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(""); err != nil {
		return err
	}
	if !m.valid() {
		return errors.New("invalid member identity")
	}
	if s.data.Members[m.Key] == m {
		return nil
	}
	next := s.data
	next.Members = maps.Clone(next.Members)
	next.Members[m.Key] = m
	return s.persist(next)
}

func (s *Store) DeleteMember(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(""); err != nil {
		return err
	}
	if _, ok := s.data.Members[key]; !ok {
		return nil
	}
	next := s.data
	next.Members = maps.Clone(next.Members)
	delete(next.Members, key)
	return s.persist(next)
}

func (s *Store) Members() ([]Member, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(""); err != nil {
		return nil, err
	}
	out := slices.Collect(maps.Values(s.data.Members))
	slices.SortFunc(out, func(a, b Member) int { return strings.Compare(a.Key, b.Key) })
	return out, nil
}

func (s *Store) persist(next snapshot) error {
	const pending = "delivered.pending"
	f, err := s.root.OpenFile(pending, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(next)
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if err = s.root.Rename(pending, recordFile); err != nil {
		return err
	}
	s.data = next
	dir, err := s.root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
