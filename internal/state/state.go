package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	recordFile = "delivered.json"
	maxClaims  = 32768
	maxID      = 128
)

type Store struct {
	dir  string
	mu   sync.Mutex
	root *os.Root
	lock *os.File
	ids  map[string]int64
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
	ids := make(map[string]int64)
	data, err := root.ReadFile(recordFile)
	if err == nil {
		err = json.Unmarshal(data, &ids)
		if err == nil && (ids == nil || len(ids) > maxClaims) {
			err = errors.New("invalid delivered-id record")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return errors.Join(err, file.Close(), root.Close())
	}
	s.root, s.lock, s.ids = root, file, ids
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	err := errors.Join(s.lock.Close(), s.root.Close())
	s.root, s.lock, s.ids = nil, nil, nil
	return err
}

// Claim reserves an id durably before the inbox write; false means it was already claimed.
func (s *Store) Claim(id string, expiresAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return false, errors.New("daemon state is not locked")
	}
	now := time.Now()
	if id == "" || len(id) > maxID || !expiresAt.After(now) {
		return false, errors.New("invalid delivered-id claim")
	}
	if expires, ok := s.ids[id]; ok && expires > now.UnixNano() {
		return false, nil
	}
	next := maps.Clone(s.ids)
	maps.DeleteFunc(next, func(_ string, expires int64) bool { return expires <= now.UnixNano() })
	if len(next) >= maxClaims {
		return false, errors.New("delivered-id record full")
	}
	next[id] = expiresAt.UnixNano()
	if err := s.persist(next); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Forget(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return errors.New("daemon state is not locked")
	}
	if _, ok := s.ids[id]; !ok {
		return nil
	}
	next := maps.Clone(s.ids)
	delete(next, id)
	return s.persist(next)
}

func (s *Store) persist(next map[string]int64) error {
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
	s.ids = next
	dir, err := s.root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
