package state

import (
	"errors"
	"time"
)

type Store struct{}

func New(dir string) *Store { return &Store{} }
func (*Store) Lock() error  { return errors.ErrUnsupported }
func (*Store) Close() error { return errors.ErrUnsupported }

// Claim reserves an id durably before the inbox write; false means it was already claimed.
func (*Store) Claim(id string, expiresAt time.Time) (bool, error) {
	return false, errors.ErrUnsupported
}
func (*Store) Forget(id string) error { return errors.ErrUnsupported }
