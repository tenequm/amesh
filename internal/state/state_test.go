package state

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestLockSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "amesh-wire-state")
	a, b := New(dir), New(dir)
	if err := a.Lock(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	if err := b.Lock(); err == nil {
		t.Fatal("second daemon acquired lock")
	}
	end := time.Now().Add(25 * time.Hour)
	var wins atomic.Int64
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			ok, err := a.Claim("message", end)
			if err != nil {
				t.Error(err)
			}
			if ok {
				wins.Add(1)
			}
		})
	}
	group.Wait()
	if wins.Load() != 1 {
		t.Fatal(wins.Load())
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Lock(); err != nil {
		t.Fatal(err)
	}
	if ok, err := b.Claim("message", end); err != nil || ok {
		t.Fatalf("replay %t, %v", ok, err)
	}
	if err := b.Forget("message"); err != nil {
		t.Fatal(err)
	}
	if ok, err := b.Claim("message", end); err != nil || !ok {
		t.Fatalf("released claim %t, %v", ok, err)
	}
	for file, mode := range map[string]os.FileMode{".": 0o700, recordFile: 0o600, "daemon.lock": 0o600} {
		info, err := os.Stat(filepath.Join(dir, file))
		if err != nil || info.Mode().Perm() != mode {
			t.Fatal(file, info, err)
		}
	}
}

func TestExpiryAndBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(t.TempDir())
		if err := s.Lock(); err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		now := time.Now()
		if ok, err := s.Claim("id", now.Add(time.Hour)); !ok || err != nil {
			t.Fatal(ok, err)
		}
		for i := range maxClaims - 1 {
			s.ids[string(rune(0x1000+i))] = now.Add(2 * time.Hour).UnixNano()
		}
		if _, err := s.Claim("new", now.Add(3*time.Hour)); err == nil {
			t.Fatal("evicted live claim")
		}
		time.Sleep(time.Hour)
		if ok, err := s.Claim("id", now.Add(3*time.Hour)); !ok || err != nil {
			t.Fatal(ok, err)
		}
	})
}

func TestCorruptRecordBlocksStartup(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, recordFile), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := New(dir)
	if err := s.Lock(); err == nil {
		t.Fatal("discarded corrupt record")
	}
	if _, err := s.Claim("id", time.Now().Add(time.Hour)); err == nil {
		t.Fatal("claim without lock")
	}
}
