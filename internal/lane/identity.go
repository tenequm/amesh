package lane

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// A lane can precede enrollment. Use the same local P-256 key so enrollment never moves a live relay.
func machineID(dir string) (string, error) {
	path := filepath.Join(dir, "machine.key")
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		if err = createKey(dir, path); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
		f, err = os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	}
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("machine key must be a private regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return "", err
	}
	key, err := x509.ParseECPrivateKey(raw)
	if err != nil || key.Curve != elliptic.P256() {
		return "", errors.New("invalid P-256 machine key")
	}
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(spki)
	return "pm_" + base64.RawURLEncoding.EncodeToString(sum[:16]), nil
}

func createKey(dir, path string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	raw, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".amesh-lanemode-key-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if err = os.Link(f.Name(), path); err != nil {
		return err
	}
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}
