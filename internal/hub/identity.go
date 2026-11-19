package hub

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/tenequm/amesh/internal/config"
	"golang.org/x/sys/unix"
)

var (
	encoding    = base64.RawURLEncoding
	machineName = regexp.MustCompile("^[a-z0-9][a-z0-9-]{0,15}$")
)

type identity struct {
	key      *ecdsa.PrivateKey
	id, spki string
}

func loadKey(path string) (identity, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return identity{}, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return identity{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return identity{}, errors.New("machine key must be a private regular file (0600)")
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return identity{}, err
	}
	key, err := x509.ParseECPrivateKey(data)
	if err != nil || key.Curve != elliptic.P256() {
		return identity{}, errors.New("invalid P-256 machine key")
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return identity{}, err
	}
	hash := sha256.Sum256(der)
	return identity{key, "pm_" + encoding.EncodeToString(hash[:16]), encoding.EncodeToString(der)}, nil
}

func saveFile(path string, data []byte, exclusive bool) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".amesh-hub-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err = errors.Join(err, closeErr); err != nil {
		return err
	}
	if exclusive {
		err = os.Link(f.Name(), path)
	} else {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func Enroll(ctx context.Context, cfg config.Config, out io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !machineName.MatchString(cfg.MachineName) {
		return errors.New("enroll requires a valid machine-name")
	}
	path := filepath.Join(cfg.StateDir, "machine.key")
	id, err := loadKey(path)
	if errors.Is(err, os.ErrNotExist) {
		key, genErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if genErr != nil {
			return genErr
		}
		data, marshalErr := x509.MarshalECPrivateKey(key)
		if marshalErr != nil {
			return marshalErr
		}
		if err = saveFile(path, data, true); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		id, err = loadKey(path)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "ncl peers add --name %s --public-key %s\n", cfg.MachineName, id.spki)
	return err
}

func canonical(method, target, id string, ts int64, nonce string, body []byte) (string, string) {
	sum := sha256.Sum256(body)
	bh := hex.EncodeToString(sum[:])
	return strings.Join([]string{"HEYDAN-SIG-V1", "1", method, target, strconv.FormatInt(ts, 10), nonce, bh, id}, "\n"), bh
}

func (id identity) sign(method, target string, ts int64, body []byte) (string, error) {
	var n [16]byte
	rand.Read(n[:])
	nonce := encoding.EncodeToString(n[:])
	text, bh := canonical(method, target, id.id, ts, nonce, body)
	hash := sha256.Sum256([]byte(text))
	sig, err := ecdsa.SignASN1(rand.Reader, id.key, hash[:])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("HeyDan-Sig v=1,p=1,d=%s,t=%d,n=%s,bh=%s,s=%s", id.id, ts, nonce, bh, encoding.EncodeToString(sig)), nil
}
