package peercred

import (
	"errors"
	"net"
)

type Credentials struct {
	PID int
	UID uint32
}

func Read(conn *net.UnixConn) (Credentials, error) {
	if conn == nil {
		return Credentials{}, errors.New("missing Unix connection")
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return Credentials{}, err
	}
	var credentials Credentials
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		credentials, socketErr = read(int(fd))
	})
	if err != nil {
		return Credentials{}, err
	}
	return credentials, socketErr
}
