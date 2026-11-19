package peercred

import "golang.org/x/sys/unix"

func read(fd int) (Credentials, error) {
	cred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{PID: int(cred.Pid), UID: cred.Uid}, nil
}
