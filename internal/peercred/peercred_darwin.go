package peercred

import "golang.org/x/sys/unix"

func read(fd int) (Credentials, error) {
	pid, err := unix.GetsockoptInt(fd, unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	if err != nil {
		return Credentials{}, err
	}
	cred, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{PID: pid, UID: cred.Uid}, nil
}
