package registry

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func process(pid int) (int, string, error) {
	infos, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if errors.Is(err, unix.ESRCH) || (err == nil && (len(infos) != 1 || int(infos[0].Proc.P_pid) != pid)) {
		return 0, "", os.ErrNotExist
	}
	if err != nil {
		return 0, "", err
	}
	info := infos[0]
	return int(info.Eproc.Ppid), time.Unix(info.Proc.P_starttime.Sec, 0).UTC().Format(time.ANSIC), nil
}
