package peercred

import (
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
)

func TestPeer(t *testing.T) {
	const childSocket = "AMESH_TEST_CHILD_SOCKET"
	if socket := os.Getenv(childSocket); socket != "" {
		conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: socket, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := os.Stdout.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, conn); err != nil {
			t.Fatal(err)
		}
		return
	}

	// A relative address also fits Darwin's 104-byte socket path limit in deep checkouts.
	t.Chdir(t.TempDir())
	addr := &net.UnixAddr{Name: "peer.sock", Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPeer$")
	cmd.Env = append(os.Environ(), childSocket+"="+addr.Name)
	ready, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	var signal [1]byte
	if _, err = io.ReadFull(ready, signal[:]); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Read(conn)
	conn.Close()
	if waitErr := cmd.Wait(); waitErr != nil {
		t.Fatal(waitErr)
	}
	if err != nil || got.PID != cmd.Process.Pid || int(got.UID) != os.Geteuid() {
		t.Fatalf("got %+v, %v; want pid %d uid %d", got, err, cmd.Process.Pid, os.Geteuid())
	}
	if _, err := Read(conn); err == nil {
		t.Fatal("closed connection returned credentials")
	}
}
