package control

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/mcpserver"
)

func dialPipe(ctx context.Context, socket string, args []string, reconnect bool) (*bufferedConn, error) {
	q := url.Values{"arg": args}
	if reconnect {
		q.Set("reconnect", "1")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://amesh.localhost/amesh/mcp?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "mcp-stdio")
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	reader := bufio.NewReader(conn)
	if err = req.Write(conn); err == nil {
		var resp *http.Response
		resp, err = http.ReadResponse(reader, req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusSwitchingProtocols && strings.EqualFold(resp.Header.Get("Upgrade"), "mcp-stdio") && headerToken(resp.Header.Values("Connection"), "upgrade") {
				return &bufferedConn{conn, reader}, nil
			}
			var refusal contract.LocalError
			if e := json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&refusal); e == nil && refusal.Message != "" {
				err = localError(refusal.Message)
			} else {
				err = fmt.Errorf("daemon refused upgrade (%s)", resp.Status)
			}
		}
	}
	_ = conn.Close()
	return nil, err
}

func redial(ctx context.Context, dial func(context.Context) (*bufferedConn, error)) (*bufferedConn, error) {
	retry, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var last error
	for delay := 250 * time.Millisecond; ; delay = min(delay*2, 2*time.Second) {
		select {
		case <-retry.Done():
			return nil, fmt.Errorf("MCP reconnect: %w", errors.Join(retry.Err(), last))
		case <-time.After(delay):
			if retry.Err() != nil {
				continue
			}
			conn, err := dial(retry)
			if err == nil {
				return conn, nil
			}
			if _, refused := errors.AsType[localError](err); refused {
				return nil, err
			}
			last = err
		}
	}
}

type inputBlock struct {
	data []byte
	err  error
}

func readInput(ctx context.Context, in io.Reader, requests <-chan struct{}, blocks chan<- inputBlock) {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return
		case <-requests:
		}
		n, err := in.Read(buf)
		select {
		case <-ctx.Done():
			return
		case blocks <- inputBlock{buf[:n], err}:
		}
		if err != nil {
			return
		}
	}
}

func Pipe(ctx context.Context, socket string, args []string, in io.Reader, out io.Writer) error {
	conn, err := dialPipe(ctx, socket, args, false)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	requests := make(chan struct{})
	blocks := make(chan inputBlock)
	go readInput(ctx, in, requests, blocks)
	needInput := true
	for {
		eof, copyErr := forward(ctx, conn, requests, blocks, out, &needInput)
		if copyErr != nil || eof {
			return copyErr
		}
		conn, err = redial(ctx, func(ctx context.Context) (*bufferedConn, error) { return dialPipe(ctx, socket, args, true) })
		if err != nil {
			return err
		}
	}
}

type outputWriter struct {
	dest io.Writer
	err  error
}

func (w *outputWriter) Write(p []byte) (int, error) {
	n, err := w.dest.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

func forward(ctx context.Context, conn *bufferedConn, requests chan<- struct{}, blocks <-chan inputBlock, out io.Writer, needInput *bool) (bool, error) {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	done := make(chan error, 1)
	go func() { w := outputWriter{dest: out}; _, _ = io.Copy(&w, conn); done <- w.err }()
	defer conn.Close()
	for {
		var grant chan<- struct{}
		if *needInput {
			grant = requests
		}
		select {
		case grant <- struct{}{}:
			*needInput = false
		case outputErr := <-done:
			return outputErr != nil, errors.Join(ctx.Err(), mcpserver.IgnorePeerClose(outputErr))
		case block := <-blocks:
			_, writeErr := conn.Write(block.data)
			if block.err != nil {
				inputErr := mcpserver.IgnorePeerClose(block.err)
				if inputErr == nil && writeErr == nil {
					writeErr = conn.Conn.(*net.UnixConn).CloseWrite()
				}
				if inputErr != nil || writeErr != nil {
					_ = conn.Close()
				}
				return true, errors.Join(ctx.Err(), inputErr, mcpserver.IgnorePeerClose(writeErr), mcpserver.IgnorePeerClose(<-done))
			}
			*needInput = true
			if writeErr != nil {
				_ = conn.Close()
				outputErr := <-done
				return outputErr != nil, errors.Join(ctx.Err(), mcpserver.IgnorePeerClose(outputErr))
			}
		}
	}
}
