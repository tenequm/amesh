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
			var refusal localError
			if e := json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&refusal); e == nil && refusal.Message != "" {
				err = errors.New(refusal.Message)
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
			last = err
		}
	}
}

type inputBlock struct {
	data []byte
	err  error
}

func readInput(ctx context.Context, in io.Reader, requests <-chan struct{}, blocks chan<- inputBlock) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-requests:
		}
		buf := make([]byte, 32*1024)
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
	requests := make(chan struct{}, 1)
	blocks := make(chan inputBlock)
	go readInput(ctx, in, requests, blocks)
	requests <- struct{}{}
	for {
		eof, copyErr := forward(ctx, conn, requests, blocks, out)
		if copyErr != nil || eof {
			return copyErr
		}
		conn, err = redial(ctx, func(ctx context.Context) (*bufferedConn, error) { return dialPipe(ctx, socket, args, true) })
		if err != nil {
			return err
		}
	}
}

func forward(ctx context.Context, conn *bufferedConn, requests chan<- struct{}, blocks <-chan inputBlock, out io.Writer) (bool, error) {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	done := make(chan error, 1)
	go func() { _, err := io.Copy(out, conn); done <- err }()
	defer conn.Close()
	for {
		select {
		case e := <-done:
			if e != nil {
				if _, ok := errors.AsType[net.Error](e); !ok {
					return false, e
				}
			}
			return false, ctx.Err()
		case block := <-blocks:
			_, err := conn.Write(block.data)
			if block.err != nil {
				if !errors.Is(block.err, io.EOF) {
					_ = conn.Close()
					return true, errors.Join(block.err, <-done)
				}
				if err == nil {
					err = conn.Conn.(*net.UnixConn).CloseWrite()
				}
				if err != nil {
					_ = conn.Close()
				}
				return true, <-done
			}
			requests <- struct{}{}
			if err != nil {
				_ = conn.Close()
				if copyErr := <-done; copyErr != nil && !errors.Is(copyErr, net.ErrClosed) {
					return false, copyErr
				}
				return false, ctx.Err()
			}
		}
	}
}
