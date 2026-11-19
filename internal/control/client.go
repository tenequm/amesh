package control

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

func localRequest(ctx context.Context, socket, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://amesh.localhost"+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	return (&http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
}

// Hook fails open: a missing or failing daemon must never block Claude Code.
func Hook(ctx context.Context, socket string, in io.Reader, out io.Writer) {
	resp, err := localRequest(ctx, socket, http.MethodPost, "/amesh/hooks", in)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		_, _ = io.Copy(out, resp.Body)
	}
}

func Status(ctx context.Context, socket string, out io.Writer) error {
	resp, err := localRequest(ctx, socket, http.MethodGet, "/amesh/status", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("daemon status: %s", resp.Status)
	}
	_, err = io.Copy(out, resp.Body)
	return err
}
