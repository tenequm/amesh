package control

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/tenequm/amesh/internal/registry"
)

type (
	origin struct {
		Kind string `json:"kind"`
	}
	promptRow struct {
		Type      string    `json:"type"`
		SessionID string    `json:"sessionId"`
		PromptID  string    `json:"promptId"`
		Timestamp time.Time `json:"timestamp"`
		Origin    origin    `json:"origin"`
		Message   struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
		Attachment struct {
			Type   string `json:"type"`
			Prompt string `json:"prompt"`
			Origin origin `json:"origin"`
		} `json:"attachment"`
	}
)

func (d *daemon) confirmTurn(s registry.Session, h hookInput, received time.Time) {
	projects := filepath.Join(s.ConfigDir, "projects")
	rel, err := filepath.Rel(projects, h.Transcript)
	if err != nil || !filepath.IsLocal(rel) || filepath.Base(rel) != s.SessionID+".jsonl" || len(strings.Split(rel, string(filepath.Separator))) != 2 {
		return
	}
	root, err := os.OpenRoot(s.ConfigDir)
	if err != nil {
		return
	}
	defer root.Close()
	var offset int64
	for {
		current, e := d.registry.Resolve(d.ctx, s.PID)
		if e != nil || current.Key != s.Key || current.SessionID != s.SessionID || !d.mesh.IsMember(s.Key) || d.meshExcluded(current) {
			return
		}
		next, matched, err := scanTranscript(root, filepath.Join("projects", rel), offset, s.SessionID, h, received)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return
		}
		offset = next
		if matched != nil {
			if matched.Kind == "human" {
				if err := d.mesh.HumanTurn(d.ctx, current); err != nil && d.ctx.Err() == nil {
					slog.Warn("human turn report failed", "error", err)
				}
			}
			return
		}
		select {
		case <-d.ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Consume complete JSONL records only. Claude can append the rest of a row between polls.
func scanTranscript(root *os.Root, rel string, offset int64, id string, h hookInput, after time.Time) (int64, *origin, error) {
	f, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return offset, nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return offset, nil, err
	}
	if !info.Mode().IsRegular() {
		return offset, nil, errors.New("invalid transcript file")
	}
	if offset > info.Size() {
		offset = 0
	}
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return offset, nil, err
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 32*1024), 16<<20)
	scanner.Split(func(b []byte, _ bool) (int, []byte, error) {
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			return i + 1, b[:i+1], nil
		}
		return 0, nil, nil
	})
	for scanner.Scan() {
		line := scanner.Bytes()
		offset += int64(len(line))
		var row promptRow
		if json.Unmarshal(line, &row) != nil || row.SessionID != id {
			continue
		}
		if h.PromptID != "" && row.PromptID != "" {
			if row.PromptID != h.PromptID {
				continue
			}
		} else if row.Timestamp.Before(after) {
			continue
		}
		switch row.Type {
		case "user":
			if promptMatches(row.Message.Content, *h.Prompt) {
				return offset, &row.Origin, nil
			}
		case "attachment":
			if row.Attachment.Type == "queued_command" && row.Attachment.Prompt == *h.Prompt {
				return offset, &row.Attachment.Origin, nil
			}
		}
	}
	return offset, nil, scanner.Err()
}

func promptMatches(raw json.RawMessage, prompt string) bool {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text == prompt
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return false
	}
	for _, block := range blocks {
		if block.Type == "text" && block.Text == prompt {
			return true
		}
	}
	return false
}
