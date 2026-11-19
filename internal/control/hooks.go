package control

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tenequm/amesh/internal/registry"
)

type hookInput struct {
	Mode       string `json:"permission_mode"`
	Transcript string `json:"transcript_path"`
	PromptID   string `json:"prompt_id"`
	Input      struct {
		To json.RawMessage `json:"to"`
	} `json:"tool_input"`
	Event     string  `json:"hook_event_name"`
	SessionID *string `json:"session_id"`
	Tool      *string `json:"tool_name"`
	Agent     *string `json:"agent_id"`
	Prompt    *string `json:"prompt"`
}

const denySubagent = `{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"peer messaging is for the main session only, not subagents"}}`

func (d *daemon) hook(w http.ResponseWriter, r *http.Request) {
	pid, _ := r.Context().Value(peerPID{}).(int)
	session, err := d.registry.Resolve(r.Context(), pid)
	if err != nil {
		reject(w, 403, "not_in_session", "This only works inside a Claude Code session.")
		return
	}
	d.applyHook(w, r, session)
}

func (d *daemon) applyHook(w http.ResponseWriter, r *http.Request, s registry.Session) {
	received := time.Now().Truncate(time.Millisecond)
	var h hookInput
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	if err != nil || json.Unmarshal(b, &h) != nil || h.SessionID == nil {
		reject(w, 400, "bad_request", "Invalid hook input.")
		return
	}
	switch {
	case h.Event == "PreToolUse" && h.Tool != nil:
		d.registry.RecordPermissionMode(s.Key, h.Mode)
		if h.Agent != nil && (strings.HasPrefix(*h.Tool, "mcp__amesh__") || (*h.Tool == "SendMessage" && d.relayTarget(h.Input.To))) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, denySubagent)
			return
		}
	case h.Event == "UserPromptSubmit" && h.Prompt != nil:
		d.registry.RecordPermissionMode(s.Key, h.Mode)
		if humanPrompt(*h.Prompt) && !d.meshExcluded(s) && d.mesh.IsMember(s.Key) {
			if h.Transcript != "" {
				d.work.Go(func() { d.confirmTurn(s, h, received) })
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}
	default:
		reject(w, 400, "bad_request", "Unknown or malformed hook event.")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func humanPrompt(prompt string) bool {
	return !strings.HasPrefix(prompt, "<cross-session-message") && !strings.HasPrefix(prompt, "<agent-message") && !strings.HasPrefix(prompt, "<task-notification>")
}

func (d *daemon) relayTarget(raw json.RawMessage) bool {
	var address string
	if json.Unmarshal(raw, &address) != nil || !strings.HasPrefix(address, "uds:") {
		return false
	}
	return d.ownsRelay != nil && d.ownsRelay(address)
}
