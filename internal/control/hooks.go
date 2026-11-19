package control

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tenequm/amesh/internal/registry"
)

type hookInput struct {
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
	var h hookInput
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	if err != nil || json.Unmarshal(b, &h) != nil || h.SessionID == nil {
		reject(w, 400, "bad_request", "Invalid hook input.")
		return
	}
	switch {
	case h.Event == "PreToolUse" && h.Tool != nil:
		if h.Agent != nil && strings.HasPrefix(*h.Tool, "mcp__amesh__") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, denySubagent)
			return
		}
	case h.Event == "UserPromptSubmit" && h.Prompt != nil:
		if humanPrompt(*h.Prompt) && !excluded(s.CWD, d.cfg.ExcludeRoots) && d.meshMember(s.Key) {
			d.work.Go(func() {
				if err := d.mesh.HumanTurn(d.ctx, s); err != nil && d.ctx.Err() == nil {
					slog.Warn("human turn report failed", "error", err)
				}
			})
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

func (d *daemon) meshMember(key registry.Key) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for reg := range d.pipes {
		if reg.parent == "" && reg.key == key {
			return true
		}
	}
	return false
}
