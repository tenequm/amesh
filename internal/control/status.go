package control

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/version"
)

type (
	configDir struct {
		Path     string `json:"path"`
		Sessions int    `json:"sessions"`
	}
	pipeStatus struct {
		Key       string `json:"key"`
		Name      string `json:"name"`
		SessionID string `json:"sessionId"`
		ConfigDir string `json:"configDir"`
		Parent    string `json:"parent,omitempty"`
	}
)

type memberStatus struct {
	Key     string `json:"key"`
	Address string `json:"address"`
	Status  string `json:"status"`
	Role    string `json:"role"`
}

// Populated when the native end-to-end delivery proof passes for a Claude Code release.
var verifiedNativeVersions []string

func (d *daemon) status(w http.ResponseWriter, r *http.Request) {
	live, err := d.registry.List(r.Context())
	if err != nil {
		http.Error(w, "cannot read session registry", http.StatusServiceUnavailable)
		return
	}
	counts := map[string]int{}
	byKey := map[registry.Key]registry.Session{}
	for _, s := range live {
		counts[s.ConfigDir]++
		byKey[s.Key] = s
	}
	dirs := make([]configDir, 0, len(d.cfg.ClaudeConfigDirs))
	for _, dir := range d.cfg.ClaudeConfigDirs {
		dirs = append(dirs, configDir{dir, counts[dir]})
	}
	pipes := []pipeStatus{}
	members := []memberStatus{}
	warnings := map[string]bool{}
	lanePipes := map[registry.Key]bool{}
	d.mu.Lock()
	for reg := range d.pipes {
		s := byKey[reg.key]
		pipes = append(pipes, pipeStatus{sessionKey(reg.key), s.Name, s.SessionID, s.ConfigDir, reg.parent})
		if reg.parent != "" {
			lanePipes[reg.key] = true
		}
	}
	d.mu.Unlock()
	for key, s := range byKey {
		member := d.mesh.IsMember(key)
		if !member && !lanePipes[key] {
			continue
		}
		address := s.Name + "@" + s.Project
		if d.cfg.MachineName != "" {
			address += ":" + d.cfg.MachineName
		}
		if s.Version == "" {
			warnings[fmt.Sprintf("%s has no recorded Claude Code version for the native delivery check.", address)] = true
		} else if !slices.Contains(verifiedNativeVersions, s.Version) {
			warnings[fmt.Sprintf("%s runs Claude Code %s without a recorded native delivery check.", address, s.Version)] = true
		}
		if member {
			members = append(members, memberStatus{sessionKey(key), address, s.Status, "member"})
		}
	}
	slices.SortFunc(pipes, func(a, b pipeStatus) int {
		if a.Key == b.Key {
			return strings.Compare(a.Parent, b.Parent)
		}
		return strings.Compare(a.Key, b.Key)
	})
	slices.SortFunc(members, func(a, b memberStatus) int { return strings.Compare(a.Key, b.Key) })
	started := d.started.UnixMilli()
	status := map[string]any{
		"daemon":  map[string]any{"version": version.Value, "pid": os.Getpid(), "startedAt": started},
		"machine": nil, "hub": map[string]any{"state": "connecting", "since": started, "epoch": nil, "cursor": 0, "halt": nil},
		"configDirs": dirs, "pipes": pipes, "sessions": members, "lanes": []any{},
		"outbox": map[string]any{"count": 0, "oldestAt": nil}, "open": map[string]int{"count": 0},
	}
	if len(warnings) > 0 {
		status["warnings"] = slices.Sorted(maps.Keys(warnings))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

func sessionKey(key registry.Key) string {
	digest := sha256.Sum256([]byte(key.ProcStart))
	return fmt.Sprintf("%d-%x", key.PID, digest[:4])
}
