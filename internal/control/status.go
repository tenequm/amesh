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

	"github.com/tenequm/amesh/internal/contract"
	"github.com/tenequm/amesh/internal/registry"
	"github.com/tenequm/amesh/internal/version"
)

type (
	pipeStatus struct {
		Key       string `json:"key"`
		Name      string `json:"name"`
		SessionID string `json:"sessionId"`
		ConfigDir string `json:"configDir"`
		Parent    string `json:"parent,omitempty"`
	}
)

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
	dirs := make([]contract.DaemonStatusConfigDirsItem, 0, len(d.cfg.ClaudeConfigDirs))
	for _, dir := range d.cfg.ClaudeConfigDirs {
		dirs = append(dirs, contract.DaemonStatusConfigDirsItem{Path: dir, Sessions: int64(counts[dir])})
	}
	pipes := []pipeStatus{}
	var result struct {
		contract.DaemonStatus
		Pipes []pipeStatus `json:"pipes"`
	}
	result.DaemonStatus = contract.DaemonStatus{
		ConfigDirs: dirs, Sessions: []contract.DaemonStatusSessionsItem{}, Lanes: []contract.DaemonStatusLanesItem{},
		Daemon: contract.DaemonStatusDaemon{Version: version.Value, PID: int64(os.Getpid()), StartedAt: d.started.UnixMilli()},
	}
	if err := d.mesh.Status(&result.DaemonStatus); err != nil {
		http.Error(w, "cannot read mesh status", http.StatusServiceUnavailable)
		return
	}
	if d.lane != nil {
		result.Lanes = d.lane.LaneStatus(r.Context())
	}
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
	}
	slices.SortFunc(pipes, func(a, b pipeStatus) int {
		if a.Key == b.Key {
			return strings.Compare(a.Parent, b.Parent)
		}
		return strings.Compare(a.Key, b.Key)
	})
	result.Pipes = pipes
	if len(warnings) > 0 {
		list := slices.Sorted(maps.Keys(warnings))
		result.Warnings = &list
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func sessionKey(key registry.Key) string {
	digest := sha256.Sum256([]byte(key.ProcStart))
	return fmt.Sprintf("%d-%x", key.PID, digest[:4])
}
