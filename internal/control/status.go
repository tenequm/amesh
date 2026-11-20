package control

import (
	"encoding/json"
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
	d.mu.Lock()
	for reg := range d.pipes {
		s, ok := byKey[reg.key]
		if !ok {
			continue
		}
		pipes = append(pipes, pipeStatus{s.MeshKey(), s.Name, s.SessionID, s.ConfigDir, reg.parent})
	}
	d.mu.Unlock()
	slices.SortFunc(pipes, func(a, b pipeStatus) int {
		if a.Key == b.Key {
			return strings.Compare(a.Parent, b.Parent)
		}
		return strings.Compare(a.Key, b.Key)
	})
	result.Pipes = pipes
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func sessionKey(key registry.Key) string {
	return key.MeshKey()
}
