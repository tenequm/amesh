package registry

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func process(pid int) (int, string, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, "", err
	}
	stat := string(b)
	end := strings.LastIndexByte(stat, ')')
	fields := strings.Fields(stat[end+1:])
	if end < 0 || len(fields) < 20 {
		return 0, "", fmt.Errorf("invalid proc stat for pid %d", pid)
	}
	parent, err := strconv.Atoi(fields[1])
	return parent, fields[19], err
}
