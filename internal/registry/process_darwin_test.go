package registry

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

func TestSingleDigitDayVector(t *testing.T) {
	t.Setenv("TZ", "Pacific/Honolulu")
	stamp := darwinStart(time.Date(2026, 11, 5, 9, 4, 11, 0, time.UTC).Unix())
	digest := sha256.Sum256([]byte(stamp))
	if stamp != "Thu Nov  5 09:04:11 2026" || fmt.Sprintf("4821-%x", digest[:4]) != "4821-f2151faf" {
		t.Fatalf("unexpected procStart %q", stamp)
	}
}
