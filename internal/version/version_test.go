package version

import "testing"

func TestDevelopmentVersion(t *testing.T) {
	if Value != "development" {
		t.Fatalf("unstamped version = %q", Value)
	}
}
