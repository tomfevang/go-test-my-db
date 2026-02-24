package version

import (
	"fmt"
	"runtime"
)

// Set via ldflags at build time by GoReleaser.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func Version() string {
	return version
}

func BuildInfo() string {
	return fmt.Sprintf("go-seed-my-db %s (commit: %s, built: %s, go: %s)",
		version, commit, date, runtime.Version())
}
