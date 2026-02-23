package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// resolveString returns the first non-empty value in priority order:
// CLI flag (if explicitly set) > env var > config file value > default.
func resolveString(cmd *cobra.Command, flagName, flagVal, envVar, cfgVal, defaultVal string) string {
	if cmd.Flags().Changed(flagName) {
		return flagVal
	}
	if envVar != "" {
		if v := os.Getenv(envVar); v != "" {
			return v
		}
	}
	if cfgVal != "" {
		return cfgVal
	}
	return defaultVal
}

// resolveInt returns the first meaningful value in priority order:
// CLI flag (if explicitly set) > config file value (if > 0) > default.
func resolveInt(cmd *cobra.Command, flagName string, flagVal, cfgVal, defaultVal int) int {
	if cmd.Flags().Changed(flagName) {
		return flagVal
	}
	if cfgVal > 0 {
		return cfgVal
	}
	return defaultVal
}
