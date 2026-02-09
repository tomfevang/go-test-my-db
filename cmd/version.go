package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/tomfevang/go-seed-my-db/internal/version"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version and build information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version.BuildInfo())
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
