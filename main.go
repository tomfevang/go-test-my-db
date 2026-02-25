package main

import (
	"fmt"
	"os"

	"github.com/tomfevang/go-test-my-db/cmd"
)

func main() {
	cmd.ExamplesFS = examplesFS
	cmd.SkillsFS = examplesFS
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
