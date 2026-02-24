package main

import (
	"fmt"
	"os"

	"github.com/tomfevang/go-seed-my-db/cmd"
)

func main() {
	cmd.ExamplesFS = examplesFS
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
