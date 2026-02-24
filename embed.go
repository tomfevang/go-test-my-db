package main

import "embed"

//go:embed examples/*
var examplesFS embed.FS
