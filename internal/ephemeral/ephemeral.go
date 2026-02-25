// Package ephemeral manages temporary MySQL containers for self-contained
// benchmarks that don't require an existing database server.
// Supports both Docker and Podman as container runtimes.
package ephemeral

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlImage   = "mysql:8.0"
	mysqlRootPwd = "seedtest"
	mysqlDB      = "seedtest"

	startTimeout = 120 * time.Second
	pingInterval = 500 * time.Millisecond
)

// DB represents a running ephemeral MySQL container.
type DB struct {
	ContainerID string
	DSN         string
	Port        int
	runtime     string // "docker" or "podman"
}

// Start launches a MySQL container on a random free port, waits for it
// to accept connections, and returns the connection details. Call Stop when done.
// It auto-detects the container runtime, preferring Docker over Podman.
func Start(ctx context.Context) (*DB, error) {
	runtime, err := detectRuntime(ctx)
	if err != nil {
		return nil, err
	}

	port, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("finding free port: %w", err)
	}

	fmt.Printf("Starting ephemeral MySQL (%s) on port %d...\n", runtime, port)

	id, err := startContainer(ctx, runtime, port)
	if err != nil {
		return nil, fmt.Errorf("starting MySQL container: %w", err)
	}

	dsn := fmt.Sprintf("root:%s@tcp(127.0.0.1:%d)/%s", mysqlRootPwd, port, mysqlDB)

	edb := &DB{
		ContainerID: id,
		DSN:         dsn,
		Port:        port,
		runtime:     runtime,
	}

	if err := waitReady(ctx, dsn); err != nil {
		// Clean up on failure.
		edb.Stop()
		return nil, fmt.Errorf("waiting for MySQL readiness: %w", err)
	}

	fmt.Println("Ephemeral MySQL is ready")
	return edb, nil
}

// Stop removes the container.
func (edb *DB) Stop() {
	if edb == nil || edb.ContainerID == "" {
		return
	}
	fmt.Println("Stopping ephemeral MySQL...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = exec.CommandContext(ctx, edb.runtime, "rm", "-f", edb.ContainerID).Run()
}

// detectRuntime finds an available container runtime, preferring Docker over Podman.
func detectRuntime(ctx context.Context) (string, error) {
	for _, rt := range []string{"docker", "podman"} {
		if _, err := exec.LookPath(rt); err != nil {
			continue
		}
		if err := exec.CommandContext(ctx, rt, "info").Run(); err != nil {
			continue
		}
		return rt, nil
	}
	return "", fmt.Errorf("no container runtime found — install Docker or Podman to use --ephemeral")
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	return port, l.Close()
}

func startContainer(ctx context.Context, runtime string, port int) (string, error) {
	cmd := exec.CommandContext(ctx, runtime, "run", "-d",
		"--name", fmt.Sprintf("seedtest-%d", port),
		"-e", "MYSQL_ROOT_PASSWORD="+mysqlRootPwd,
		"-e", "MYSQL_DATABASE="+mysqlDB,
		"-p", fmt.Sprintf("127.0.0.1:%d:3306", port),
		mysqlImage,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w\n%s", err, stderr.String())
	}
	return strings.TrimSpace(stdout.String()), nil
}

func waitReady(ctx context.Context, dsn string) error {
	deadline := time.Now().Add(startTimeout)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	for {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			err = db.PingContext(ctx)
			db.Close()
			if err == nil {
				return nil
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("MySQL did not become ready within %s", startTimeout)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pingInterval):
		}
	}
}
