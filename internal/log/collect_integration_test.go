//go:build integration

package log

import (
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRedisBackedLogSearchAndDiscovery(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOG_ROOTS", root)
	plain := filepath.Join(root, "application.log")
	compressed := filepath.Join(root, "archived.log.gz")
	if err := os.WriteFile(plain, []byte("INFO start\nERROR first\nINFO done\n"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(compressed)
	if err != nil {
		t.Fatal(err)
	}
	writer := gzip.NewWriter(file)
	_, err = writer.Write([]byte("WARN archived\nERROR second\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	c, err := New("127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer c.redisClient.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if err := c.updateLogFiles(ctx); err != nil {
		t.Fatal(err)
	}
	inventory := c.GetDiscoveredFiles()
	if len(inventory) != 3 { // root directory and both logs
		t.Fatalf("unexpected inventory: %+v", inventory)
	}
	if err := c.ProcessFiles(ctx, FileProcessRequest{
		Files:    []string{plain, compressed},
		Keywords: []string{"ERROR"},
	}); err != nil {
		t.Fatal(err)
	}
	found := make(map[string]string)
	for len(found) < 2 {
		select {
		case batch := <-c.GetLogChannel():
			for _, entry := range batch {
				found[entry.Filename] = entry.Line
			}
		case <-ctx.Done():
			t.Fatalf("missing delivered log entries: %v", found)
		}
	}
	if !strings.Contains(found[plain], "first") || !strings.Contains(found[compressed], "second") {
		t.Fatalf("incorrect search results: %+v", found)
	}
}

func TestRejectsUndiscoveredLogPaths(t *testing.T) {
	t.Setenv("LOG_ROOTS", t.TempDir())
	c, err := New("127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer c.redisClient.Close()
	ctx := context.Background()
	if err := c.updateLogFiles(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.ProcessFiles(ctx, FileProcessRequest{
		Files: []string{"/etc/passwd"}, Keywords: []string{"root"},
	}); err == nil {
		t.Fatal("unauthorized path was accepted")
	}
}
