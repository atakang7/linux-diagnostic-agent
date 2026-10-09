//go:build integration

package agent

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercises the actual agent with Redis and a real TCP listener implementing
// the same newline-delimited JSON protocol as diagnostic-client.
func TestAgentInventoryCommandAndLogDelivery(t *testing.T) {
	root := t.TempDir()
	t.Setenv("LOG_ROOTS", root)
	filePath := filepath.Join(root, "service.log")
	if err := os.WriteFile(filePath, []byte("INFO started\nERROR request failed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if tcpListener, ok := listener.(*net.TCPListener); ok {
		_ = tcpListener.SetDeadline(time.Now().Add(8 * time.Second))
	}

	a, err := New(listener.Addr().String(), "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() { exited <- a.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-exited:
			if err != nil {
				t.Errorf("agent shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("agent did not shut down promptly")
		}
	}()

	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(9 * time.Second))
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	sent := false
	for {
		var event struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := decoder.Decode(&event); err != nil {
			t.Fatalf("TCP event not received: %v", err)
		}
		switch event.Type {
		case "log_list":
			if sent {
				continue
			}
			var files []struct {
				Path string `json:"path"`
			}
			if err := json.Unmarshal(event.Payload, &files); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, item := range files {
				found = found || item.Path == filePath
			}
			if !found {
				t.Fatalf("log inventory missing file: %+v", files)
			}
			if err := encoder.Encode(map[string]interface{}{
				"type": "log_search",
				"payload": map[string]interface{}{
					"files": []string{filePath}, "keywords": []string{"ERROR"},
				},
			}); err != nil {
				t.Fatal(err)
			}
			sent = true
		case "log_data":
			var rows []struct {
				Line  string `json:"line"`
				Level string `json:"level"`
			}
			if err := json.Unmarshal(event.Payload, &rows); err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || !strings.Contains(rows[0].Line, "request failed") || rows[0].Level != "error" {
				t.Fatalf("incorrect log payload: %+v", rows)
			}
			return
		}
	}
}
