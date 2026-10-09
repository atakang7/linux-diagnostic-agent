package tunnel

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

func receiveCommand(t *testing.T, ch <-chan json.RawMessage) string {
	t.Helper()
	select {
	case data := <-ch:
		return string(data)
	case <-time.After(5 * time.Second):
		t.Fatal("command not received")
		return ""
	}
}

func TestBidirectionalTCPDoesNotLoseCommandBytes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tunnel := New(listener.Addr().String())
	if err := tunnel.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer tunnel.Close()

	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	commands := []string{
		"{\"type\":\"log_search\",\"payload\":{\"files\":[\"one\"],\"keywords\":[\"error\"]}}",
		"{\"type\":\"log_search\",\"payload\":{\"files\":[\"two\"],\"keywords\":[\"warn\"]}}",
		"{\"type\":\"log_search\",\"payload\":{\"files\":[\"three\"],\"keywords\":[\"fatal\"]}}",
	}
	for _, payload := range commands {
		if _, err := conn.Write([]byte(payload + "\n")); err != nil {
			t.Fatal(err)
		}
		if received := receiveCommand(t, tunnel.Commands()); received != payload {
			t.Fatalf("received %q, want %q", received, payload)
		}
	}

	if err := tunnel.Send(map[string]string{"type": "log_list"}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, "\"type\":\"log_list\"") {
		t.Fatalf("unexpected outbound payload: %q", line)
	}
}

func TestReconnectAndPreservePendingMessages(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tunnel := New(listener.Addr().String())
	if err := tunnel.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer tunnel.Close()

	first, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("{\"type\":\"first\"}\n")); err != nil {
		t.Fatal(err)
	}
	if got := receiveCommand(t, tunnel.Commands()); !strings.Contains(got, "first") {
		t.Fatalf("unexpected first command: %s", got)
	}
	_ = first.Close()

	if err := tunnel.Send(map[string]string{"type": "metrics"}); err != nil {
		t.Fatal(err)
	}
	second, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := bufio.NewReader(second).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, "\"type\":\"metrics\"") {
		t.Fatalf("message did not survive reconnect: %s", line)
	}
}

func TestMalformedCommandsIgnored(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	tunnel := New(listener.Addr().String())
	if err := tunnel.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tunnel.Close()
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("invalid-json\n{\"type\":\"valid\"}\n"))
	if got := receiveCommand(t, tunnel.Commands()); got != "{\"type\":\"valid\"}" {
		t.Fatalf("invalid command was forwarded: %s", got)
	}
}
