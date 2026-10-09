package tunnel

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// Tunnel owns the only reader and writer of its TCP connection.
// Messages accepted into the bounded queue are retried after a disconnect.
// Delivery is at-least-once across uncertain write failures; consumers must
// tolerate duplicates.
type Tunnel struct {
	hostAddr string
	mu       sync.RWMutex
	conn     net.Conn
	sendChan chan []byte
	commands chan json.RawMessage
	ctx      context.Context
	cancel   context.CancelFunc
	start    sync.Once
	close    sync.Once
	wg       sync.WaitGroup
}

func New(hostAddr string) *Tunnel {
	ctx, cancel := context.WithCancel(context.Background())
	return &Tunnel{
		hostAddr: hostAddr,
		sendChan: make(chan []byte, 1000),
		commands: make(chan json.RawMessage, 128),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Connect starts the reconnecting background transport. It does not require
// the remote peer to be available immediately; outgoing messages are buffered.
func (t *Tunnel) Connect(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	t.start.Do(func() {
		stop := context.AfterFunc(ctx, t.cancel)
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer stop()
			t.run()
		}()
	})
	return nil
}

func (t *Tunnel) setConnection(conn net.Conn) {
	t.mu.Lock()
	t.conn = conn
	t.mu.Unlock()
}

func (t *Tunnel) GetConnection() net.Conn {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.conn
}

func (t *Tunnel) Commands() <-chan json.RawMessage {
	return t.commands
}

func (t *Tunnel) run() {
	backoff := 200 * time.Millisecond
	var pending []byte

	for t.ctx.Err() == nil {
		conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(t.ctx, "tcp", t.hostAddr)
		if err != nil {
			if t.ctx.Err() == nil {
				log.Printf("[TUNNEL] connect failed: %v", err)
			}
			if !t.pause(backoff) {
				return
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 200 * time.Millisecond
		t.setConnection(conn)
		disconnected := make(chan struct{})
		go t.readLoop(conn, disconnected)

	connected:
		for t.ctx.Err() == nil {
			if pending == nil {
				select {
				case <-t.ctx.Done():
					break connected
				case <-disconnected:
					break connected
				case pending = <-t.sendChan:
				}
			}
			if pending == nil {
				continue
			}
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			data := append(append([]byte(nil), pending...), '\n')
			for len(data) > 0 {
				n, err := conn.Write(data)
				if err != nil || n == 0 {
					log.Printf("[TUNNEL] write interrupted: %v; will retry after reconnect", err)
					break connected
				}
				data = data[n:]
			}
			pending = nil
		}

		_ = conn.Close()
		<-disconnected
		t.setConnection(nil)
	}
}

func (t *Tunnel) readLoop(conn net.Conn, done chan<- struct{}) {
	defer close(done)
	reader := bufio.NewScanner(conn)
	reader.Buffer(make([]byte, 64*1024), 1024*1024)
	for reader.Scan() {
		raw := append(json.RawMessage(nil), reader.Bytes()...)
		if !json.Valid(raw) {
			log.Print("[TUNNEL] ignoring malformed JSON command")
			continue
		}
		select {
		case t.commands <- raw:
		case <-t.ctx.Done():
			return
		}
	}
	if err := reader.Err(); err != nil && t.ctx.Err() == nil {
		log.Printf("[TUNNEL] connection closed: %v", err)
	}
}

func (t *Tunnel) pause(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-t.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Send accepts a message into a bounded queue; acceptance is not confirmation
// of delivery. The queue is memory-only and is lost on process restart.
func (t *Tunnel) Send(message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode transport message: %w", err)
	}
	select {
	case <-t.ctx.Done():
		return fmt.Errorf("tunnel is closed")
	default:
	}
	select {
	case <-t.ctx.Done():
		return fmt.Errorf("tunnel is closed")
	case t.sendChan <- data:
		return nil
	default:
		return fmt.Errorf("tunnel send queue full")
	}
}

func (t *Tunnel) Close() error {
	t.close.Do(func() {
		t.cancel()
		if conn := t.GetConnection(); conn != nil {
			_ = conn.Close()
		}
		t.wg.Wait()
	})
	return nil
}
