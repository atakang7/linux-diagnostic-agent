// internal/agent/agent.go
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	logcollect "diagnostic-agent/internal/log"
	"diagnostic-agent/internal/network"
	"diagnostic-agent/internal/tunnel"
)

type MessageType string

const (
	TypeMetrics   MessageType = "metrics"
	TypeLogList   MessageType = "log_list"
	TypeLogData   MessageType = "log_data"
	TypeLogSearch MessageType = "log_search"
	TypeCommand   MessageType = "command"
)

type Message struct {
	Type    MessageType `json:"type"`
	Payload interface{} `json:"payload"`
}

type LogSearchCommand struct {
	Files    []string `json:"files"`
	Keywords []string `json:"keywords"`
}

type AgentStats struct {
	ActiveSearches    int64
	CompletedSearches int64
	FailedSearches    int64
	MessagesSent      int64
	BytesSent         int64
	LastError         string
	LastErrorTime     time.Time
}

type Agent struct {
	networkCollector *network.Collector
	logCollector     *logcollect.Collector
	tunnel           *tunnel.Tunnel
	redisAddr        string
	activeSearches   sync.WaitGroup
	stats            AgentStats
	statsMutex       sync.RWMutex
}

func New(hostAddr, redisAddr string) (*Agent, error) {
	networkCollector, err := network.New(redisAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create network collector: %w", err)
	}

	logCollector, err := logcollect.New(redisAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create log collector: %w", err)
	}

	return &Agent{
		networkCollector: networkCollector,
		logCollector:     logCollector,
		tunnel:           tunnel.New(hostAddr),
		redisAddr:        redisAddr,
	}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	log.Println(`
	_____
   /     \
  /       \
 /_________\
|          |
|  Diagnostic |
|   Agent   |
|__________|
	`)

	// Connect tunnel
	if err := a.tunnel.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect tunnel: %w", err)
	}
	defer a.tunnel.Close()

	// The initial inventory must exist before the first log_list and before
	// processing remote searches; otherwise the first update may take minutes.
	if err := a.logCollector.Refresh(ctx); err != nil {
		return fmt.Errorf("discover initial logs: %w", err)
	}

	log.Println("Starting collectors...")
	go a.logCollector.Start(ctx)
	go a.networkCollector.Start(ctx)
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		a.handleCommands(ctx)
	}()

	go func() {
		defer wg.Done()
		a.handleNetworkMetrics(ctx)
	}()

	go func() {
		defer wg.Done()
		a.handleLogs(ctx)
	}()

	wg.Wait()
	return nil
}

func (a *Agent) handleCommands(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case raw := <-a.tunnel.Commands():
			var msg struct {
				Type MessageType `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(raw, &msg); err != nil {
				log.Printf("[AGENT] invalid command: %v", err)
				continue
			}
			switch msg.Type {
			case TypeLogSearch:
				var cmd LogSearchCommand
				if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
					a.recordError(err)
					continue
				}
				if len(cmd.Files) == 0 || len(cmd.Files) > 128 ||
					len(cmd.Keywords) == 0 || len(cmd.Keywords) > 16 {
					log.Printf("[AGENT] rejecting log search with invalid file/keyword count")
					continue
				}
				a.handleSearchCommand(ctx, cmd)
			default:
				log.Printf("[AGENT] ignoring unsupported command: %s", msg.Type)
			}
		}
	}
}

func (a *Agent) handleSearchCommand(ctx context.Context, cmd LogSearchCommand) {
	a.updateStats(func(s *AgentStats) {
		s.ActiveSearches++
	})

	a.activeSearches.Add(1)
	go func() {
		defer a.activeSearches.Done()
		defer a.updateStats(func(s *AgentStats) {
			s.ActiveSearches--
		})
		log.Printf("[AGENT] searching %d files", len(cmd.Files))
		req := logcollect.FileProcessRequest{
			Files:    cmd.Files,
			Keywords: cmd.Keywords,
		}

		if err := a.logCollector.ProcessFiles(ctx, req); err != nil {
			log.Printf("[AGENT][ERROR] File processing failed: %v", err)
			a.recordError(err)
			a.updateStats(func(s *AgentStats) {
				s.FailedSearches++
			})
		} else {
			a.updateStats(func(s *AgentStats) {
				s.CompletedSearches++
			})
		}
	}()
}

func (a *Agent) handleNetworkMetrics(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case metrics := <-a.networkCollector.Metrics():
			msg := Message{
				Type:    TypeMetrics,
				Payload: metrics,
			}
			if err := a.tunnel.Send(msg); err != nil {
				log.Printf("[AGENT][ERROR] Failed to send network metrics: %v", err)
				a.recordError(err)
			} else {
				if data, err := json.Marshal(metrics); err == nil {
					a.updateStats(func(s *AgentStats) {
						s.MessagesSent++
						s.BytesSent += int64(len(data))
					})
				}
			}
		}
	}
}

func (a *Agent) handleLogs(ctx context.Context) {
	log.Println("[AGENT] Starting log handler")
	logChan := a.logCollector.GetLogChannel()
	discoverTicker := time.NewTicker(5 * time.Minute)
	defer discoverTicker.Stop()

	// Send initial file list
	if files := a.logCollector.GetDiscoveredFiles(); len(files) > 0 {
		log.Printf("[AGENT] Sending initial file list: found %d files", len(files))
		msg := Message{
			Type:    TypeLogList,
			Payload: files,
		}
		if err := a.tunnel.Send(msg); err != nil {
			log.Printf("[AGENT][ERROR] Failed to send initial file list: %v", err)
			a.recordError(err)
		} else {
			a.updateStats(func(s *AgentStats) {
				s.MessagesSent++
			})
		}
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[AGENT] Log handler shutting down...")
			// Wait for active searches to complete
			a.activeSearches.Wait()
			return

		case logEntries := <-logChan:
			if len(logEntries) == 0 {
				continue
			}

			msg := Message{
				Type:    TypeLogData,
				Payload: logEntries,
			}

			// Try to send with exponential backoff
			maxRetries := 3
			for attempt := 0; attempt < maxRetries; attempt++ {
				if err := a.tunnel.Send(msg); err != nil {
					if attempt == maxRetries-1 {
						log.Printf("[AGENT][ERROR] Failed to send log entries after %d attempts: %v", maxRetries, err)
						a.recordError(err)
						break
					}
					backoff := time.Duration(1<<attempt) * time.Second
					time.Sleep(backoff)
					continue
				}

				if data, err := json.Marshal(logEntries); err == nil {
					a.updateStats(func(s *AgentStats) {
						s.MessagesSent++
						s.BytesSent += int64(len(data))
					})
				}
				break
			}

		case <-discoverTicker.C:
			files := a.logCollector.GetDiscoveredFiles()
			msg := Message{
				Type:    TypeLogList,
				Payload: files,
			}
			if err := a.tunnel.Send(msg); err != nil {
				log.Printf("[AGENT][ERROR] Failed to send file list update: %v", err)
				a.recordError(err)
			} else {
				if data, err := json.Marshal(files); err == nil {
					a.updateStats(func(s *AgentStats) {
						s.MessagesSent++
						s.BytesSent += int64(len(data))
					})
				}
			}

			// Log periodic stats
			stats := a.GetStats()
			collectorStats := a.logCollector.GetStats()
			log.Printf(`[AGENT] Stats:
				Active Searches: %d
				Completed Searches: %d
				Failed Searches: %d
				Messages Sent: %d
				Bytes Sent: %d
				Processed Lines: %d
				Buffered Batches: %d
				Delivered Batches: %d`,
				stats.ActiveSearches,
				stats.CompletedSearches,
				stats.FailedSearches,
				stats.MessagesSent,
				stats.BytesSent,
				collectorStats.ProcessedLines,
				collectorStats.BufferedBatches,
				collectorStats.DeliveredBatches,
			)
		}
	}
}

// Stats management
func (a *Agent) updateStats(updater func(*AgentStats)) {
	a.statsMutex.Lock()
	defer a.statsMutex.Unlock()
	updater(&a.stats)
}

func (a *Agent) recordError(err error) {
	a.statsMutex.Lock()
	defer a.statsMutex.Unlock()
	a.stats.LastError = err.Error()
	a.stats.LastErrorTime = time.Now()
}

func (a *Agent) GetStats() AgentStats {
	a.statsMutex.RLock()
	defer a.statsMutex.RUnlock()
	return a.stats
}
