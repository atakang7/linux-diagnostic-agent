package log

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	chunkSize            = 10 * 1024 * 1024 // 10MB chunks for processing
	maxBatchSize         = 1000
	deliveryRateLimit    = 1 * time.Second // Time between sending batches to client
	redisKeyExpiry       = 24 * time.Hour  // How long to keep results in Redis
	defaultChannelBuffer = 200             // Default size for the log channel buffer
)

type LogFile struct {
	Path        string    `json:"path"`
	ParentPath  string    `json:"parent_path"`
	Name        string    `json:"name"`
	IsDirectory bool      `json:"is_directory"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"mod_time"`
	IsGzipped   bool      `json:"is_gzipped"`
}

type NotificationType string

const (
	Warning NotificationType = "warning"
	Error   NotificationType = "error"
	Other   NotificationType = "other"
)

type LogEntry struct {
	Filename  string           `json:"filename"`
	Line      string           `json:"line"`
	LineNum   int              `json:"line_num"`
	Timestamp time.Time        `json:"timestamp"`
	Type      NotificationType `json:"notification_type"`
}

type FileProcessRequest struct {
	Files    []string `json:"files"`
	Keywords []string `json:"keywords"`
}

type CollectorStats struct {
	ProcessedFiles    int64
	ProcessedLines    int64
	BytesProcessed    int64
	BufferedBatches   int64
	DeliveredBatches  int64
	Errors            int64
	LastError         string
	LastErrorTime     time.Time
	CurrentWorkers    int
	ProcessingSpeed   float64
	AverageLatency    float64
	LastMetricsUpdate time.Time
}

type Collector struct {
	redisClient  *redis.Client
	logChan      chan []LogEntry
	knownFiles   map[string]LogFile
	mutex        sync.RWMutex
	lastUpdate   time.Time
	updatePeriod time.Duration
	stats        CollectorStats
	statsMutex   sync.RWMutex
}

func New(redisAddr string) (*Collector, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &Collector{
		redisClient:  rdb,
		logChan:      make(chan []LogEntry, defaultChannelBuffer),
		knownFiles:   make(map[string]LogFile),
		updatePeriod: 1 * time.Minute,
	}, nil
}

func (c *Collector) Start(ctx context.Context) {
	if err := c.updateLogFiles(ctx); err != nil {
		log.Printf("Initial log file discovery failed: %v", err)
	}

	ticker := time.NewTicker(c.updatePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.updateLogFiles(ctx); err != nil {
				log.Printf("Log file discovery failed: %v", err)
			}
		}
	}
}

func (c *Collector) ProcessFiles(ctx context.Context, req FileProcessRequest) error {
	if len(req.Files) == 0 || len(req.Files) > 128 || len(req.Keywords) == 0 || len(req.Keywords) > 16 {
		return fmt.Errorf("search requires 1-128 files and 1-16 keywords")
	}
	// Only files discovered under configured log roots may be queried by the
	// remote service. Never open arbitrary requested paths.
	c.mutex.RLock()
	for _, path := range req.Files {
		entry, ok := c.knownFiles[path]
		if !ok || entry.IsDirectory {
			c.mutex.RUnlock()
			return fmt.Errorf("file is not in the discovered log inventory: %s", path)
		}
	}
	c.mutex.RUnlock()

	searchID := fmt.Sprintf("search:%d", time.Now().UnixNano())
	metaKey := searchID + ":meta"
	meta := map[string]interface{}{
		"status": "processing",
		"start_time": time.Now().UTC().Format(time.RFC3339),
		"files": strings.Join(req.Files, ","),
		"keywords": strings.Join(req.Keywords, ","),
	}
	if err := c.redisClient.HSet(ctx, metaKey, meta).Err(); err != nil {
		return fmt.Errorf("store search metadata: %w", err)
	}
	_ = c.redisClient.Expire(ctx, metaKey, redisKeyExpiry).Err()

	// Bounded per-search workers avoid races between concurrent requests.
	workers := len(req.Files)
	if workers > 4 {
		workers = 4
	}
	c.updateWorkerCount(workers)
	queue := make(chan string)
	errs := make(chan error, len(req.Files))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range queue {
				if err := c.processFile(ctx, name, req.Keywords, searchID); err != nil {
					errs <- fmt.Errorf("process %s: %w", name, err)
				}
			}
		}()
	}
	for _, name := range req.Files {
		select {
		case <-ctx.Done():
			close(queue)
			wg.Wait()
			return ctx.Err()
		case queue <- name:
		}
	}
	close(queue)
	wg.Wait()
	close(errs)

	var failures []string
	for err := range errs {
		failures = append(failures, err.Error())
		c.recordError(err.Error())
	}
	status := "completed"
	if len(failures) > 0 {
		status = "failed"
	}
	_ = c.redisClient.HSet(ctx, metaKey, map[string]interface{}{
		"status": status,
		"end_time": time.Now().UTC().Format(time.RFC3339),
		"errors": strings.Join(failures, "; "),
	}).Err()
	go c.startConsumer(ctx, searchID)
	if len(failures) > 0 {
		return fmt.Errorf("search errors: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (c *Collector) processFile(ctx context.Context, filePath string, keywords []string, searchID string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file
	if strings.HasSuffix(strings.ToLower(filePath), ".gz") {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("open compressed log: %w", err)
		}
		defer gz.Close()
		reader = gz
	}

	var batch []LogEntry
	lineNum := 0
	var bytesScanned int
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, chunkSize), chunkSize)

	// Expensive search
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			lineNum++
			line := scanner.Text()
			bytesScanned += len(line)

			for _, keyword := range keywords {
				if strings.Contains(line, keyword) {
					batch = append(batch, LogEntry{
						Filename:  filePath,
						Line:      line,
						LineNum:   lineNum,
						Timestamp: time.Now(),
						Type:      getNotificationType(keyword),
					})
					break
				}
			}

			if len(batch) >= maxBatchSize {
				if err := c.bufferBatch(ctx, batch, searchID); err != nil {
					return fmt.Errorf("buffer log batch: %w", err)
				}
				batch = make([]LogEntry, 0, maxBatchSize)
			}
		}
	}

	// Buffer remaining entries
	if len(batch) > 0 {
		if err := c.bufferBatch(ctx, batch, searchID); err != nil {
			return fmt.Errorf("buffer final log batch: %w", err)
		}
	}

	c.updateStats(lineNum, bytesScanned)
	return scanner.Err()
}

func getNotificationType(keyword string) NotificationType {
	if keyword == "error" {
		return Error
	} else if keyword == "warning" {
		return Warning
	}
	return Other
}

func (c *Collector) bufferBatch(ctx context.Context, entries []LogEntry, searchID string) error {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(entries); err != nil {
		_ = gz.Close()
		return fmt.Errorf("failed to encode entries: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("finish compressed batch: %w", err)
	}

	key := fmt.Sprintf("%s:results", searchID)
	if err := c.redisClient.RPush(ctx, key, buf.Bytes()).Err(); err != nil {
		return fmt.Errorf("failed to store in Redis: %w", err)
	}

	c.redisClient.Expire(ctx, key, redisKeyExpiry)
	c.updateBufferStats(len(entries))
	return nil
}

func (c *Collector) startConsumer(ctx context.Context, searchID string) {
	throttle := time.NewTicker(deliveryRateLimit)
	defer throttle.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-throttle.C:
			entries, err := c.consumeBatch(ctx, searchID)
			if err == redis.Nil {
				return // all buffered search results delivered; no lingering goroutine
			}
			if err != nil {
				log.Printf("[LOG] failed to deliver search batch: %v", err)
				return
			}
			select {
			case c.logChan <- entries:
				c.updateDeliveryStats(len(entries))
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *Collector) consumeBatch(ctx context.Context, searchID string) ([]LogEntry, error) {
	key := fmt.Sprintf("%s:results", searchID)
	result, err := c.redisClient.LPop(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader([]byte(result))
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gz.Close()

	var entries []LogEntry
	if err := json.NewDecoder(gz).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to decode entries: %w", err)
	}

	return entries, nil
}

func (c *Collector) updateLogFiles(ctx context.Context) error {
	files, err := c.findLogFiles()
	if err != nil {
		return err
	}

	c.mutex.Lock()
	inventory := make(map[string]LogFile, len(files))
	for _, file := range files {
		inventory[file.Path] = file
	}
	c.knownFiles = inventory
	c.lastUpdate = time.Now()
	c.mutex.Unlock()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(files); err != nil {
		return fmt.Errorf("failed to encode files: %w", err)
	}
	gz.Close()

	return c.redisClient.Set(ctx, "log_files", buf.Bytes(), redisKeyExpiry).Err()
}

func (c *Collector) updateStats(lineCount, batchSize int) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.ProcessedLines += int64(lineCount)
	c.stats.BytesProcessed += int64(batchSize)
	c.stats.ProcessedFiles++
}

func (c *Collector) updateBufferStats(count int) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.BufferedBatches++
}

func (c *Collector) updateDeliveryStats(count int) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.DeliveredBatches++
}

func (c *Collector) recordError(errMsg string) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.Errors++
	c.stats.LastError = errMsg
	c.stats.LastErrorTime = time.Now()
}

func (c *Collector) updateWorkerCount(count int) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.CurrentWorkers = count
}

func (c *Collector) updateMetrics(metrics ProcessingMetrics) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.ProcessingSpeed = metrics.ProcessingSpeed
	c.stats.AverageLatency = metrics.BatchLatency
	c.stats.LastMetricsUpdate = time.Now()
}

func (c *Collector) GetStats() CollectorStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

func (c *Collector) GetLogChannel() <-chan []LogEntry {
	return c.logChan
}

func (c *Collector) GetDiscoveredFiles() []LogFile {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	files := make([]LogFile, 0, len(c.knownFiles))
	for _, file := range c.knownFiles {
		if info, err := os.Stat(file.Path); err == nil {
			file.Size = info.Size()
			file.ModTime = info.ModTime()
			files = append(files, file)
		}
	}
	return files
}

// Helper functions for system metrics
func isLogFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".log" || ext == ".txt" || strings.HasSuffix(strings.ToLower(path), ".log.gz")
}

func (c *Collector) findLogFiles() ([]LogFile, error) {
	roots := []string{"/var/log"}
	if configured := os.Getenv("LOG_ROOTS"); configured != "" {
		roots = strings.Split(configured, string(os.PathListSeparator))
	}
	var files []LogFile
	for _, root := range roots {
		if strings.TrimSpace(root) == "" {
			continue
		}
		root, err := filepath.Abs(root)
		if err != nil {
			return nil, err
		}
		err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if path == root {
					return walkErr
				}
				log.Printf("[LOG] skipping inaccessible path %s: %v", path, walkErr)
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return nil
			}
			if !entry.IsDir() && (!entry.Type().IsRegular() || !isLogFile(path)) {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return nil
			}
			parent := filepath.Dir(path)
			files = append(files, LogFile{
				Path: path,
				ParentPath: parent,
				Name: entry.Name(),
				IsDirectory: entry.IsDir(),
				Size: info.Size(),
				ModTime: info.ModTime(),
				IsGzipped: strings.HasSuffix(strings.ToLower(path), ".gz"),
			})
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("discover logs under %s: %w", root, err)
		}
	}
	return files, nil
}
