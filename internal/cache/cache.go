// Package cache provides a simple disk-backed write-ahead queue.
// When remote_write or Loki are unreachable, messages are spooled here
// and retried on the configured interval.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	cfg "github.com/mikeosude/ome-kafka-bridge/internal/config"
	"github.com/mikeosude/ome-kafka-bridge/internal/parser"
)

// EntryKind differentiates cached metric batches from log batches.
type EntryKind string

const (
	EntryKindMetrics EntryKind = "metrics"
	EntryKindLogs    EntryKind = "logs"
)

// CachedEntry wraps a batch with metadata for retry and expiry.
type CachedEntry struct {
	ID        string      `json:"id"`
	Kind      EntryKind   `json:"kind"`
	CreatedAt time.Time   `json:"created_at"`
	Metrics   []parser.MetricSample `json:"metrics,omitempty"`
	Logs      []parser.LogEntry     `json:"logs,omitempty"`
}

// Cache manages on-disk queuing of failed forward attempts.
type Cache struct {
	cfg cfg.CacheConfig
	log *logrus.Logger
	mu  sync.Mutex

	retryTicker *time.Ticker
	done        chan struct{}

	// Callbacks to call when retrying. Set by the pipeline after init.
	OnRetryMetrics func([]parser.MetricSample)
	OnRetryLogs    func([]parser.LogEntry)
}

// New creates a Cache. The cache dir is created if it doesn't exist.
func New(c cfg.CacheConfig, log *logrus.Logger) (*Cache, error) {
	if !c.Enabled {
		return nil, nil //nolint: nilnil
	}
	if err := os.MkdirAll(c.Dir, 0750); err != nil {
		return nil, fmt.Errorf("creating cache dir %s: %w", c.Dir, err)
	}
	cache := &Cache{
		cfg:  c,
		log:  log,
		done: make(chan struct{}),
	}
	cache.retryTicker = time.NewTicker(c.RetryInterval.Duration)
	go cache.retryLoop()
	return cache, nil
}

// StoreMetrics serialises a metric batch to disk.
func (c *Cache) StoreMetrics(samples []parser.MetricSample) error {
	if c == nil {
		return nil
	}
	entry := CachedEntry{
		ID:        fmt.Sprintf("metrics_%d", time.Now().UnixNano()),
		Kind:      EntryKindMetrics,
		CreatedAt: time.Now(),
		Metrics:   samples,
	}
	return c.write(entry)
}

// StoreLogs serialises a log batch to disk.
func (c *Cache) StoreLogs(entries []parser.LogEntry) error {
	if c == nil {
		return nil
	}
	entry := CachedEntry{
		ID:        fmt.Sprintf("logs_%d", time.Now().UnixNano()),
		Kind:      EntryKindLogs,
		CreatedAt: time.Now(),
		Logs:      entries,
	}
	return c.write(entry)
}

func (c *Cache) write(entry CachedEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Enforce max size by dropping oldest files first
	c.evictIfNeeded()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	path := filepath.Join(c.cfg.Dir, entry.ID+".json")
	return os.WriteFile(path, data, 0640)
}

func (c *Cache) evictIfNeeded() {
	var totalBytes int64
	entries, _ := c.listEntries()
	for _, e := range entries {
		info, _ := os.Stat(e)
		if info != nil {
			totalBytes += info.Size()
		}
	}

	maxBytes := c.cfg.MaxSizeMB * 1024 * 1024
	for totalBytes > maxBytes && len(entries) > 0 {
		oldest := entries[0]
		info, _ := os.Stat(oldest)
		if info != nil {
			totalBytes -= info.Size()
		}
		_ = os.Remove(oldest)
		entries = entries[1:]
		c.log.WithField("file", oldest).Warn("cache evicted oldest entry due to size limit")
	}
}

func (c *Cache) retryLoop() {
	for {
		select {
		case <-c.retryTicker.C:
			c.replayAll()
		case <-c.done:
			return
		}
	}
}

func (c *Cache) replayAll() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c.mu.Lock()
	entries, err := c.listEntries()
	c.mu.Unlock()

	if err != nil || len(entries) == 0 {
		return
	}

	cutoff := time.Now().Add(-c.cfg.MaxAge.Duration)

	for _, path := range entries {
		select {
		case <-ctx.Done():
			return
		default:
		}

		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var entry CachedEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			c.log.WithField("file", path).WithError(err).Warn("corrupt cache entry, removing")
			_ = os.Remove(path)
			continue
		}

		if entry.CreatedAt.Before(cutoff) {
			c.log.WithField("file", path).Warn("cache entry expired, discarding")
			_ = os.Remove(path)
			continue
		}

		// Replay
		switch entry.Kind {
		case EntryKindMetrics:
			if c.OnRetryMetrics != nil && len(entry.Metrics) > 0 {
				c.OnRetryMetrics(entry.Metrics)
				_ = os.Remove(path)
			}
		case EntryKindLogs:
			if c.OnRetryLogs != nil && len(entry.Logs) > 0 {
				c.OnRetryLogs(entry.Logs)
				_ = os.Remove(path)
			}
		}
	}
}

// listEntries returns all cached .json files sorted by name (= creation time).
func (c *Cache) listEntries() ([]string, error) {
	pattern := filepath.Join(c.cfg.Dir, "*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// Stats returns a snapshot of cache utilisation.
type Stats struct {
	EntryCount int   `json:"entry_count"`
	TotalBytes int64 `json:"total_bytes_used"`
	MaxBytes   int64 `json:"max_bytes"`
}

func (c *Cache) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	entries, _ := c.listEntries()
	var total int64
	for _, e := range entries {
		info, _ := os.Stat(e)
		if info != nil {
			total += info.Size()
		}
	}
	return Stats{
		EntryCount: len(entries),
		TotalBytes: total,
		MaxBytes:   c.cfg.MaxSizeMB * 1024 * 1024,
	}
}

func (c *Cache) Close() {
	if c == nil {
		return
	}
	c.retryTicker.Stop()
	close(c.done)
}
