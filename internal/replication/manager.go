package replication

import (
	"context"
	"log/slog"
	"time"

	"github.com/codex/distcache/internal/cache"
	"github.com/codex/distcache/internal/events"
	"github.com/codex/distcache/internal/metrics"
	"github.com/codex/distcache/internal/transport"
)

type Operation string

const (
	OperationSet    Operation = "set"
	OperationDelete Operation = "delete"
)

type Task struct {
	Operation Operation
	Key       string
	Value     []byte
	ExpiresAt time.Time
	Target    string
	Source    string
}

type Client interface {
	Replicate(context.Context, string, *transport.ReplicateRequest) (*transport.ReplicateResponse, error)
	Delete(context.Context, string, *transport.DeleteRequest) (*transport.DeleteResponse, error)
}

type Manager struct {
	client  Client
	metrics *metrics.Metrics
	events  *events.Log
	logger  *slog.Logger
	queue   chan Task
	workers int
	timeout time.Duration
	retries int
}

func New(client Client, workers int, queueSize int, timeout time.Duration, logger *slog.Logger, m *metrics.Metrics, eventLog *events.Log) *Manager {
	return NewWithRetries(client, workers, queueSize, timeout, 3, logger, m, eventLog)
}

func NewWithRetries(client Client, workers int, queueSize int, timeout time.Duration, retries int, logger *slog.Logger, m *metrics.Metrics, eventLog *events.Log) *Manager {
	if workers <= 0 {
		workers = 8
	}
	if queueSize <= 0 {
		queueSize = 1000
	}
	if timeout <= 0 {
		timeout = 750 * time.Millisecond
	}
	if retries < 0 {
		retries = 3
	}
	return &Manager{
		client:  client,
		metrics: m,
		events:  eventLog,
		logger:  logger,
		queue:   make(chan Task, queueSize),
		workers: workers,
		timeout: timeout,
		retries: retries,
	}
}

func (m *Manager) Start(ctx context.Context) {
	for i := 0; i < m.workers; i++ {
		go m.worker(ctx)
	}
}

func (m *Manager) Enqueue(task Task) bool {
	select {
	case m.queue <- task:
		return true
	default:
		if m.metrics != nil {
			m.metrics.IncReplicationFailure()
		}
		if m.logger != nil {
			m.logger.Warn("replication queue full", "event", "replication_queue_full", "target", task.Target, "key_hash", cache.KeyHash(task.Key))
		}
		return false
	}
}

func (m *Manager) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-m.queue:
			m.process(ctx, task)
		}
	}
}

func (m *Manager) process(parent context.Context, task Task) {
	var lastErr error
	for attempt := 0; attempt <= m.retries; attempt++ {
		ctx, cancel := context.WithTimeout(parent, m.timeout)
		err := m.send(ctx, task)
		cancel()
		if err == nil {
			if m.metrics != nil {
				m.metrics.IncReplicationSuccess()
			}
			if m.events != nil {
				m.events.Add("info", "replication_success", "replication completed", map[string]string{
					"target":   task.Target,
					"key_hash": cache.KeyHash(task.Key),
					"op":       string(task.Operation),
				})
			}
			return
		}
		lastErr = err
		if attempt == m.retries {
			break
		}
		select {
		case <-parent.Done():
			return
		case <-time.After(retryDelay(attempt)):
		}
	}

	if m.metrics != nil {
		m.metrics.IncReplicationFailure()
	}
	if m.logger != nil {
		m.logger.Warn("replication failed",
			"event", "replication_failed",
			"target", task.Target,
			"key_hash", cache.KeyHash(task.Key),
			"op", task.Operation,
			"error", lastErr,
		)
	}
	if m.events != nil {
		m.events.Add("warn", "replication_failed", "replication failed", map[string]string{
			"target":   task.Target,
			"key_hash": cache.KeyHash(task.Key),
			"op":       string(task.Operation),
		})
	}
}

func retryDelay(attempt int) time.Duration {
	switch attempt {
	case 0:
		return 100 * time.Millisecond
	case 1:
		return 200 * time.Millisecond
	default:
		return 400 * time.Millisecond
	}
}

func (m *Manager) send(ctx context.Context, task Task) error {
	switch task.Operation {
	case OperationDelete:
		_, err := m.client.Delete(ctx, task.Target, &transport.DeleteRequest{
			Key:             task.Key,
			OriginNode:      task.Source,
			HopCount:        1,
			SkipReplication: true,
		})
		return err
	default:
		expiresAt := int64(0)
		if !task.ExpiresAt.IsZero() {
			expiresAt = task.ExpiresAt.UnixNano()
		}
		_, err := m.client.Replicate(ctx, task.Target, &transport.ReplicateRequest{
			Key:               task.Key,
			Value:             task.Value,
			ExpiresAtUnixNano: expiresAt,
			SourceNode:        task.Source,
		})
		return err
	}
}
