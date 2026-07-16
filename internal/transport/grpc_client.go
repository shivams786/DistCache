package transport

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/codex/distcache/internal/cluster"
	"github.com/codex/distcache/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientPool struct {
	selfID  string
	timeout time.Duration
	logger  *slog.Logger
	metrics *metrics.Metrics

	mu      sync.Mutex
	nodes   map[string]cluster.Node
	conns   map[string]*grpc.ClientConn
	clients map[string]CacheNodeClient
}

func NewClientPool(selfID string, nodes []cluster.Node, timeout time.Duration, logger *slog.Logger, m *metrics.Metrics) *ClientPool {
	nodeMap := make(map[string]cluster.Node, len(nodes))
	for _, node := range nodes {
		nodeMap[node.ID] = node
	}
	return &ClientPool{
		selfID:  selfID,
		timeout: timeout,
		logger:  logger,
		metrics: m,
		nodes:   nodeMap,
		conns:   map[string]*grpc.ClientConn{},
		clients: map[string]CacheNodeClient{},
	}
}

func (p *ClientPool) Get(ctx context.Context, nodeID string, req *GetRequest) (*GetResponse, error) {
	start := time.Now()
	client, err := p.client(ctx, nodeID)
	if err != nil {
		p.observe("Get", "error", start)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	resp, err := client.Get(ctx, req)
	p.observe("Get", status(err), start)
	return resp, err
}

func (p *ClientPool) Set(ctx context.Context, nodeID string, req *SetRequest) (*SetResponse, error) {
	start := time.Now()
	client, err := p.client(ctx, nodeID)
	if err != nil {
		p.observe("Set", "error", start)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	resp, err := client.Set(ctx, req)
	p.observe("Set", status(err), start)
	return resp, err
}

func (p *ClientPool) Delete(ctx context.Context, nodeID string, req *DeleteRequest) (*DeleteResponse, error) {
	start := time.Now()
	client, err := p.client(ctx, nodeID)
	if err != nil {
		p.observe("Delete", "error", start)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	resp, err := client.Delete(ctx, req)
	p.observe("Delete", status(err), start)
	return resp, err
}

func (p *ClientPool) Replicate(ctx context.Context, nodeID string, req *ReplicateRequest) (*ReplicateResponse, error) {
	start := time.Now()
	client, err := p.client(ctx, nodeID)
	if err != nil {
		p.observe("Replicate", "error", start)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	resp, err := client.Replicate(ctx, req)
	p.observe("Replicate", status(err), start)
	return resp, err
}

func (p *ClientPool) Health(ctx context.Context, nodeID string, req *HealthRequest) (*HealthResponse, error) {
	start := time.Now()
	client, err := p.client(ctx, nodeID)
	if err != nil {
		p.observe("Health", "error", start)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	resp, err := client.Health(ctx, req)
	p.observe("Health", status(err), start)
	return resp, err
}

func (p *ClientPool) Sync(ctx context.Context, nodeID string, req *SyncRequest) ([]SyncEntry, error) {
	start := time.Now()
	client, err := p.client(ctx, nodeID)
	if err != nil {
		p.observe("Sync", "error", start)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout*5)
	defer cancel()
	stream, err := client.Sync(ctx, req)
	if err != nil {
		p.observe("Sync", "error", start)
		return nil, err
	}
	var entries []SyncEntry
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			p.observe("Sync", "ok", start)
			return entries, nil
		}
		if err != nil {
			p.observe("Sync", "error", start)
			return nil, err
		}
		entries = append(entries, *entry)
	}
}

func (p *ClientPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, conn := range p.conns {
		if err := conn.Close(); err != nil && p.logger != nil {
			p.logger.Warn("failed to close grpc connection", "event", "grpc_close_failed", "node", id, "error", err)
		}
	}
}

func (p *ClientPool) client(ctx context.Context, nodeID string) (CacheNodeClient, error) {
	if nodeID == p.selfID {
		return nil, fmt.Errorf("refusing to create grpc client for self")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if client, ok := p.clients[nodeID]; ok {
		return client, nil
	}
	node, ok := p.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node %q", nodeID)
	}

	dialCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		node.GRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(JSONCodec{})),
	)
	if err != nil {
		return nil, err
	}
	client := NewCacheNodeClient(conn)
	p.conns[nodeID] = conn
	p.clients[nodeID] = client
	return client, nil
}

func (p *ClientPool) observe(method, callStatus string, start time.Time) {
	if p.metrics != nil {
		p.metrics.ObserveGRPC(method, callStatus, time.Since(start))
	}
}

func status(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}
