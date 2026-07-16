package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/codex/distcache/internal/cache"
	"github.com/codex/distcache/internal/cluster"
	"github.com/codex/distcache/internal/config"
	"github.com/codex/distcache/internal/dashboard"
	"github.com/codex/distcache/internal/events"
	"github.com/codex/distcache/internal/metrics"
	"github.com/codex/distcache/internal/replication"
	"github.com/codex/distcache/internal/transport"
	"google.golang.org/grpc"
)

type App struct {
	cfg         config.Config
	logger      *slog.Logger
	cache       *cache.Cache
	membership  *cluster.Membership
	metrics     *metrics.Metrics
	events      *events.Log
	clients     *transport.ClientPool
	replication *replication.Manager
	recoverySem chan struct{}

	httpServer *http.Server
	grpcServer *grpc.Server
	once       sync.Once
}

func New(cfg config.Config, logger *slog.Logger) (*App, error) {
	cfg = normalizeConfig(cfg)
	m := metrics.New(cfg.NodeID)
	eventLog := events.New(128)
	c := cache.New(cache.Config{
		MaxEntries:      cfg.CacheMaxEntries,
		CleanupInterval: cfg.CleanupInterval,
		Logger:          logger,
	})
	membership := cluster.NewMembershipWithThresholds(
		cfg.NodeID,
		cfg.ClusterNodes,
		cfg.VirtualNodes,
		cfg.HealthFailureThreshold,
		cfg.HealthRecoveryThreshold,
	)
	for _, status := range membership.Snapshot() {
		m.SetNodeHealth(status.Node.ID, status.Healthy)
	}
	clients := transport.NewClientPool(cfg.NodeID, cfg.ClusterNodes, cfg.RequestTimeout, logger, m)
	replicationManager := replication.NewWithRetries(
		clients,
		cfg.ReplicationWorkers,
		cfg.ReplicationQueue,
		cfg.RequestTimeout,
		cfg.ReplicationRetries,
		logger,
		m,
		eventLog,
	)
	return &App{
		cfg:         cfg,
		logger:      logger,
		cache:       c,
		membership:  membership,
		metrics:     m,
		events:      eventLog,
		clients:     clients,
		replication: replicationManager,
		recoverySem: make(chan struct{}, cfg.MaxConcurrentRecovery),
	}, nil
}

func normalizeConfig(cfg config.Config) config.Config {
	if cfg.CacheMaxEntries <= 0 {
		cfg.CacheMaxEntries = 10000
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 300 * time.Second
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = 5 * time.Second
	}
	if cfg.MaxValueBytes <= 0 {
		cfg.MaxValueBytes = 1024 * 1024
	}
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = 100
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 2
	}
	if cfg.ReplicationWorkers <= 0 {
		cfg.ReplicationWorkers = 8
	}
	if cfg.ReplicationQueue <= 0 {
		cfg.ReplicationQueue = 1000
	}
	if cfg.ReplicationRetries < 0 {
		cfg.ReplicationRetries = 3
	}
	if cfg.HealthCheckInterval <= 0 {
		cfg.HealthCheckInterval = 2 * time.Second
	}
	if cfg.HealthCheckTimeout <= 0 {
		cfg.HealthCheckTimeout = 500 * time.Millisecond
	}
	if cfg.HealthFailureThreshold <= 0 {
		cfg.HealthFailureThreshold = 3
	}
	if cfg.HealthRecoveryThreshold <= 0 {
		cfg.HealthRecoveryThreshold = 2
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 750 * time.Millisecond
	}
	if cfg.GracefulShutdownTimeout <= 0 {
		cfg.GracefulShutdownTimeout = 10 * time.Second
	}
	if cfg.RecoverySyncBatchSize <= 0 {
		cfg.RecoverySyncBatchSize = 250
	}
	if cfg.RecoverySyncTimeout <= 0 {
		cfg.RecoverySyncTimeout = 30 * time.Second
	}
	if cfg.MaxConcurrentRecovery <= 0 {
		cfg.MaxConcurrentRecovery = 2
	}
	if cfg.HTTPReadTimeout <= 0 {
		cfg.HTTPReadTimeout = 3 * time.Second
	}
	if cfg.HTTPWriteTimeout <= 0 {
		cfg.HTTPWriteTimeout = 5 * time.Second
	}
	if cfg.HTTPIdleTimeout <= 0 {
		cfg.HTTPIdleTimeout = 30 * time.Second
	}
	if cfg.MaxForwardingHops <= 0 {
		cfg.MaxForwardingHops = 1
	}
	return cfg
}

func (a *App) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	httpListener, err := net.Listen("tcp", a.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	grpcListener, err := net.Listen("tcp", a.cfg.GRPCAddr)
	if err != nil {
		_ = httpListener.Close()
		return fmt.Errorf("listen grpc: %w", err)
	}

	a.grpcServer = grpc.NewServer()
	transport.RegisterCacheNodeServer(a.grpcServer, a)
	a.httpServer = &http.Server{
		Handler:      a.routes(),
		ReadTimeout:  a.cfg.HTTPReadTimeout,
		WriteTimeout: a.cfg.HTTPWriteTimeout,
		IdleTimeout:  a.cfg.HTTPIdleTimeout,
	}

	go a.cache.StartCleanup(runCtx)
	a.replication.Start(runCtx)
	go a.runHealthChecks(runCtx)

	errCh := make(chan error, 2)
	go func() {
		a.logger.Info("grpc server started", "event", "node_startup", "grpc_addr", a.cfg.GRPCAddr)
		if err := a.grpcServer.Serve(grpcListener); err != nil {
			errCh <- err
		}
	}()
	go func() {
		a.logger.Info("http server started", "event", "node_startup", "http_addr", a.cfg.HTTPAddr)
		if err := a.httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	a.events.Add("info", "node_startup", "node started", map[string]string{"node": a.cfg.NodeID})

	select {
	case <-ctx.Done():
		a.shutdown()
		return ctx.Err()
	case err := <-errCh:
		a.shutdown()
		return err
	}
}

func (a *App) shutdown() {
	a.once.Do(func() {
		a.logger.Info("node shutting down", "event", "node_shutdown")
		ctx, cancel := context.WithTimeout(context.Background(), a.cfg.GracefulShutdownTimeout)
		defer cancel()
		if a.httpServer != nil {
			_ = a.httpServer.Shutdown(ctx)
		}
		if a.grpcServer != nil {
			stopped := make(chan struct{})
			go func() {
				a.grpcServer.GracefulStop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-ctx.Done():
				a.grpcServer.Stop()
			}
		}
		a.clients.Close()
	})
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/", a.handleCache)
	mux.HandleFunc("/cluster/health", a.handleClusterHealth)
	mux.HandleFunc("/health/live", a.handleLive)
	mux.HandleFunc("/health/ready", a.handleReady)
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/metrics", a.handleMetrics)
	mux.HandleFunc("/admin/api/cluster", a.handleAdminCluster)
	mux.HandleFunc("/admin/api/stats", a.handleAdminStats)
	mux.HandleFunc("/admin/api/events", a.handleAdminEvents)
	mux.HandleFunc("/admin", dashboard.Serve)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/admin", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})
	return mux
}

func (a *App) handleCache(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	operation := strings.ToLower(r.Method)
	status := "ok"
	defer func() {
		a.metrics.IncRequest(operation, status)
		a.metrics.ObserveRequest(operation, status, time.Since(start))
	}()

	key, err := cacheKey(r.URL.Path)
	if err != nil {
		status = "bad_request"
		writeError(w, http.StatusBadRequest, err)
		return
	}

	switch r.Method {
	case http.MethodPut:
		a.handleSet(w, r, key, &status)
	case http.MethodGet:
		a.handleGet(w, r, key, &status)
	case http.MethodDelete:
		a.handleDelete(w, r, key, &status)
	default:
		status = "method_not_allowed"
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (a *App) handleSet(w http.ResponseWriter, r *http.Request, key string, status *string) {
	var payload struct {
		Value      json.RawMessage `json:"value"`
		TTLSeconds int64           `json:"ttl_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		*status = "bad_request"
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(payload.Value) == 0 {
		*status = "bad_request"
		writeError(w, http.StatusBadRequest, errors.New("value is required"))
		return
	}
	value := decodeValue(payload.Value)
	if len(value) > a.cfg.MaxValueBytes {
		*status = "payload_too_large"
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Errorf("value exceeds maximum size of %d bytes", a.cfg.MaxValueBytes))
		return
	}
	expiresAt := time.Time{}
	if payload.TTLSeconds > 0 {
		expiresAt = time.Now().UTC().Add(time.Duration(payload.TTLSeconds) * time.Second)
	} else {
		expiresAt = time.Now().UTC().Add(a.cfg.DefaultTTL)
	}

	owner, primary, failover := a.membership.WriteOwner(key, a.cfg.ReplicationFactor)
	if failover {
		a.recordFailover(key, primary, owner)
	}
	if owner == a.cfg.NodeID {
		resp := a.applySet(key, value, expiresAt, owner, false)
		writeJSON(w, http.StatusOK, resp)
		return
	}

	resp, err := a.clients.Set(r.Context(), owner, &transport.SetRequest{
		Key:               key,
		Value:             value,
		TTLSeconds:        payload.TTLSeconds,
		ExpiresAtUnixNano: unixNano(expiresAt),
		OriginNode:        a.cfg.NodeID,
		HopCount:          1,
	})
	if err != nil {
		a.membership.RecordHealth(owner, false, 0, 0)
		if fallback, ok := a.fallbackOwner(key, owner); ok {
			a.recordFailover(key, owner, fallback)
			if fallback == a.cfg.NodeID {
				resp := a.applySet(key, value, expiresAt, fallback, false)
				writeJSON(w, http.StatusOK, resp)
				return
			}
			resp, err = a.clients.Set(r.Context(), fallback, &transport.SetRequest{
				Key:               key,
				Value:             value,
				TTLSeconds:        payload.TTLSeconds,
				ExpiresAtUnixNano: unixNano(expiresAt),
				OriginNode:        a.cfg.NodeID,
				HopCount:          1,
			})
			if err == nil {
				resp.PrimaryNode = fallback
				writeJSON(w, http.StatusOK, resp)
				return
			}
		}
		*status = "bad_gateway"
		writeError(w, http.StatusBadGateway, err)
		return
	}
	resp.PrimaryNode = owner
	writeJSON(w, http.StatusOK, resp)
}

func (a *App) handleGet(w http.ResponseWriter, r *http.Request, key string, status *string) {
	for _, owner := range a.membership.ReadOwners(key, a.cfg.ReplicationFactor) {
		if owner == a.cfg.NodeID {
			resp := a.localGet(key)
			if resp.Found {
				writeGetJSON(w, resp)
				return
			}
			continue
		}
		resp, err := a.clients.Get(r.Context(), owner, &transport.GetRequest{
			Key:        key,
			OriginNode: a.cfg.NodeID,
			HopCount:   1,
		})
		if err != nil {
			a.membership.RecordHealth(owner, false, 0, 0)
			continue
		}
		if resp.Found {
			writeGetJSON(w, resp)
			return
		}
	}
	*status = "miss"
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "found": false})
}

func (a *App) handleDelete(w http.ResponseWriter, r *http.Request, key string, status *string) {
	owner, primary, failover := a.membership.WriteOwner(key, a.cfg.ReplicationFactor)
	if failover {
		a.recordFailover(key, primary, owner)
	}
	if owner == a.cfg.NodeID {
		resp := a.applyDelete(key, owner, false)
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp, err := a.clients.Delete(r.Context(), owner, &transport.DeleteRequest{
		Key:        key,
		OriginNode: a.cfg.NodeID,
		HopCount:   1,
	})
	if err != nil {
		a.membership.RecordHealth(owner, false, 0, 0)
		if fallback, ok := a.fallbackOwner(key, owner); ok {
			a.recordFailover(key, owner, fallback)
			if fallback == a.cfg.NodeID {
				resp := a.applyDelete(key, fallback, false)
				writeJSON(w, http.StatusOK, resp)
				return
			}
			resp, err = a.clients.Delete(r.Context(), fallback, &transport.DeleteRequest{
				Key:        key,
				OriginNode: a.cfg.NodeID,
				HopCount:   1,
			})
			if err == nil {
				resp.PrimaryNode = fallback
				writeJSON(w, http.StatusOK, resp)
				return
			}
		}
		*status = "bad_gateway"
		writeError(w, http.StatusBadGateway, err)
		return
	}
	resp.PrimaryNode = owner
	writeJSON(w, http.StatusOK, resp)
}

func (a *App) localGet(key string) *transport.GetResponse {
	entry, found := a.cache.Get(key)
	if !found {
		a.logger.Info("cache miss", "event", "cache_miss", "key_hash", cache.KeyHash(key))
		return &transport.GetResponse{Key: key, Found: false, ServedBy: a.cfg.NodeID}
	}
	a.logger.Info("cache hit", "event", "cache_hit", "key_hash", cache.KeyHash(key))
	return &transport.GetResponse{
		Key:               key,
		Value:             entry.Value,
		Found:             true,
		ServedBy:          a.cfg.NodeID,
		ExpiresAtUnixNano: unixNano(entry.ExpiresAt),
	}
}

func (a *App) applySet(key string, value []byte, expiresAt time.Time, primary string, skipReplication bool) *transport.SetResponse {
	a.cache.SetWithExpiration(key, value, expiresAt)
	repQueued := false
	if !skipReplication {
		if replica, ok := a.membership.ReplicaFor(key, a.cfg.NodeID); ok {
			repQueued = a.replication.Enqueue(replication.Task{
				Operation: replication.OperationSet,
				Key:       key,
				Value:     append([]byte(nil), value...),
				ExpiresAt: expiresAt,
				Target:    replica,
				Source:    a.cfg.NodeID,
			})
		}
	}
	a.logger.Info("cache set",
		"event", "cache_set",
		"key_hash", cache.KeyHash(key),
		"primary_node", primary,
		"replication_queued", repQueued,
	)
	a.events.Add("info", "cache_set", "key stored", map[string]string{"key_hash": cache.KeyHash(key), "primary": primary})
	return &transport.SetResponse{Key: key, Stored: true, PrimaryNode: primary, ReplicationQueued: repQueued}
}

func (a *App) applyDelete(key string, primary string, skipReplication bool) *transport.DeleteResponse {
	deleted := a.cache.Delete(key)
	repQueued := false
	if !skipReplication {
		if replica, ok := a.membership.ReplicaFor(key, a.cfg.NodeID); ok {
			repQueued = a.replication.Enqueue(replication.Task{
				Operation: replication.OperationDelete,
				Key:       key,
				Target:    replica,
				Source:    a.cfg.NodeID,
			})
		}
	}
	a.logger.Info("cache delete",
		"event", "cache_delete",
		"key_hash", cache.KeyHash(key),
		"primary_node", primary,
		"deleted", deleted,
		"replication_queued", repQueued,
	)
	a.events.Add("info", "cache_delete", "key deleted", map[string]string{"key_hash": cache.KeyHash(key), "primary": primary})
	return &transport.DeleteResponse{Key: key, Deleted: deleted, PrimaryNode: primary, ReplicationQueued: repQueued}
}

func (a *App) recordFailover(key, primary, owner string) {
	a.metrics.IncFailover()
	a.logger.Warn("failover routing",
		"event", "failover",
		"key_hash", cache.KeyHash(key),
		"primary_node", primary,
		"fallback_node", owner,
	)
	a.events.Add("warn", "failover", "request routed to healthy fallback", map[string]string{
		"key_hash": cache.KeyHash(key),
		"primary":  primary,
		"fallback": owner,
	})
}

func (a *App) fallbackOwner(key, failed string) (string, bool) {
	for _, candidate := range a.membership.AllOwners(key) {
		if candidate != failed && a.membership.Healthy(candidate) {
			return candidate, true
		}
	}
	return "", false
}

func (a *App) handleClusterHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id": a.cfg.NodeID,
		"nodes":   a.membership.Snapshot(),
	})
}

func (a *App) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id": a.cfg.NodeID,
		"healthy": true,
	})
}

func (a *App) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id": a.cfg.NodeID,
		"live":    true,
	})
}

func (a *App) handleReady(w http.ResponseWriter, _ *http.Request) {
	statuses := a.membership.Snapshot()
	healthy := 0
	for _, status := range statuses {
		if status.Healthy {
			healthy++
		}
	}
	httpStatus := http.StatusOK
	if healthy == 0 {
		httpStatus = http.StatusServiceUnavailable
	}
	writeJSON(w, httpStatus, map[string]any{
		"node_id":       a.cfg.NodeID,
		"ready":         healthy > 0,
		"healthy_nodes": healthy,
		"known_nodes":   len(statuses),
	})
}

func (a *App) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(a.metrics.Render(a.cache.Stats())))
}

func (a *App) handleAdminCluster(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id": a.cfg.NodeID,
		"nodes":   a.membership.Snapshot(),
	})
}

func (a *App) handleAdminStats(w http.ResponseWriter, _ *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":        a.cfg.NodeID,
		"cache":          a.cache.Stats(),
		"metrics":        a.metrics.Snapshot(),
		"requests_total": a.metrics.RequestCount(),
		"memory": map[string]uint64{
			"alloc_bytes":       mem.Alloc,
			"sys_bytes":         mem.Sys,
			"heap_inuse_bytes":  mem.HeapInuse,
			"heap_objects":      mem.HeapObjects,
			"total_alloc_bytes": mem.TotalAlloc,
		},
	})
}

func (a *App) handleAdminEvents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"events": a.events.List()})
}

func (a *App) runHealthChecks(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.HealthCheckInterval)
	defer ticker.Stop()

	a.checkPeers(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.checkPeers(ctx)
		}
	}
}

func (a *App) checkPeers(ctx context.Context) {
	a.membership.MarkSelf(a.cache.Size(), a.metrics.RequestCount())
	for _, status := range a.membership.Snapshot() {
		a.metrics.SetNodeHealth(status.Node.ID, status.Healthy)
	}
	for _, node := range a.membership.Nodes() {
		if node.ID == a.cfg.NodeID {
			continue
		}
		checkCtx, cancel := context.WithTimeout(ctx, a.cfg.HealthCheckTimeout)
		resp, err := a.clients.Health(checkCtx, node.ID, &transport.HealthRequest{RequesterNode: a.cfg.NodeID})
		cancel()
		ok := err == nil && resp != nil && resp.Healthy
		var entries int64
		var requests uint64
		if resp != nil {
			entries = resp.CacheEntries
			requests = resp.RequestCount
		}
		changed, healthy := a.membership.RecordHealth(node.ID, ok, entries, requests)
		a.metrics.SetNodeHealth(node.ID, healthy)
		if changed {
			level := "warn"
			logLevel := slog.LevelWarn
			event := "node_unhealthy"
			message := "node marked unhealthy"
			if healthy {
				level = "info"
				logLevel = slog.LevelInfo
				event = "node_recovery"
				message = "node recovered"
			}
			a.logger.Log(ctx, logLevel, message, "event", event, "node", node.ID, "healthy", healthy)
			a.events.Add(level, event, message, map[string]string{"node": node.ID})
			if healthy {
				go a.recoverNode(context.Background(), node.ID)
			}
		}
	}
}

func (a *App) recoverNode(ctx context.Context, nodeID string) {
	select {
	case a.recoverySem <- struct{}{}:
		defer func() { <-a.recoverySem }()
	default:
		a.logger.Warn("recovery skipped because limit is reached", "event", "recovery_limit_reached", "target", nodeID)
		a.events.Add("warn", "recovery_limit_reached", "maximum concurrent recovery operations reached", map[string]string{"target": nodeID})
		return
	}

	recoveryCtx, cancel := context.WithTimeout(ctx, a.cfg.RecoverySyncTimeout)
	defer cancel()

	entries := a.cache.Snapshot()
	copied := 0
	scanned := 0
	for _, entry := range entries {
		select {
		case <-recoveryCtx.Done():
			a.logger.Warn("node synchronization timed out", "event", "sync_timeout", "target", nodeID, "entries", copied)
			a.events.Add("warn", "sync_timeout", "node synchronization timed out", map[string]string{
				"target":  nodeID,
				"entries": fmt.Sprint(copied),
			})
			return
		default:
		}
		if !a.membership.ShouldStoreOnNode(entry.Key, nodeID, a.cfg.ReplicationFactor) {
			continue
		}
		_, err := a.clients.Replicate(recoveryCtx, nodeID, &transport.ReplicateRequest{
			Key:               entry.Key,
			Value:             entry.Value,
			ExpiresAtUnixNano: unixNano(entry.ExpiresAt),
			SourceNode:        a.cfg.NodeID,
		})
		if err == nil {
			copied++
		}
		scanned++
		if scanned%a.cfg.RecoverySyncBatchSize == 0 {
			time.Sleep(0)
		}
	}
	a.logger.Info("node synchronization completed", "event", "sync_completed", "target", nodeID, "entries", copied)
	a.events.Add("info", "sync_completed", "node synchronization completed", map[string]string{
		"target":  nodeID,
		"entries": fmt.Sprint(copied),
	})
}

func (a *App) Get(_ context.Context, req *transport.GetRequest) (*transport.GetResponse, error) {
	if err := a.checkHop(req.HopCount); err != nil {
		return nil, err
	}
	return a.localGet(req.Key), nil
}

func (a *App) Set(_ context.Context, req *transport.SetRequest) (*transport.SetResponse, error) {
	if err := a.checkHop(req.HopCount); err != nil {
		return nil, err
	}
	if len(req.Value) > a.cfg.MaxValueBytes {
		return nil, fmt.Errorf("value exceeds maximum size of %d bytes", a.cfg.MaxValueBytes)
	}
	expiresAt := timeFromUnix(req.ExpiresAtUnixNano)
	if expiresAt.IsZero() && req.TTLSeconds > 0 {
		expiresAt = time.Now().UTC().Add(time.Duration(req.TTLSeconds) * time.Second)
	} else if expiresAt.IsZero() {
		expiresAt = time.Now().UTC().Add(a.cfg.DefaultTTL)
	}
	return a.applySet(req.Key, req.Value, expiresAt, a.cfg.NodeID, req.SkipReplication), nil
}

func (a *App) Delete(_ context.Context, req *transport.DeleteRequest) (*transport.DeleteResponse, error) {
	if err := a.checkHop(req.HopCount); err != nil {
		return nil, err
	}
	return a.applyDelete(req.Key, a.cfg.NodeID, req.SkipReplication), nil
}

func (a *App) Replicate(_ context.Context, req *transport.ReplicateRequest) (*transport.ReplicateResponse, error) {
	if len(req.Value) > a.cfg.MaxValueBytes {
		return nil, fmt.Errorf("value exceeds maximum size of %d bytes", a.cfg.MaxValueBytes)
	}
	a.cache.SetWithExpiration(req.Key, req.Value, timeFromUnix(req.ExpiresAtUnixNano))
	a.logger.Info("replica stored key",
		"event", "cache_replicated",
		"key_hash", cache.KeyHash(req.Key),
		"source_node", req.SourceNode,
	)
	a.events.Add("info", "cache_replicated", "replica stored key", map[string]string{
		"key_hash": cache.KeyHash(req.Key),
		"source":   req.SourceNode,
	})
	return &transport.ReplicateResponse{Key: req.Key, Stored: true, NodeID: a.cfg.NodeID}, nil
}

func (a *App) checkHop(hopCount int32) error {
	if int(hopCount) > a.cfg.MaxForwardingHops {
		return fmt.Errorf("maximum forwarding hops exceeded: %d", hopCount)
	}
	return nil
}

func (a *App) Health(_ context.Context, _ *transport.HealthRequest) (*transport.HealthResponse, error) {
	return &transport.HealthResponse{
		NodeID:       a.cfg.NodeID,
		Healthy:      true,
		CacheEntries: int64(a.cache.Size()),
		RequestCount: a.metrics.RequestCount(),
		TimeUnixNano: time.Now().UTC().UnixNano(),
	}, nil
}

func (a *App) Sync(_ *transport.SyncRequest, stream transport.CacheNode_SyncServer) error {
	for _, entry := range a.cache.Snapshot() {
		if err := stream.Send(&transport.SyncEntry{
			Key:               entry.Key,
			Value:             entry.Value,
			ExpiresAtUnixNano: unixNano(entry.ExpiresAt),
		}); err != nil {
			return err
		}
	}
	return nil
}

func cacheKey(path string) (string, error) {
	key := strings.TrimPrefix(path, "/cache/")
	if key == "" || key == path {
		return "", errors.New("cache key is required")
	}
	decoded, err := url.PathUnescape(key)
	if err != nil {
		return "", err
	}
	if decoded == "" {
		return "", errors.New("cache key is required")
	}
	return decoded, nil
}

func decodeValue(raw json.RawMessage) []byte {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return []byte(text)
	}
	return append([]byte(nil), raw...)
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func timeFromUnix(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeGetJSON(w http.ResponseWriter, resp *transport.GetResponse) {
	payload := map[string]any{
		"key":   resp.Key,
		"found": resp.Found,
	}
	if resp.Found {
		payload["value"] = string(resp.Value)
		payload["served_by"] = resp.ServedBy
	}
	writeJSON(w, http.StatusOK, payload)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}
