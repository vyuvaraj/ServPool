package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/vyuvaraj/ServShared"
	"servdb/pkg/analytics"
	"servdb/pkg/migration"
	"servdb/pkg/pool"
)

// QueryRequest is the HTTP request body for /api/db/query.
type QueryRequest struct {
	Query string `json:"query"`
}

// QueryResponse is the HTTP response body for /api/db/query.
type QueryResponse struct {
	Status   string                   `json:"status"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	Duration int64                    `json:"duration_ms"`
}

// StatsResponse is the HTTP response body for /api/db/stats.
type StatsResponse struct {
	Primary pool.PoolStats `json:"primary"`
	Replica pool.PoolStats `json:"replica"`
}

// Enterprise hooks (overridden in EE build).
var (
	EnterpriseRouteQuery = func(srv *Server, query string, region string) (pool.Manager, string) {
		return nil, ""
	}
)

// Server is the central ServDB request handler.
type Server struct {
	primaryPool pool.Manager
	replicaPool pool.Manager

	regionPools   map[string]pool.Manager
	regionPoolsMu sync.RWMutex

	storeClient *ServShared.StoreClient

	queryAnalytics map[string]*analytics.QueryMetric
	analyticsMu    sync.RWMutex

	migrations   []migration.Migration
	migrationsMu sync.RWMutex

	queryCache   map[string]analytics.CachedResult
	queryCacheMu sync.RWMutex

	peers   []string
	peersMu sync.RWMutex

	activeTables   map[string]bool
	activeTablesMu sync.RWMutex
}

// NewServer initialises a Server and loads any persisted migrations.
func NewServer(primary, replica pool.Manager, store *ServShared.StoreClient) *Server {
	srv := &Server{
		primaryPool:    primary,
		replicaPool:    replica,
		regionPools:    make(map[string]pool.Manager),
		storeClient:    store,
		queryAnalytics: make(map[string]*analytics.QueryMetric),
		migrations:     make([]migration.Migration, 0),
		queryCache:     make(map[string]analytics.CachedResult),
		peers:          make([]string, 0),
		activeTables:   make(map[string]bool),
	}
	srv.loadMigrationsFromStore()
	return srv
}

// AddRegionPool registers a named regional connection pool.
func (srv *Server) AddRegionPool(region string, p pool.Manager) {
	srv.regionPoolsMu.Lock()
	defer srv.regionPoolsMu.Unlock()
	srv.regionPools[region] = p
}

// HasActiveTable returns true if the named table is currently tracked as active.
// Provided for test assertions without exposing internal mutexes.
func (srv *Server) HasActiveTable(name string) bool {
	srv.activeTablesMu.RLock()
	defer srv.activeTablesMu.RUnlock()
	return srv.activeTables[name]
}

// SetPeers updates the list of peer ServDB nodes for write replication.
func (srv *Server) SetPeers(peers []string) {
	srv.peersMu.Lock()
	defer srv.peersMu.Unlock()
	srv.peers = peers
}

// Shutdown drains all pools gracefully.
func (srv *Server) Shutdown(ctx context.Context) error {
	var errs []string
	if err := srv.primaryPool.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Sprintf("primary pool shutdown error: %v", err))
	}
	if err := srv.replicaPool.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Sprintf("replica pool shutdown error: %v", err))
	}
	srv.regionPoolsMu.RLock()
	for region, p := range srv.regionPools {
		if err := p.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("region pool %s shutdown error: %v", region, err))
		}
	}
	srv.regionPoolsMu.RUnlock()
	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (srv *Server) loadMigrationsFromStore() {
	if srv.storeClient == nil {
		return
	}
	if data, err := srv.storeClient.Get("serv-db-migrations", "migrations.json"); err == nil {
		srv.migrationsMu.Lock()
		var loaded []migration.Migration
		if json.Unmarshal(data, &loaded) == nil {
			srv.migrations = loaded
			ServShared.LogJSON(nil, "info", fmt.Sprintf("Loaded %d migrations from ServStore", len(srv.migrations)))
		}
		srv.migrationsMu.Unlock()
	} else {
		ServShared.LogJSON(nil, "warn", fmt.Sprintf("Failed to load migrations (will use default/empty): %v", err))
	}
}

func (srv *Server) saveMigrationsToStore() {
	if srv.storeClient == nil {
		return
	}
	srv.migrationsMu.RLock()
	data, err := json.Marshal(srv.migrations)
	srv.migrationsMu.RUnlock()
	if err == nil {
		_ = srv.storeClient.Put("serv-db-migrations", "migrations.json", data)
	}
}

// HandleQuery serves POST /api/db/query.
func (srv *Server) HandleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}
	start := time.Now()

	var targetPool pool.Manager
	var targetName string

	if ep, name := EnterpriseRouteQuery(srv, req.Query, r.Header.Get("X-Region")); ep != nil {
		targetPool = ep
		targetName = name
	} else {
		targetPool = srv.primaryPool
		targetName = "primary"
	}

	if targetPool.Dialect() == "postgres" && strings.Contains(req.Query, "?") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"dialect_mismatch","message":"PostgreSQL dialect requires '$1' placeholders, found '?'"}`))
		return
	}
	if targetPool.Dialect() == "mysql" && strings.Contains(req.Query, "$1") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"dialect_mismatch","message":"MySQL dialect requires '?' placeholders, found '$1'"}`))
		return
	}

	if targetName == "replica" {
		srv.queryCacheMu.RLock()
		cached, found := srv.queryCache[req.Query]
		srv.queryCacheMu.RUnlock()
		if found && cached.ExpiresAt.After(time.Now()) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(QueryResponse{
				Status:   "success",
				Rows:     cached.Rows,
				Duration: time.Since(start).Milliseconds(),
			})
			return
		}
	}

	conn, err := targetPool.Acquire()
	if err != nil {
		http.Error(w, "Database unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer targetPool.Release(conn)

	if strings.Contains(strings.ToLower(req.Query), "sleep") {
		time.Sleep(110 * time.Millisecond)
	} else {
		time.Sleep(10 * time.Millisecond)
	}
	targetPool.IncrementQueries()

	durationMs := time.Since(start).Milliseconds()
	if durationMs > 100 {
		ServShared.LogJSON(r, "warn", fmt.Sprintf("Slow query detected in ServDB: %q (duration: %dms)", req.Query, durationMs))
	}

	srv.analyticsMu.Lock()
	metric, exists := srv.queryAnalytics[req.Query]
	if !exists {
		metric = &analytics.QueryMetric{}
		srv.queryAnalytics[req.Query] = metric
	}
	metric.Count++
	metric.TotalLatency += durationMs
	srv.analyticsMu.Unlock()

	rows := []map[string]interface{}{
		{"id": 1, "query": req.Query, "status": "executed", "conn_id": conn.ID, "pool": targetName},
	}

	if targetName == "replica" {
		srv.queryCacheMu.Lock()
		srv.queryCache[req.Query] = analytics.CachedResult{
			Rows:      rows,
			CachedAt:  time.Now(),
			ExpiresAt: time.Now().Add(5 * time.Second),
		}
		srv.queryCacheMu.Unlock()
	}

	if targetName == "primary" && r.Header.Get("X-ServDB-Replicated") != "true" {
		srv.replicateQuery(req.Query)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(QueryResponse{
		Status:   "success",
		Rows:     rows,
		Duration: durationMs,
	})
}

func (srv *Server) replicateQuery(query string) {
	srv.peersMu.RLock()
	peers := make([]string, len(srv.peers))
	copy(peers, srv.peers)
	srv.peersMu.RUnlock()

	for _, peer := range peers {
		go func(addr string) {
			if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
				addr = "http://" + addr
			}
			payload := map[string]string{"query": query}
			bodyBytes, _ := json.Marshal(payload)
			req, err := http.NewRequest("POST", addr+"/api/db/query", strings.NewReader(string(bodyBytes)))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-ServDB-Replicated", "true")
				client := &http.Client{Timeout: 5 * time.Second}
				resp, err := client.Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}
		}(peer)
	}
}

// HandleStats serves GET /api/db/stats.
func (srv *Server) HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	res := StatsResponse{
		Primary: srv.primaryPool.Stats(),
		Replica: srv.replicaPool.Stats(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

// HandleAnalytics serves GET /api/db/analytics.
func (srv *Server) HandleAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	srv.analyticsMu.RLock()
	defer srv.analyticsMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(srv.queryAnalytics)
}

// HandleMigrate serves POST /api/db/migrate.
func (srv *Server) HandleMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Version  int    `json:"version"`
		Name     string `json:"name"`
		SQL      string `json:"sql"`
		Rollback string `json:"rollback"`
		Action   string `json:"action"` // "migrate" or "rollback"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Action == "rollback" {
		srv.migrationsMu.Lock()
		defer srv.migrationsMu.Unlock()

		var targetIdx = -1
		if req.Version > 0 {
			for i, m := range srv.migrations {
				if m.Version == req.Version {
					targetIdx = i
					break
				}
			}
		} else if len(srv.migrations) > 0 {
			targetIdx = len(srv.migrations) - 1
		}

		if targetIdx == -1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"not_found","message":"Migration version not found to rollback"}`))
			return
		}

		target := srv.migrations[targetIdx]
		rollbackSQL := target.Rollback
		if rollbackSQL == "" {
			rollbackSQL = req.Rollback
		}

		created, dropped := migration.ParseTablesFromSQL(rollbackSQL)
		srv.activeTablesMu.Lock()
		for _, tbl := range created {
			srv.activeTables[tbl] = true
		}
		for _, tbl := range dropped {
			delete(srv.activeTables, tbl)
		}
		srv.activeTablesMu.Unlock()

		srv.migrations = append(srv.migrations[:targetIdx], srv.migrations[targetIdx+1:]...)
		srv.saveMigrationsToStore()
		_ = ServShared.EmitAuditEvent("ServDB", "MIGRATION_ROLLBACK", "system",
			map[string]interface{}{"version": target.Version, "name": target.Name})

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "rolled_back",
			"version": target.Version,
			"name":    target.Name,
		})
		return
	}

	srv.migrationsMu.Lock()
	for _, m := range srv.migrations {
		if m.Version == req.Version {
			srv.migrationsMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"skipped","message":"Migration already applied"}`))
			return
		}
	}

	created, dropped := migration.ParseTablesFromSQL(req.SQL)
	srv.activeTablesMu.Lock()
	for _, tbl := range created {
		srv.activeTables[tbl] = true
	}
	for _, tbl := range dropped {
		delete(srv.activeTables, tbl)
	}
	srv.activeTablesMu.Unlock()

	newMigration := migration.Migration{
		Version:   req.Version,
		Name:      req.Name,
		AppliedAt: time.Now(),
		SQL:       req.SQL,
		Rollback:  req.Rollback,
	}
	srv.migrations = append(srv.migrations, newMigration)
	srv.migrationsMu.Unlock()
	srv.saveMigrationsToStore()
	_ = ServShared.EmitAuditEvent("ServDB", "MIGRATION_APPLY", "system",
		map[string]interface{}{"version": req.Version, "name": req.Name})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"migration": newMigration,
	})
}

// HandleClearCache serves POST /api/db/cache/clear.
func (srv *Server) HandleClearCache(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	srv.queryCacheMu.Lock()
	srv.queryCache = make(map[string]analytics.CachedResult)
	srv.queryCacheMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success","message":"Query cache invalidated successfully"}`))
}

// HandleDbHealth serves GET /api/db/health.
func (srv *Server) HandleDbHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	primaryStats := srv.primaryPool.Stats()
	replicaStats := srv.replicaPool.Stats()
	deadlockAlert := primaryStats.ActiveConnections >= primaryStats.MaxConnections

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"pools": map[string]interface{}{
			"primary": primaryStats,
			"replica": replicaStats,
		},
		"deadlock_alert": deadlockAlert,
		"active_leases":  primaryStats.ActiveConnections + replicaStats.ActiveConnections,
	})
}

// HandlePrometheusMetrics serves GET /metrics in Prometheus text format.
func (srv *Server) HandlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	type poolEntry struct {
		label string
		stats pool.PoolStats
	}

	pools := []poolEntry{
		{"primary", srv.primaryPool.Stats()},
		{"replica", srv.replicaPool.Stats()},
	}

	srv.regionPoolsMu.RLock()
	for region, p := range srv.regionPools {
		pools = append(pools, poolEntry{"region_" + region, p.Stats()})
	}
	srv.regionPoolsMu.RUnlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, p := range pools {
		lbl := fmt.Sprintf(`pool="%s"`, p.label)
		fmt.Fprintf(w, "# HELP servdb_pool_active_connections Active connections in pool.\n")
		fmt.Fprintf(w, "# TYPE servdb_pool_active_connections gauge\n")
		fmt.Fprintf(w, "servdb_pool_active_connections{%s} %d\n\n", lbl, p.stats.ActiveConnections)

		fmt.Fprintf(w, "# HELP servdb_pool_idle_connections Idle connections waiting in pool.\n")
		fmt.Fprintf(w, "# TYPE servdb_pool_idle_connections gauge\n")
		fmt.Fprintf(w, "servdb_pool_idle_connections{%s} %d\n\n", lbl, p.stats.IdleConnections)

		fmt.Fprintf(w, "# HELP servdb_pool_max_connections Maximum allowed connections for pool.\n")
		fmt.Fprintf(w, "# TYPE servdb_pool_max_connections gauge\n")
		fmt.Fprintf(w, "servdb_pool_max_connections{%s} %d\n\n", lbl, p.stats.MaxConnections)

		fmt.Fprintf(w, "# HELP servdb_pool_total_queries_total Total queries processed by pool.\n")
		fmt.Fprintf(w, "# TYPE servdb_pool_total_queries_total counter\n")
		fmt.Fprintf(w, "servdb_pool_total_queries_total{%s} %d\n\n", lbl, p.stats.TotalQueries)
	}
}
