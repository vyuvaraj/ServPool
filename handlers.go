package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/vyuvaraj/ServShared"
)

type Server struct {
	primaryPool PoolManager
	replicaPool PoolManager

	regionPools   map[string]PoolManager
	regionPoolsMu sync.RWMutex

	storeClient *ServShared.StoreClient

	queryAnalytics map[string]*QueryMetric
	analyticsMu    sync.RWMutex

	migrations   []Migration
	migrationsMu sync.RWMutex

	queryCache   map[string]CachedResult
	queryCacheMu sync.RWMutex

	peers   []string
	peersMu sync.RWMutex

	activeTables   map[string]bool
	activeTablesMu sync.RWMutex
}

func NewServer(primary, replica PoolManager, store *ServShared.StoreClient) *Server {
	srv := &Server{
		primaryPool:    primary,
		replicaPool:    replica,
		regionPools:    make(map[string]PoolManager),
		storeClient:    store,
		queryAnalytics: make(map[string]*QueryMetric),
		migrations:     make([]Migration, 0),
		queryCache:     make(map[string]CachedResult),
		peers:          make([]string, 0),
		activeTables:   make(map[string]bool),
	}
	srv.loadMigrationsFromStore()
	return srv
}

func (srv *Server) AddRegionPool(region string, pool PoolManager) {
	srv.regionPoolsMu.Lock()
	defer srv.regionPoolsMu.Unlock()
	srv.regionPools[region] = pool
}

func (srv *Server) Shutdown(ctx context.Context) error {
	var errs []string
	if err := srv.primaryPool.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Sprintf("primary pool shutdown error: %v", err))
	}
	if err := srv.replicaPool.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Sprintf("replica pool shutdown error: %v", err))
	}

	srv.regionPoolsMu.RLock()
	for region, pool := range srv.regionPools {
		if err := pool.Shutdown(ctx); err != nil {
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
		var loadedMigrations []Migration
		if json.Unmarshal(data, &loadedMigrations) == nil {
			srv.migrations = loadedMigrations
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

func (srv *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
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

	var targetPool PoolManager
	var targetName string
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(req.Query)), "SELECT") {
		region := r.Header.Get("X-Region")
		if region != "" {
			srv.regionPoolsMu.RLock()
			regPool, exists := srv.regionPools[region]
			srv.regionPoolsMu.RUnlock()
			if exists {
				targetPool = regPool
				targetName = "replica-" + region
			}
		}
		if targetPool == nil {
			targetPool = srv.replicaPool
			targetName = "replica"
		}
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
		metric = &QueryMetric{}
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
		srv.queryCache[req.Query] = CachedResult{
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

func (srv *Server) SetPeers(peers []string) {
	srv.peersMu.Lock()
	defer srv.peersMu.Unlock()
	srv.peers = peers
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

func (srv *Server) handleStats(w http.ResponseWriter, r *http.Request) {
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

func (srv *Server) handleAnalytics(w http.ResponseWriter, r *http.Request) {
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

func (srv *Server) handleMigrate(w http.ResponseWriter, r *http.Request) {
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

		targetMigration := srv.migrations[targetIdx]
		rollbackSQL := targetMigration.Rollback
		if rollbackSQL == "" {
			rollbackSQL = req.Rollback
		}

		created, dropped := ParseTablesFromSQL(rollbackSQL)
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
		_ = ServShared.EmitAuditEvent("ServDB", "MIGRATION_ROLLBACK", "system", map[string]interface{}{"version": targetMigration.Version, "name": targetMigration.Name})

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "rolled_back",
			"version": targetMigration.Version,
			"name":    targetMigration.Name,
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

	created, dropped := ParseTablesFromSQL(req.SQL)
	srv.activeTablesMu.Lock()
	for _, tbl := range created {
		srv.activeTables[tbl] = true
	}
	for _, tbl := range dropped {
		delete(srv.activeTables, tbl)
	}
	srv.activeTablesMu.Unlock()

	newMigration := Migration{
		Version:   req.Version,
		Name:      req.Name,
		AppliedAt: time.Now(),
		SQL:       req.SQL,
		Rollback:  req.Rollback,
	}
	srv.migrations = append(srv.migrations, newMigration)
	srv.migrationsMu.Unlock()
	srv.saveMigrationsToStore()
	_ = ServShared.EmitAuditEvent("ServDB", "MIGRATION_APPLY", "system", map[string]interface{}{"version": req.Version, "name": req.Name})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"migration": newMigration,
	})
}

func (srv *Server) handleClearCache(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	srv.queryCacheMu.Lock()
	srv.queryCache = make(map[string]CachedResult)
	srv.queryCacheMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success","message":"Query cache invalidated successfully"}`))
}

func (srv *Server) handleDbHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	primaryStats := srv.primaryPool.Stats()
	replicaStats := srv.replicaPool.Stats()

	deadlockAlert := false
	if primaryStats.ActiveConnections >= primaryStats.MaxConnections {
		deadlockAlert = true
	}

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

func (srv *Server) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	type poolEntry struct {
		label string
		stats PoolStats
	}

	pools := []poolEntry{
		{"primary", srv.primaryPool.Stats()},
		{"replica", srv.replicaPool.Stats()},
	}

	srv.regionPoolsMu.RLock()
	for region, pool := range srv.regionPools {
		pools = append(pools, poolEntry{"region_" + region, pool.Stats()})
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
