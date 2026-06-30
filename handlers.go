package main

import (
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

	storeClient *ServShared.StoreClient

	queryAnalytics map[string]*QueryMetric
	analyticsMu    sync.RWMutex

	migrations   []Migration
	migrationsMu sync.RWMutex

	queryCache   map[string]CachedResult
	queryCacheMu sync.RWMutex
}

func NewServer(primary, replica PoolManager, store *ServShared.StoreClient) *Server {
	srv := &Server{
		primaryPool:    primary,
		replicaPool:    replica,
		storeClient:    store,
		queryAnalytics: make(map[string]*QueryMetric),
		migrations:     make([]Migration, 0),
		queryCache:     make(map[string]CachedResult),
	}
	srv.loadMigrationsFromStore()
	return srv
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
		targetPool = srv.replicaPool
		targetName = "replica"
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(QueryResponse{
		Status:   "success",
		Rows:     rows,
		Duration: durationMs,
	})
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
		Version int    `json:"version"`
		Name    string `json:"name"`
		SQL     string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
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

	newMigration := Migration{
		Version:   req.Version,
		Name:      req.Name,
		AppliedAt: time.Now(),
	}
	srv.migrations = append(srv.migrations, newMigration)
	srv.migrationsMu.Unlock()
	srv.saveMigrationsToStore()

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
