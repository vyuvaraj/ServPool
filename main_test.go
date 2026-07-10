package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	testSrv        *Server
	primaryPool    *ConnectionPool
	replicaPool    *ConnectionPool
	queryAnalytics = make(map[string]*QueryMetric)
	analyticsMu    sync.RWMutex
	migrations     = make([]Migration, 0)
	migrationsMu   sync.RWMutex
	queryCache     = make(map[string]CachedResult)
	queryCacheMu   sync.RWMutex
)

func setupTest() {
	primaryPool = NewConnectionPool(10, "postgres")
	replicaPool = NewConnectionPool(10, "postgres")
	queryAnalytics = make(map[string]*QueryMetric)
	migrations = make([]Migration, 0)
	queryCache = make(map[string]CachedResult)
	testSrv = NewServer(primaryPool, replicaPool, nil)
	testSrv.queryAnalytics = queryAnalytics
	testSrv.migrations = migrations
	testSrv.queryCache = queryCache
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	testSrv.primaryPool = primaryPool
	testSrv.replicaPool = replicaPool
	testSrv.queryAnalytics = queryAnalytics
	testSrv.migrations = migrations
	testSrv.queryCache = queryCache
	testSrv.handleQuery(w, r)
	queryAnalytics = testSrv.queryAnalytics
	migrations = testSrv.migrations
	queryCache = testSrv.queryCache
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	testSrv.primaryPool = primaryPool
	testSrv.replicaPool = replicaPool
	testSrv.handleStats(w, r)
}

func handleAnalytics(w http.ResponseWriter, r *http.Request) {
	testSrv.queryAnalytics = queryAnalytics
	testSrv.handleAnalytics(w, r)
	queryAnalytics = testSrv.queryAnalytics
}

func handleMigrate(w http.ResponseWriter, r *http.Request) {
	testSrv.migrations = migrations
	testSrv.handleMigrate(w, r)
	migrations = testSrv.migrations
}

func handleClearCache(w http.ResponseWriter, r *http.Request) {
	testSrv.queryCache = queryCache
	testSrv.handleClearCache(w, r)
	queryCache = testSrv.queryCache
}

func handleDbHealth(w http.ResponseWriter, r *http.Request) {
	testSrv.primaryPool = primaryPool
	testSrv.replicaPool = replicaPool
	testSrv.handleDbHealth(w, r)
}


func TestServDBDialectValidation(t *testing.T) {
	setupTest()
	// Configure with PostgreSQL dialect
	primaryPool = NewConnectionPool(1, "postgres")
	replicaPool = NewConnectionPool(1, "postgres")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/query", handleQuery)

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// Try querying using MySQL placeholder '?' on Postgres pool -> should fail!
	reqPayload := QueryRequest{Query: "SELECT * FROM users WHERE id = ?;"}
	body, _ := json.Marshal(reqPayload)
	resp, err := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected StatusBadRequest for dialect placeholder mismatch, got %d", resp.StatusCode)
	}

	// Try with Postgres placeholder '$1' on Postgres pool -> should succeed!
	reqPayload2 := QueryRequest{Query: "SELECT * FROM users WHERE id = $1;"}
	body2, _ := json.Marshal(reqPayload2)
	resp2, err := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected StatusOK for valid Postgres placeholder, got %d", resp2.StatusCode)
	}
}

func TestServDBSlowQueryAndAnalytics(t *testing.T) {
	setupTest()
	primaryPool = NewConnectionPool(1, "postgres")
	replicaPool = NewConnectionPool(1, "postgres")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/query", handleQuery)
	mux.HandleFunc("/api/db/analytics", handleAnalytics)

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// 1. Run a query containing 'sleep' to trigger a slow query log
	reqPayload := QueryRequest{Query: "SELECT sleep(2) FROM dual;"}
	body, _ := json.Marshal(reqPayload)
	resp, err := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected StatusOK for slow query, got %d", resp.StatusCode)
	}

	// 2. Fetch analytics
	analResp, err := http.Get(testServer.URL + "/api/db/analytics")
	if err != nil {
		t.Fatalf("failed to get analytics: %v", err)
	}
	defer analResp.Body.Close()

	if analResp.StatusCode != http.StatusOK {
		t.Fatalf("expected analytics StatusOK, got %d", analResp.StatusCode)
	}

	var metrics map[string]QueryMetric
	json.NewDecoder(analResp.Body).Decode(&metrics)

	metric, exists := metrics["SELECT sleep(2) FROM dual;"]
	if !exists {
		t.Fatalf("expected metric to exist for query signature")
	}

	if metric.Count != 1 || metric.TotalLatency < 100 {
		t.Errorf("unexpected query metric values: %+v", metric)
	}
}

func TestServDBMigrations(t *testing.T) {
	setupTest()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/migrate", handleMigrate)

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// 1. Post a migration SQL execution request
	payload := map[string]interface{}{
		"version": 1,
		"name":    "create_users_table",
		"sql":     "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100));",
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(testServer.URL+"/api/db/migrate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("migration post failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected StatusCreated, got %d", resp.StatusCode)
	}

	var res struct {
		Status    string    `json:"status"`
		Migration Migration `json:"migration"`
	}
	json.NewDecoder(resp.Body).Decode(&res)

	if res.Status != "success" || res.Migration.Version != 1 || res.Migration.Name != "create_users_table" {
		t.Errorf("invalid migration response: %+v", res)
	}

	// 2. Post same migration again -> should skip (status: skipped)
	resp2, err := http.Post(testServer.URL+"/api/db/migrate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed duplicate migration request: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected StatusOK for skipped migration, got %d", resp2.StatusCode)
	}

	var res2 struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	json.NewDecoder(resp2.Body).Decode(&res2)
	if res2.Status != "skipped" {
		t.Errorf("expected status 'skipped' for duplicate migration, got %q", res2.Status)
	}
}


func TestServDBHealth(t *testing.T) {
	setupTest()
	primaryPool = NewConnectionPool(2, "postgres")
	replicaPool = NewConnectionPool(2, "postgres")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/health", handleDbHealth)

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// 1. Check healthy stats
	resp, err := http.Get(testServer.URL + "/api/db/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to query health: %v", err)
	}
	var res map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()

	if res["status"] != "healthy" || res["deadlock_alert"].(bool) != false {
		t.Errorf("unexpected healthy payload: %+v", res)
	}

	// 2. Lease connections to max -> should trigger deadlock alert
	c1, _ := primaryPool.Acquire()
	c2, _ := primaryPool.Acquire()
	defer primaryPool.Release(c1)
	defer primaryPool.Release(c2)

	resp2, _ := http.Get(testServer.URL + "/api/db/health")
	var res2 map[string]interface{}
	json.NewDecoder(resp2.Body).Decode(&res2)
	resp2.Body.Close()

	if res2["deadlock_alert"].(bool) != true {
		t.Errorf("expected deadlock alert to be true when pool is full, got false")
	}
}

func TestTableDrivenQueryValidation(t *testing.T) {
	setupTest()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/query", handleQuery)
	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	tests := []struct {
		name       string
		query      string
		wantStatus int
	}{
		{
			name:       "Empty Query",
			query:      "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "Postgres Placeholder Mismatch",
			query:      "SELECT * FROM users WHERE id = ?;",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqPayload := QueryRequest{Query: tt.query}
			body, _ := json.Marshal(reqPayload)
			resp, err := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, resp.StatusCode)
			}
		})
	}
}

func TestConnectionPoolTuning(t *testing.T) {
	pool := NewConnectionPool(2, "postgres")
	pool.maxLifetime = 50 * time.Millisecond // Use short lifetime for testing

	// 1. Verify fresh connections are created and recycled
	c1, err := pool.Acquire()
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	c2, err := pool.Acquire()
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Release c1
	pool.Release(c1)

	// Acquire again -> should recycle c1
	c1Recycled, err := pool.Acquire()
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if c1Recycled.ID != c1.ID {
		t.Errorf("expected recycled connection %d, got %d", c1.ID, c1Recycled.ID)
	}

	// Release both
	pool.Release(c1Recycled)
	pool.Release(c2)

	// Wait for connections to expire
	time.Sleep(100 * time.Millisecond)

	// Acquire again -> should invalidate stale ones and create fresh one
	c3, err := pool.Acquire()
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if c3.ID == c1.ID || c3.ID == c2.ID {
		t.Errorf("expected fresh connection, got stale connection %d", c3.ID)
	}

	// 2. Verify adaptive scaling (expansion to 2x baseMaxConns)
	c4, _ := pool.Acquire() // base limit 2 is now exhausted
	c5, err := pool.Acquire() // should scale maxConns up and acquire successfully
	if err != nil {
		t.Fatalf("expected successful expansion acquisition, got: %v", err)
	}
	if pool.Stats().MaxConnections != 4 {
		t.Errorf("expected pool capacity expanded to 4, got %d", pool.Stats().MaxConnections)
	}

	// Release all
	pool.Release(c3)
	pool.Release(c4)
	pool.Release(c5)

	// Wait for pool scaling cooldown
	time.Sleep(1200 * time.Millisecond)

	// Verify pool scaled down back to baseMaxConns
	if pool.Stats().MaxConnections != 2 {
		t.Errorf("expected pool capacity shrunk back to 2, got %d", pool.Stats().MaxConnections)
	}
}

func BenchmarkQueryCacheLookup(b *testing.B) {
	// Pre-populate cache
	queryCacheMu.Lock()
	queryCache["SELECT * FROM users;"] = CachedResult{
		Rows:      []map[string]interface{}{{"id": 1}},
		CachedAt:  time.Now(),
		ExpiresAt: time.Now().Add(1 * time.Hour),
	}
	queryCacheMu.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queryCacheMu.RLock()
		_, _ = queryCache["SELECT * FROM users;"]
		queryCacheMu.RUnlock()
	}
}

func BenchmarkConnectionPoolAcquireRelease(b *testing.B) {
	pool := NewConnectionPool(100, "postgres") // high max conns to prevent exhaustion in benchmark
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c, err := pool.Acquire()
			if err == nil {
				pool.Release(c)
			}
		}
	})
}

func TestDatabaseQueryReplication(t *testing.T) {
	primaryPool1 := NewConnectionPool(2, "postgres")
	replicaPool1 := NewConnectionPool(2, "postgres")
	srv1 := NewServer(primaryPool1, replicaPool1, nil)
	mux1 := http.NewServeMux()
	mux1.HandleFunc("/api/db/query", srv1.handleQuery)
	server1 := httptest.NewServer(mux1)
	defer server1.Close()

	primaryPool2 := NewConnectionPool(2, "postgres")
	replicaPool2 := NewConnectionPool(2, "postgres")
	srv2 := NewServer(primaryPool2, replicaPool2, nil)
	mux2 := http.NewServeMux()
	var queryReplicatedMutex sync.Mutex
	var replicatedQueryString string
	mux2.HandleFunc("/api/db/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ServDB-Replicated") == "true" {
			var req QueryRequest
			json.NewDecoder(r.Body).Decode(&req)
			queryReplicatedMutex.Lock()
			replicatedQueryString = req.Query
			queryReplicatedMutex.Unlock()
		}
		srv2.handleQuery(w, r)
	})
	server2 := httptest.NewServer(mux2)
	defer server2.Close()

	srv1.SetPeers([]string{server2.URL})

	reqPayload := QueryRequest{Query: "INSERT INTO users (name) VALUES ('alice');"}
	body, _ := json.Marshal(reqPayload)
	resp, err := http.Post(server1.URL+"/api/db/query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()

	time.Sleep(50 * time.Millisecond)

	queryReplicatedMutex.Lock()
	gotQuery := replicatedQueryString
	queryReplicatedMutex.Unlock()

	expectedQuery := "INSERT INTO users (name) VALUES ('alice');"
	if gotQuery != expectedQuery {
		t.Errorf("expected replicated query %q, got %q", expectedQuery, gotQuery)
	}
}

func TestDatabaseMigrationsRollback(t *testing.T) {
	primaryPool := NewConnectionPool(2, "postgres")
	replicaPool := NewConnectionPool(2, "postgres")
	srv := NewServer(primaryPool, replicaPool, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/migrate", srv.handleMigrate)
	server := httptest.NewServer(mux)
	defer server.Close()

	// 1. Post a new migration creating 'customers' table
	migratePayload := map[string]interface{}{
		"version":  10,
		"name":     "create_customers",
		"sql":      "CREATE TABLE customers (id INT);",
		"rollback": "DROP TABLE customers;",
	}
	body, _ := json.Marshal(migratePayload)
	resp, err := http.Post(server.URL+"/api/db/migrate", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("failed migration: %v", err)
	}
	resp.Body.Close()

	// Check table was registered
	srv.activeTablesMu.RLock()
	hasCustomers := srv.activeTables["customers"]
	srv.activeTablesMu.RUnlock()
	if !hasCustomers {
		t.Errorf("expected 'customers' table to be tracked after migration")
	}

	// 2. Post rollback action for version 10
	rollbackPayload := map[string]interface{}{
		"action":  "rollback",
		"version": 10,
	}
	bodyRb, _ := json.Marshal(rollbackPayload)
	respRb, err := http.Post(server.URL+"/api/db/migrate", "application/json", bytes.NewReader(bodyRb))
	if err != nil || respRb.StatusCode != http.StatusOK {
		t.Fatalf("failed rollback request: %v", err)
	}
	respRb.Body.Close()

	// Check table was dropped
	srv.activeTablesMu.RLock()
	hasCustomersAfterRb := srv.activeTables["customers"]
	srv.activeTablesMu.RUnlock()
	if hasCustomersAfterRb {
		t.Errorf("expected 'customers' table to be untracked after rollback")
	}
}

func TestServDBConnectionDraining(t *testing.T) {
	pool := NewConnectionPool(2, "postgres")
	
	// Acquire a connection to keep it active
	conn, err := pool.Acquire()
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}

	// Trigger shutdown in a goroutine
	shutdownErr := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() {
		shutdownErr <- pool.Shutdown(ctx)
	}()

	// Wait a bit to ensure it blocks since connection is active
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-shutdownErr:
		t.Fatalf("expected Shutdown to block, but it returned early: %v", err)
	default:
		// Working as expected, it's blocking
	}

	// Release the active connection
	pool.Release(conn)

	// Now it should complete successfully
	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Errorf("expected no error from Shutdown after release, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for Shutdown to complete after release")
	}
}


func TestConnectionPoolDeadlockTimeout(t *testing.T) {
	os.Setenv("SERVDB_CONN_TIMEOUT", "20ms")
	defer os.Unsetenv("SERVDB_CONN_TIMEOUT")

	// Pool of max 1 connection (which can adaptively scale to 2 under load)
	pool := NewConnectionPool(1, "sqlite")
	defer pool.Shutdown(context.Background())

	conn1, err := pool.Acquire()
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	conn2, err := pool.Acquire()
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}

	// Try acquiring 3rd connection, should exhaust the pool (since max adaptive limit is 2)
	_, err = pool.Acquire()
	if err == nil {
		t.Fatal("expected third acquire to fail due to pool exhaustion")
	}

	// Wait for the janitor to run and timeout the connection (timeout is 20ms, janitor runs every 100ms)
	time.Sleep(150 * time.Millisecond)

	// Try acquiring again, it should succeed because the janitor reclaimed the leaked connections
	conn3, err := pool.Acquire()
	if err != nil {
		t.Fatalf("acquire after timeout failed: %v", err)
	}
	if conn3.ID != conn1.ID && conn3.ID != conn2.ID {
		t.Errorf("expected reclaimed connection ID %d or %d, got %d", conn1.ID, conn2.ID, conn3.ID)
	}

	// Release connections to allow Shutdown to succeed cleanly
	pool.Release(conn1)
	pool.Release(conn2)
	pool.Release(conn3)
}

