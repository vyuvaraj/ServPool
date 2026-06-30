package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestServDBConnectionPoolingAndRouting(t *testing.T) {
	setupTest()
	primaryPool = NewConnectionPool(3, "postgres")
	replicaPool = NewConnectionPool(3, "postgres")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/query", handleQuery)
	mux.HandleFunc("/api/db/stats", handleStats)

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// 1. Run concurrent queries to verify pool limit acquisition and routing
	var wg sync.WaitGroup

	// SELECT queries
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqPayload := QueryRequest{Query: "SELECT * FROM users;"}
			body, _ := json.Marshal(reqPayload)
			resp, err := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
			if err == nil {
				var queryRes QueryResponse
				_ = json.NewDecoder(resp.Body).Decode(&queryRes)
				if len(queryRes.Rows) > 0 && queryRes.Rows[0]["pool"] != "replica" {
					t.Errorf("expected SELECT query to route to replica pool, got %v", queryRes.Rows[0]["pool"])
				}
				resp.Body.Close()
			}
		}()
	}

	// INSERT query
	wg.Add(1)
	go func() {
		defer wg.Done()
		reqPayload := QueryRequest{Query: "INSERT INTO users (name) VALUES ('John');"}
		body, _ := json.Marshal(reqPayload)
		resp, err := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
		if err == nil {
			var queryRes QueryResponse
			_ = json.NewDecoder(resp.Body).Decode(&queryRes)
			if len(queryRes.Rows) > 0 && queryRes.Rows[0]["pool"] != "primary" {
				t.Errorf("expected INSERT query to route to primary pool, got %v", queryRes.Rows[0]["pool"])
			}
			resp.Body.Close()
		}
	}()

	wg.Wait()

	// 2. Fetch stats
	statsResp, err := http.Get(testServer.URL + "/api/db/stats")
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	defer statsResp.Body.Close()

	var stats StatsResponse
	if err := json.NewDecoder(statsResp.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if stats.Primary.MaxConnections != 3 || stats.Replica.MaxConnections != 3 {
		t.Errorf("expected max connections to be 3 in pools, got %+v", stats)
	}

	if stats.Primary.Dialect != "postgres" {
		t.Errorf("expected dialect postgres, got %q", stats.Primary.Dialect)
	}
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

func TestServDBQueryCaching(t *testing.T) {
	setupTest()
	primaryPool = NewConnectionPool(5, "postgres")
	replicaPool = NewConnectionPool(5, "postgres")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/query", handleQuery)
	mux.HandleFunc("/api/db/cache/clear", handleClearCache)

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// 1. Fire SELECT query -> should fetch from DB and save to cache
	queryPayload := map[string]string{"query": "SELECT * FROM products;"}
	body, _ := json.Marshal(queryPayload)
	resp, err := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("first SELECT failed: %v", err)
	}
	var res1 QueryResponse
	json.NewDecoder(resp.Body).Decode(&res1)
	resp.Body.Close()

	if len(res1.Rows) == 0 {
		t.Fatalf("expected rows returned")
	}
	connID1 := res1.Rows[0]["conn_id"].(float64)

	// Reset replica query counts to check if connection pool is bypassed
	replicaPool.mu.Lock()
	replicaPool.totalQueries = 0
	replicaPool.mu.Unlock()

	// 2. Fire identical SELECT query again -> should hit cache, bypassing database connection acquire
	resp2, _ := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
	var res2 QueryResponse
	json.NewDecoder(resp2.Body).Decode(&res2)
	resp2.Body.Close()

	connID2 := res2.Rows[0]["conn_id"].(float64)
	if connID1 != connID2 {
		t.Errorf("expected same connection ID context from cached rows")
	}

	replicaPool.mu.Lock()
	queriesRun := replicaPool.totalQueries
	replicaPool.mu.Unlock()
	if queriesRun != 0 {
		t.Errorf("expected connection pool queries run to be 0 (bypassed via cache), got %d", queriesRun)
	}

	// 3. Clear cache
	clearResp, err := http.Post(testServer.URL+"/api/db/cache/clear", "application/json", nil)
	if err != nil || clearResp.StatusCode != http.StatusOK {
		t.Fatalf("clear cache failed: %v", err)
	}
	clearResp.Body.Close()

	// 4. Run SELECT query again -> should acquire connection again
	resp3, _ := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
	resp3.Body.Close()

	replicaPool.mu.Lock()
	queriesRunAfter := replicaPool.totalQueries
	replicaPool.mu.Unlock()
	if queriesRunAfter != 1 {
		t.Errorf("expected connection pool queries run to be 1 after invalidation, got %d", queriesRunAfter)
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
