package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"servpool/pkg/pool"
)

type mockPool struct {
	dialectVal string
}

func (m *mockPool) Acquire() (*pool.DbConn, error)              { return &pool.DbConn{ID: 1}, nil }
func (m *mockPool) Release(conn *pool.DbConn)                   {}
func (m *mockPool) IncrementQueries()                           {}
func (m *mockPool) Stats() pool.PoolStats                       { return pool.PoolStats{Dialect: m.dialectVal} }
func (m *mockPool) Dialect() string                             { return m.dialectVal }
func (m *mockPool) Shutdown(ctx context.Context) error          { return nil }

func TestRoutingServerMetrics(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()
	srv.HandlePrometheusMetrics(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestRoutingServerDbHealth(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	req := httptest.NewRequest("GET", "/api/db/health", nil)
	rr := httptest.NewRecorder()
	srv.HandleDbHealth(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestRoutingAddRegionPool(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)
	rPool := &mockPool{dialectVal: "mysql"}
	srv.AddRegionPool("us-west", rPool)

	srv.regionPoolsMu.RLock()
	p := srv.regionPools["us-west"]
	srv.regionPoolsMu.RUnlock()
	if p != rPool {
		t.Error("failed to register regional pool")
	}
}

func TestRoutingHasActiveTable(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)
	srv.activeTables["users"] = true
	if !srv.HasActiveTable("users") {
		t.Error("expected users table to be active")
	}
}

func TestRoutingSetPeers(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)
	srv.SetPeers([]string{"peer1"})
	if len(srv.peers) != 1 || srv.peers[0] != "peer1" {
		t.Errorf("expected peers [peer1], got %v", srv.peers)
	}
}

func TestRoutingShutdown(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)
	err := srv.Shutdown(context.Background())
	if err != nil {
		t.Errorf("failed to shutdown gracefully: %v", err)
	}
}

func TestRoutingHandleQueryInvalidJSON(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	req := httptest.NewRequest("POST", "/api/db/query", bytes.NewReader([]byte("{invalid}")))
	rr := httptest.NewRecorder()
	srv.HandleQuery(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", rr.Code)
	}
}

func TestRoutingHandleQueryEmptyQuery(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	body, _ := json.Marshal(QueryRequest{Query: ""})
	req := httptest.NewRequest("POST", "/api/db/query", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	srv.HandleQuery(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr.Code)
	}
}

func TestRoutingHandleStats(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	req := httptest.NewRequest("GET", "/api/db/stats", nil)
	rr := httptest.NewRecorder()
	srv.HandleStats(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestRoutingHandleAnalytics(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	req := httptest.NewRequest("GET", "/api/db/analytics", nil)
	rr := httptest.NewRecorder()
	srv.HandleAnalytics(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestRoutingHandleClearCache(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	req := httptest.NewRequest("POST", "/api/db/cache/clear", nil)
	rr := httptest.NewRecorder()
	srv.HandleClearCache(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestRoutingHandleMigrateInvalidJSON(t *testing.T) {
	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	req := httptest.NewRequest("POST", "/api/db/migrate", bytes.NewReader([]byte("{invalid}")))
	rr := httptest.NewRecorder()
	srv.HandleMigrate(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", rr.Code)
	}
}

type mockQueryOptimizer struct {
	routeCalled bool
	getCachedCalled bool
	setCachedCalled bool
	clearCacheCalled bool
}

func (m *mockQueryOptimizer) Route(srv *Server, query string, region string) (pool.Manager, string) {
	m.routeCalled = true
	return srv.primaryPool, "primary"
}

func (m *mockQueryOptimizer) GetCached(query string) ([]map[string]interface{}, bool) {
	m.getCachedCalled = true
	return nil, false
}

func (m *mockQueryOptimizer) SetCached(query string, rows []map[string]interface{}) {
	m.setCachedCalled = true
}

func (m *mockQueryOptimizer) ClearCache() {
	m.clearCacheCalled = true
}

func TestPluggableQueryOptimizer(t *testing.T) {
	mockOpt := &mockQueryOptimizer{}
	ActiveQueryOptimizer = mockOpt
	defer func() { ActiveQueryOptimizer = nil }()

	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	body, _ := json.Marshal(QueryRequest{Query: "SELECT 1;"})
	req := httptest.NewRequest("POST", "/api/db/query", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	srv.HandleQuery(rr, req)

	if !mockOpt.routeCalled {
		t.Error("expected Route to be called")
	}
	if !mockOpt.getCachedCalled {
		t.Error("expected GetCached to be called")
	}
	if !mockOpt.setCachedCalled {
		t.Error("expected SetCached to be called")
	}

	clearReq := httptest.NewRequest("POST", "/api/db/cache/clear", nil)
	clearRR := httptest.NewRecorder()
	srv.HandleClearCache(clearRR, clearReq)

	if !mockOpt.clearCacheCalled {
		t.Error("expected ClearCache to be called")
	}
}

// rwSplitOptimizer routes SELECTs to the replica and all writes to primary.
type rwSplitOptimizer struct{}

func (o *rwSplitOptimizer) Route(srv *Server, query string, region string) (pool.Manager, string) {
	q := query
	if len(q) >= 6 && q[:6] == "SELECT" {
		return srv.replicaPool, "replica"
	}
	return srv.primaryPool, "primary"
}
func (o *rwSplitOptimizer) GetCached(query string) ([]map[string]interface{}, bool) { return nil, false }
func (o *rwSplitOptimizer) SetCached(query string, rows []map[string]interface{})   {}
func (o *rwSplitOptimizer) ClearCache()                                              {}

// TestReadWriteRoutingAccuracy verifies 100% routing correctness:
// SELECT queries must go to replica, all other DML to primary.
func TestReadWriteRoutingAccuracy(t *testing.T) {
	ActiveQueryOptimizer = &rwSplitOptimizer{}
	defer func() { ActiveQueryOptimizer = nil }()

	primary := &mockPool{dialectVal: "postgres"}
	replica := &mockPool{dialectVal: "postgres"}
	srv := NewServer(primary, replica, nil)

	readQueries := []string{
		"SELECT id FROM users;",
		"SELECT name FROM products WHERE active = true;",
		"SELECT count(*) FROM orders;",
	}
	writeQueries := []string{
		"INSERT INTO users (name) VALUES ('alice');",
		"UPDATE users SET name = 'bob' WHERE id = 1;",
		"DELETE FROM users WHERE id = 2;",
	}

	type result struct {
		Status string `json:"status"`
		Rows   []struct {
			Pool string `json:"pool"`
		} `json:"rows"`
	}

	wrongRoutes := 0

	// Run 50 read queries — all must route to "replica"
	for i := 0; i < 50; i++ {
		q := readQueries[i%len(readQueries)]
		body, _ := json.Marshal(QueryRequest{Query: q})
		req := httptest.NewRequest("POST", "/api/db/query", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		srv.HandleQuery(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("[read %d] unexpected status %d", i, rr.Code)
			continue
		}
		var res result
		if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
			t.Errorf("[read %d] decode error: %v", i, err)
			continue
		}
		if len(res.Rows) == 0 || res.Rows[0].Pool != "replica" {
			wrongRoutes++
			t.Errorf("[read %d] query %q routed to %q, want replica", i, q, res.Rows[0].Pool)
		}
	}

	// Run 50 write queries — all must route to "primary"
	for i := 0; i < 50; i++ {
		q := writeQueries[i%len(writeQueries)]
		body, _ := json.Marshal(QueryRequest{Query: q})
		req := httptest.NewRequest("POST", "/api/db/query", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		srv.HandleQuery(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("[write %d] unexpected status %d", i, rr.Code)
			continue
		}
		var res result
		if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
			t.Errorf("[write %d] decode error: %v", i, err)
			continue
		}
		if len(res.Rows) == 0 || res.Rows[0].Pool != "primary" {
			wrongRoutes++
			t.Errorf("[write %d] query %q routed to %q, want primary", i, q, res.Rows[0].Pool)
		}
	}

	if wrongRoutes > 0 {
		t.Errorf("routing accuracy: %d/%d queries misrouted", wrongRoutes, 100)
	} else {
		t.Logf("routing accuracy: 100%% (100/100 queries correctly routed)")
	}
}
