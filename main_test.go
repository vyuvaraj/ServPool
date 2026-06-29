package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestServDBConnectionPoolingAndRouting(t *testing.T) {
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
