package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestServDBConnectionPooling(t *testing.T) {
	pool = NewConnectionPool(3) // limit to 3 conns max

	mux := http.NewServeMux()
	mux.HandleFunc("/api/db/query", handleQuery)
	mux.HandleFunc("/api/db/stats", handleStats)

	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// 1. Run concurrent queries to verify pool limit acquisition
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqPayload := QueryRequest{Query: "SELECT * FROM users;"}
			body, _ := json.Marshal(reqPayload)
			resp, err := http.Post(testServer.URL+"/api/db/query", "application/json", bytes.NewReader(body))
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	// 2. Fetch stats
	statsResp, err := http.Get(testServer.URL + "/api/db/stats")
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	defer statsResp.Body.Close()

	var stats PoolStats
	if err := json.NewDecoder(statsResp.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if stats.MaxConnections != 3 {
		t.Errorf("expected MaxConnections to be 3, got %d", stats.MaxConnections)
	}

	// Connections should be returned to idle array
	if stats.IdleConnections == 0 && stats.ActiveConnections == 0 {
		t.Errorf("expected pooled connections, got %+v", stats)
	}
}
