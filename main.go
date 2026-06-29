package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/vyuvaraj/ServShared"
)

type QueryRequest struct {
	Query string `json:"query"`
}

type QueryResponse struct {
	Status   string                   `json:"status"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	Duration int64                    `json:"duration_ms"`
}

type PoolStats struct {
	ActiveConnections int    `json:"active_connections"`
	IdleConnections   int    `json:"idle_connections"`
	MaxConnections    int    `json:"max_connections"`
	TotalQueries      int64  `json:"total_queries"`
	Dialect           string `json:"dialect"`
}

type StatsResponse struct {
	Primary PoolStats `json:"primary"`
	Replica PoolStats `json:"replica"`
}

// Simulated Connection
type DbConn struct {
	ID        int
	CreatedAt time.Time
}

type ConnectionPool struct {
	mu           sync.Mutex
	maxConns     int
	activeConns  map[int]*DbConn
	idleConns    []*DbConn
	totalQueries int64
	nextConnID   int
	dialect      string
}

func NewConnectionPool(max int, dialect string) *ConnectionPool {
	return &ConnectionPool{
		maxConns:    max,
		activeConns: make(map[int]*DbConn),
		idleConns:   make([]*DbConn, 0),
		dialect:     dialect,
	}
}

// Acquire gets a connection from the pool or creates a new one
func (p *ConnectionPool) Acquire() (*DbConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Reuse idle connection
	if len(p.idleConns) > 0 {
		conn := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		p.activeConns[conn.ID] = conn
		return conn, nil
	}

	// Create new connection if limit not reached
	if len(p.activeConns) < p.maxConns {
		p.nextConnID++
		conn := &DbConn{
			ID:        p.nextConnID,
			CreatedAt: time.Now(),
		}
		p.activeConns[conn.ID] = conn
		return conn, nil
	}

	return nil, errors.New("connection pool exhausted")
}

// Release returns a connection to the idle pool
func (p *ConnectionPool) Release(conn *DbConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.activeConns, conn.ID)
	p.idleConns = append(p.idleConns, conn)
}

func (p *ConnectionPool) IncrementQueries() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.totalQueries++
}

func (p *ConnectionPool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return PoolStats{
		ActiveConnections: len(p.activeConns),
		IdleConnections:   len(p.idleConns),
		MaxConnections:    p.maxConns,
		TotalQueries:      p.totalQueries,
		Dialect:           p.dialect,
	}
}

var (
	primaryPool *ConnectionPool
	replicaPool *ConnectionPool
)

func main() {
	portStr := flag.String("port", "8097", "ServDB server port")
	maxConns := flag.Int("max_conns", 10, "Maximum connection pool size")
	dialectStr := flag.String("dialect", "postgres", "Database dialect (postgres, mysql)")
	flag.Parse()

	port := os.Getenv("PORT")
	if port == "" {
		port = *portStr
	}

	primaryPool = NewConnectionPool(*maxConns, *dialectStr)
	replicaPool = NewConnectionPool(*maxConns, *dialectStr)

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/api/db/query", handleQuery)
	mux.HandleFunc("/api/db/stats", handleStats)

	serverHandler := ServShared.AuthMiddleware(mux)

	log.Printf("ServDB connection pooler starting on port %s", port)
	if err := http.ListenAndServe(":"+port, serverHandler); err != nil {
		log.Fatalf("failed to start ServDB: %v", err)
	}
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
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

	var targetPool *ConnectionPool
	var targetName string

	queryLower := strings.ToLower(strings.TrimSpace(req.Query))
	if strings.HasPrefix(queryLower, "select") {
		targetPool = replicaPool
		targetName = "replica"
	} else {
		targetPool = primaryPool
		targetName = "primary"
	}

	// Dialect placeholder format safety validation
	if targetPool.dialect == "postgres" && strings.Contains(req.Query, "?") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"dialect_mismatch","message":"PostgreSQL dialect requires '$1' placeholders, found '?'"}`))
		return
	}
	if targetPool.dialect == "mysql" && strings.Contains(req.Query, "$1") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"dialect_mismatch","message":"MySQL dialect requires '?' placeholders, found '$1'"}`))
		return
	}

	// Acquire mock pooled database connection handle
	conn, err := targetPool.Acquire()
	if err != nil {
		http.Error(w, "Database unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer targetPool.Release(conn)

	// Simulate brief query processing latency
	time.Sleep(10 * time.Millisecond)
	targetPool.IncrementQueries()

	// Simulated query output rows
	rows := []map[string]interface{}{
		{"id": 1, "query": req.Query, "status": "executed", "conn_id": conn.ID, "pool": targetName},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(QueryResponse{
		Status:   "success",
		Rows:     rows,
		Duration: time.Since(start).Milliseconds(),
	})
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	res := StatsResponse{
		Primary: primaryPool.Stats(),
		Replica: replicaPool.Stats(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}
