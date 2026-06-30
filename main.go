package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vyuvaraj/ServShared"
)

type QueryMetric struct {
	Count        int64 `json:"count"`
	TotalLatency int64 `json:"total_latency_ms"`
}

type CachedResult struct {
	Rows      []map[string]interface{} `json:"rows"`
	CachedAt  time.Time                `json:"cached_at"`
	ExpiresAt time.Time                `json:"expires_at"`
}

type QueryRequest struct {
	Query string `json:"query"`
}

type QueryResponse struct {
	Status   string                   `json:"status"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	Duration int64                    `json:"duration_ms"`
}

type StatsResponse struct {
	Primary PoolStats `json:"primary"`
	Replica PoolStats `json:"replica"`
}

func main() {
	portStr := flag.String("port", "8097", "ServDB server port")
	maxConns := flag.Int("max_conns", 10, "Maximum connection pool size")
	dialectStr := flag.String("dialect", "postgres", "Database dialect (postgres, mysql)")
	flag.Parse()

	port := os.Getenv("PORT")
	if port == "" {
		port = *portStr
	}

	primaryPool := NewConnectionPool(*maxConns, *dialectStr)
	replicaPool := NewConnectionPool(*maxConns, *dialectStr)

	storeClient := ServShared.NewStoreClient()

	srv := NewServer(primaryPool, replicaPool, storeClient)

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/api/db/query", srv.handleQuery)
	mux.HandleFunc("/api/db/stats", srv.handleStats)
	mux.HandleFunc("/api/db/analytics", srv.handleAnalytics)
	mux.HandleFunc("/api/db/migrate", srv.handleMigrate)
	mux.HandleFunc("/api/db/cache/clear", srv.handleClearCache)
	mux.HandleFunc("/api/db/health", srv.handleDbHealth)

	serverHandler := ServShared.TraceMiddleware("servdb", ServShared.AuthMiddleware(mux))

	server := &http.Server{
		Addr:    ":" + port,
		Handler: serverHandler,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[INFO] ServDB connection pooler starting on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("failed to start ServDB: %v", err)
		}
	}()

	<-stop

	log.Println("[INFO] Shutting down ServDB server...")
	ServShared.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("[INFO] ServDB server exited cleanly")
}
