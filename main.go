package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vyuvaraj/ServShared"
	"servdb/pkg/pool"
	"servdb/pkg/routing"
)

// Enterprise hooks (overridden in EE build).
var (
	EnterpriseListenAndServeTLS = func(srv *http.Server, certFile, keyFile string) error { return nil }
)

func main() {
	portStr := flag.String("port", "8097", "ServDB server port")
	maxConns := flag.Int("max_conns", 10, "Maximum connection pool size")
	dialectStr := flag.String("dialect", "postgres", "Database dialect (postgres, mysql)")
	peersStr := flag.String("peers", "", "Comma-separated list of database peer addresses")
	regionReplicasStr := flag.String("region-replicas", "", "Comma-separated list of region names to create local replica pools for (e.g. us-east,us-west)")
	flag.Parse()

	port := os.Getenv("PORT")
	if port == "" {
		port = *portStr
	}

	primaryPool := pool.NewConnectionPool(*maxConns, *dialectStr)
	replicaPool := pool.NewConnectionPool(*maxConns, *dialectStr)

	storeClient := ServShared.NewStoreClient()

	srv := routing.NewServer(primaryPool, replicaPool, storeClient)

	var regionReplicas []string
	if *regionReplicasStr != "" {
		regionReplicas = strings.Split(*regionReplicasStr, ",")
	} else if envRegions := os.Getenv("SERVDB_REGION_REPLICAS"); envRegions != "" {
		regionReplicas = strings.Split(envRegions, ",")
	}
	for _, region := range regionReplicas {
		region = strings.TrimSpace(region)
		if region != "" {
			regPool := pool.NewConnectionPool(*maxConns, *dialectStr)
			srv.AddRegionPool(region, regPool)
			log.Printf("[INFO] Initialized regional replica pool for region %s", region)
		}
	}

	var peers []string
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")
	} else if envPeers := os.Getenv("SERVDB_PEERS"); envPeers != "" {
		peers = strings.Split(envPeers, ",")
	}
	for i, p := range peers {
		peers[i] = strings.TrimSpace(p)
	}
	srv.SetPeers(peers)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/api/version", ServShared.VersionHandler("servdb", "1.0.0"))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/api/db/query", srv.HandleQuery)
	mux.HandleFunc("/api/db/stats", srv.HandleStats)
	mux.HandleFunc("/api/db/analytics", srv.HandleAnalytics)
	mux.HandleFunc("/api/db/migrate", srv.HandleMigrate)
	mux.HandleFunc("/api/db/cache/clear", srv.HandleClearCache)
	mux.HandleFunc("/api/db/health", srv.HandleDbHealth)
	mux.HandleFunc("/metrics", srv.HandlePrometheusMetrics)

	serverHandler := ServShared.TraceMiddleware("servdb", ServShared.AuthMiddleware(mux))

	server := &http.Server{
		Addr:    ":" + port,
		Handler: serverHandler,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		certFile := os.Getenv("SERVDB_TLS_CERT")
		keyFile := os.Getenv("SERVDB_TLS_KEY")
		if certFile != "" && keyFile != "" {
			log.Printf("[INFO] ServDB starting with TLS on port %s", port)
			if err := EnterpriseListenAndServeTLS(server, certFile, keyFile); err != nil && err != http.ErrServerClosed {
				log.Fatalf("failed to start ServDB: %v", err)
			}
		} else {
			log.Printf("[INFO] ServDB connection pooler starting on port %s", port)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("failed to start ServDB: %v", err)
			}
		}
	}()

	<-stop

	log.Println("[INFO] Shutting down ServDB server...")
	ServShared.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("[WARN] Server shutdown failed: %v", err)
	}

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[WARN] Connection pools draining failed: %v", err)
	}

	log.Println("[INFO] ServDB server exited cleanly")
}
