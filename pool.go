package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

type PoolStats struct {
	ActiveConnections int    `json:"active_connections"`
	IdleConnections   int    `json:"idle_connections"`
	MaxConnections    int    `json:"max_connections"`
	TotalQueries      int64  `json:"total_queries"`
	Dialect           string `json:"dialect"`
}

type DbConn struct {
	ID        int
	CreatedAt time.Time
}

type PoolManager interface {
	Acquire() (*DbConn, error)
	Release(conn *DbConn)
	IncrementQueries()
	Stats() PoolStats
	Dialect() string
	Shutdown(ctx context.Context) error
}

type ConnectionPool struct {
	mu           sync.Mutex
	baseMaxConns int
	maxConns     int
	activeConns  map[int]*DbConn
	idleConns    []*DbConn
	totalQueries int64
	nextConnID   int
	dialect      string
	maxLifetime  time.Duration
	lastActive   time.Time
	shutdownChan chan struct{}
	isShutdown   bool
}

func NewConnectionPool(max int, dialect string) *ConnectionPool {
	p := &ConnectionPool{
		baseMaxConns: max,
		maxConns:     max,
		activeConns:  make(map[int]*DbConn),
		idleConns:    make([]*DbConn, 0),
		dialect:      dialect,
		maxLifetime:  5 * time.Second, // Max lifetime for connection invalidation (short for tests)
		lastActive:   time.Now(),
		shutdownChan: make(chan struct{}),
	}

	// Start background janitor to clean up expired idle connections and scale down pool
	go p.startPoolJanitor(100 * time.Millisecond)

	return p
}

func (p *ConnectionPool) Dialect() string {
	return p.dialect
}

func (p *ConnectionPool) startPoolJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.mu.Lock()
			now := time.Now()
			
			// 1. Invalidate stale idle connections
			var freshIdle []*DbConn
			for _, conn := range p.idleConns {
				if now.Sub(conn.CreatedAt) <= p.maxLifetime {
					freshIdle = append(freshIdle, conn)
				}
			}
			p.idleConns = freshIdle

			// 2. Shrink pool limit back to baseMaxConns if idle for a cooldown period
			if len(p.activeConns) < p.baseMaxConns/2 && p.maxConns > p.baseMaxConns {
				if now.Sub(p.lastActive) > 1*time.Second { // Cooldown for scaling down (short for tests)
					p.maxConns = p.baseMaxConns
				}
			}
			p.mu.Unlock()
		case <-p.shutdownChan:
			return
		}
	}
}

func (p *ConnectionPool) Acquire() (*DbConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isShutdown {
		return nil, errors.New("connection pool is shutting down")
	}

	p.lastActive = time.Now()

	// 1. Recycle an idle connection, checking for freshness
	for len(p.idleConns) > 0 {
		conn := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]

		if time.Since(conn.CreatedAt) > p.maxLifetime {
			// Invalidate stale connection
			continue
		}

		p.activeConns[conn.ID] = conn
		return conn, nil
	}

	// 2. Adaptive pool sizing: Scale pool limit up to 2x if close to exhaustion
	if len(p.activeConns) >= p.maxConns && p.maxConns < p.baseMaxConns*2 {
		p.maxConns = p.baseMaxConns * 2
	}

	// 3. Create new connection if under maxConns limit
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

func (p *ConnectionPool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.isShutdown {
		p.mu.Unlock()
		return nil
	}
	p.isShutdown = true
	close(p.shutdownChan)
	p.mu.Unlock()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		p.mu.Lock()
		activeCount := len(p.activeConns)
		p.mu.Unlock()

		if activeCount == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
