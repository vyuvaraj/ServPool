package pool

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"
)

// PoolStats holds runtime metrics for a connection pool.
type PoolStats struct {
	ActiveConnections int    `json:"active_connections"`
	IdleConnections   int    `json:"idle_connections"`
	MaxConnections    int    `json:"max_connections"`
	TotalQueries      int64  `json:"total_queries"`
	Dialect           string `json:"dialect"`
}

// DbConn represents a single database connection handle.
type DbConn struct {
	ID           int
	CreatedAt    time.Time
	CheckedOutAt time.Time
}

// Manager is the interface every pool implementation must satisfy.
type Manager interface {
	Acquire() (*DbConn, error)
	Release(conn *DbConn)
	IncrementQueries()
	Stats() PoolStats
	Dialect() string
	Shutdown(ctx context.Context) error
}

// ConnectionPool is the in-process adaptive connection pool.
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

// NewConnectionPool creates and starts a new ConnectionPool.
func NewConnectionPool(max int, dialect string) *ConnectionPool {
	p := &ConnectionPool{
		baseMaxConns: max,
		maxConns:     max,
		activeConns:  make(map[int]*DbConn),
		idleConns:    make([]*DbConn, 0),
		dialect:      dialect,
		maxLifetime:  5 * time.Second,
		lastActive:   time.Now(),
		shutdownChan: make(chan struct{}),
	}
	go p.startPoolJanitor(100 * time.Millisecond)
	return p
}

func (p *ConnectionPool) Dialect() string { return p.dialect }

// SetMaxLifetime configures the maximum lifetime of idle connections.
// Primarily useful in tests to shorten lifetimes.
func (p *ConnectionPool) SetMaxLifetime(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxLifetime = d
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

			// 2. Shrink pool limit back to baseMaxConns after cooldown
			if len(p.activeConns) < p.baseMaxConns/2 && p.maxConns > p.baseMaxConns {
				if now.Sub(p.lastActive) > 1*time.Second {
					p.maxConns = p.baseMaxConns
				}
			}

			// 3. Reclaim deadlocked connection leases exceeding timeout
			timeout := 5 * time.Second
			if tStr := os.Getenv("SERVDB_CONN_TIMEOUT"); tStr != "" {
				if d, err := time.ParseDuration(tStr); err == nil {
					timeout = d
				}
			}
			for id, conn := range p.activeConns {
				if !conn.CheckedOutAt.IsZero() && now.Sub(conn.CheckedOutAt) > timeout {
					conn.CheckedOutAt = time.Time{}
					delete(p.activeConns, id)
					p.idleConns = append(p.idleConns, conn)
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

	// 1. Recycle a fresh idle connection
	for len(p.idleConns) > 0 {
		conn := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		if time.Since(conn.CreatedAt) > p.maxLifetime {
			continue
		}
		conn.CheckedOutAt = time.Now()
		p.activeConns[conn.ID] = conn
		return conn, nil
	}

	// 2. Adaptive sizing: scale up to 2× when close to exhaustion
	if len(p.activeConns) >= p.maxConns && p.maxConns < p.baseMaxConns*2 {
		p.maxConns = p.baseMaxConns * 2
	}

	// 3. Create new connection if under limit
	if len(p.activeConns) < p.maxConns {
		p.nextConnID++
		conn := &DbConn{
			ID:           p.nextConnID,
			CreatedAt:    time.Now(),
			CheckedOutAt: time.Now(),
		}
		p.activeConns[conn.ID] = conn
		return conn, nil
	}

	return nil, errors.New("connection pool exhausted")
}

func (p *ConnectionPool) Release(conn *DbConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	conn.CheckedOutAt = time.Time{}
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
