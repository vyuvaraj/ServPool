package main

import (
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

func (p *ConnectionPool) Dialect() string {
	return p.dialect
}

func (p *ConnectionPool) Acquire() (*DbConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.idleConns) > 0 {
		conn := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		p.activeConns[conn.ID] = conn
		return conn, nil
	}

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
