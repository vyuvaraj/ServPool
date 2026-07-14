package pool

import (
	"context"
	"testing"
	"time"
)

func TestNewConnectionPoolPostgres(t *testing.T) {
	p := NewConnectionPool(5, "postgres")
	defer p.Shutdown(context.Background())
	if p.dialect != "postgres" {
		t.Errorf("expected dialect postgres, got %s", p.dialect)
	}
	if p.maxConns != 5 {
		t.Errorf("expected maxConns 5, got %d", p.maxConns)
	}
}

func TestNewConnectionPoolSQLite(t *testing.T) {
	p := NewConnectionPool(10, "sqlite")
	defer p.Shutdown(context.Background())
	if p.dialect != "sqlite" {
		t.Errorf("expected dialect sqlite, got %s", p.dialect)
	}
}

func TestNewConnectionPoolInvalidDialect(t *testing.T) {
	p := NewConnectionPool(3, "invalid")
	defer p.Shutdown(context.Background())
	if p.dialect != "invalid" {
		t.Errorf("expected invalid, got %s", p.dialect)
	}
}

func TestConnectionPoolAcquireRelease(t *testing.T) {
	p := NewConnectionPool(2, "sqlite")
	defer p.Shutdown(context.Background())

	conn, err := p.Acquire()
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}
	if conn == nil {
		t.Fatal("acquired connection is nil")
	}

	p.Release(conn)
}

func TestConnectionPoolStats(t *testing.T) {
	p := NewConnectionPool(3, "postgres")
	defer p.Shutdown(context.Background())

	stats := p.Stats()
	if stats.Dialect != "postgres" {
		t.Errorf("expected stats dialect postgres, got %s", stats.Dialect)
	}
	if stats.MaxConnections != 3 {
		t.Errorf("expected MaxConnections 3, got %d", stats.MaxConnections)
	}
}

func TestConnectionPoolExhaustion(t *testing.T) {
	p := NewConnectionPool(1, "postgres")
	defer p.Shutdown(context.Background())

	c1, err := p.Acquire()
	if err != nil {
		t.Fatalf("c1 acquire failed: %v", err)
	}

	// Active conns: 1, baseMaxConns: 1. Adaptive size scales maxConns to 2
	c2, err := p.Acquire()
	if err != nil {
		t.Fatalf("c2 acquire failed (should adaptively scale): %v", err)
	}

	// Should exhaust now since we hit scaled maxConns (2)
	_, err = p.Acquire()
	if err == nil {
		t.Error("expected pool exhaustion error, got nil")
	}

	p.Release(c1)
	p.Release(c2)
}

func TestConnectionPoolReset(t *testing.T) {
	p := NewConnectionPool(2, "postgres")
	defer p.Shutdown(context.Background())
	p.IncrementQueries()
	stats := p.Stats()
	if stats.TotalQueries != 1 {
		t.Errorf("expected queries 1, got %d", stats.TotalQueries)
	}
}

func TestConnectionPoolCapacity(t *testing.T) {
	p := NewConnectionPool(4, "sqlite")
	defer p.Shutdown(context.Background())
	if p.Dialect() != "sqlite" {
		t.Errorf("expected sqlite dialect, got %s", p.Dialect())
	}
}

func TestPoolExhaustionAndRecovery(t *testing.T) {
	p := NewConnectionPool(1, "sqlite")
	defer p.Shutdown(context.Background())

	// Acquire 2 connections (base size 1, adaptively scales to 2)
	c1, err := p.Acquire()
	if err != nil {
		t.Fatalf("failed to acquire c1: %v", err)
	}
	c2, err := p.Acquire()
	if err != nil {
		t.Fatalf("failed to acquire c2: %v", err)
	}

	// 3rd acquire blocks in wait queue
	acquiredChan := make(chan *DbConn, 1)
	errChan := make(chan error, 1)
	go func() {
		conn, err := p.Acquire()
		if err != nil {
			errChan <- err
		} else {
			acquiredChan <- conn
		}
	}()

	// Wait briefly and verify it is indeed blocked
	time.Sleep(50 * time.Millisecond)
	select {
	case <-acquiredChan:
		t.Fatal("expected 3rd acquire to block, but it completed")
	case err := <-errChan:
		t.Fatalf("expected 3rd acquire to block, but it failed: %v", err)
	default:
		// Correct: blocked
	}

	// Release c1, should unblock the 3rd acquire immediately
	p.Release(c1)

	var c3 *DbConn
	select {
	case c3 = <-acquiredChan:
		if c3.ID != c1.ID {
			t.Errorf("expected to receive released connection ID %d, got %d", c1.ID, c3.ID)
		}
	case err := <-errChan:
		t.Fatalf("acquire failed after release: %v", err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for 3rd acquire to unblock")
	}

	// Clean up
	p.Release(c2)
	if c3 != nil {
		p.Release(c3)
	}
}
