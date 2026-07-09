# ServDB Roadmap

This roadmap outlines the planned development phases for the ServDB database proxy service.

---

## Phase 1: Connection Proxying (In Progress)
- [x] **Connection pooling** — Shared pooling and connection reuse proxy. [June 29, 2026]
- [x] **Query routing** — Read replica routing, primary write routing. [June 29, 2026]
- [x] **Multi-database support** — Multi-dialect parser backend support. [June 29, 2026]
- [x] **Serv-lang integration** — Centralized client driver connection pool setup. [June 29, 2026]

- [x] **Slow query detection** — Slow query profiling telemetry. [June 29, 2026]
- [x] **Query analytics** — CPU cost and pattern aggregation. [June 29, 2026]
- [x] **Query caching** — Invalidation caching via ServCache. [June 29, 2026]
- [x] **Centralized migrations** — Centralized schema migration runner. [June 29, 2026]
- [x] **Database health** — Active lease counts and deadlock alert telemetry. [June 29, 2026]

## Phase 2: Production Hardening & Resilience (Completed)
- [x] **State-Isolated Unit & Validation Tests** — Table-driven checks for dialect mismatch placeholders and connection routing. [June 30, 2026]
- [x] **Cache Access Benchmarks** — Performance evaluation tests for read-heavy cache queries. [June 30, 2026]
- [x] **Interface Abstractions & Decoupled Storage** — Extract connection pool logic behind `PoolManager` and migrations behind `NewServer` config injection. [June 30, 2026]
- [x] **Structured Logging & OTel Tracing** — Add TraceMiddleware for tracing context propagation and JSON log format. [June 30, 2026]
- [x] **SIGTERM Graceful Shutdown** — Register listener to shut down HTTP listener cleanly with a 5-second timeout. [June 30, 2026]

## Phase 3: Architectural Depth (Pending)
- [x] **Ecosystem Performance Suite** — Multi-tiered Go micro-benchmarks, k6 component load tests, and distributed end-to-end telemetry workloads (OPS.7)
- [x] **Dynamic Connection Pool Tuning** — Adaptive pool sizing and automated invalidation invalidations (PS.1)
- [x] **Dynamic Active-Active Cluster Replication** — Multi-leader query statement replication across peers (HA.1) [July 1, 2026]
- [ ] **Secrets Envelope Key Rotation** — Secret KMS rotation schedule & API key hashing (SEC.8)

## Phase 4: Implementation Depth & Package Structure (Pending — July 2026)

> **Issues identified:** `migration.go` is 9 lines (stub only). No `pkg/` structure despite decomposition claims.

| # | Item | Effort | Description | Status |
|---|------|--------|-------------|--------|
| 4.1 | **Implement real migration engine** | Medium | `migration.go` is a 9-line stub. Implement SQL migration execution, rollback, version tracking, and conflict detection | [ ] |
| 4.2 | **Extract `pkg/pool/`** | Medium | Move connection pool management, health checking, and adaptive sizing into dedicated package | [ ] |
| 4.3 | **Extract `pkg/routing/`** | Small | Move read/write query routing logic into a package with strategy interface | [ ] |
| 4.4 | **Extract `pkg/analytics/`** | Small | Move slow query detection, pattern aggregation, and cost estimation into dedicated package | [ ] |
| 4.5 | **Query plan analysis** | Medium | Parse EXPLAIN output to detect missing indexes, sequential scans, and suggest optimizations | [ ] |
| 4.6 | **Connection pool metrics export** | Small | Export pool utilization, wait time, and connection churn to OTel metrics for ServConsole dashboards | [ ] |

## Phase 5: Advanced Database Proxy (Pending)
- [ ] **Prepared Statement Multiplexing** — Share prepared statements across connections in the pool to reduce DB-side overhead
- [ ] **Query Result Caching (Cache-Aside)** — Intercept SELECT queries and serve from ServCache with automatic invalidation on matching INSERT/UPDATE/DELETE
- [x] **Connection Draining** — Gracefully drain connections during rolling deploys; wait for in-flight queries before closing
- [ ] **Declarative Schema Migrations DSL** — Native `.srv` syntax for schema definitions compiled to migration SQL (DX.14)
- [x] **Multi-region Query Routing** — Route reads to geo-local replicas based on request origin metadata

> See [UNIFIED_ROADMAP.md](../servverse-repo/UNIFIED_ROADMAP.md) for the full ecosystem priority matrix.
