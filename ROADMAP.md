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
- [ ] **Secrets Envelope Key Rotation** — Secret KMS rotation schedule & API key hashing (SEC.8)
