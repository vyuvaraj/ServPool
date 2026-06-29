# ServDB Roadmap

This roadmap outlines the planned development phases for the ServDB database proxy service.

---

## Phase 1: Connection Proxying (In Progress)
- [x] **Connection pooling** — Shared pooling and connection reuse proxy. [June 29, 2026]
- [x] **Query routing** — Read replica routing, primary write routing. [June 29, 2026]
- [x] **Multi-database support** — Multi-dialect parser backend support. [June 29, 2026]
- [ ] **Serv-lang integration** — Centralized client driver connection pool setup.

## Phase 2: Analytics & Performance Optimization
- [ ] **Slow query detection** — Slow query profiling telemetry.
- [ ] **Query analytics** — CPU cost and pattern aggregation.
- [ ] **Query caching** — Invalidation caching via ServCache.
- [ ] **Centralized migrations** — Centralized schema migration runner.
