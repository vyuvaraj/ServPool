# ServPool

```bash
docker run -p 8087:8087 ghcr.io/vyuvaraj/servpool:latest
```

ServPool is a database connection pooler and query routing proxy service of the Servverse ecosystem.

## Features
- **Connection Pooling**: Multiplexes and manages active pooled database connections to reduce startup overhead.
- **Read/Write Query Routing**: Automatically splits incoming operations (routing mutations to the Primary pool and read queries starting with `SELECT` to the Replica pool).
- **Multi-Database Dialects**: Dialect safety checks supporting PostgreSQL and MySQL parameter format checks.

## API Endpoints
- `POST /api/db/query` - Route and execute an SQL statement
- `GET /api/db/stats` - Fetch connection pooling and query statistics per pool

## Getting Started
To run the integration tests locally:
```bash
go test -v ./...
```
