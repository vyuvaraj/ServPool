package analytics

import "time"

// QueryMetric tracks call count and cumulative latency for a single query pattern.
type QueryMetric struct {
	Count        int64 `json:"count"`
	TotalLatency int64 `json:"total_latency_ms"`
}

// CachedResult holds a cached query result set with TTL metadata.
type CachedResult struct {
	Rows      []map[string]interface{} `json:"rows"`
	CachedAt  time.Time                `json:"cached_at"`
	ExpiresAt time.Time                `json:"expires_at"`
}
