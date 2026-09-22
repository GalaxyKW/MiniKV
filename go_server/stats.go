package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// Counters contain no request keys, values, or unbounded labels. Each atomic
// observation is independent; a report is not a transaction across processes.
type rpcMetrics struct {
	calls          atomic.Uint64
	errors         atomic.Uint64
	retries        atomic.Uint64
	poolAcquires   atomic.Uint64
	poolWaitNS     atomic.Uint64
	exchanges      atomic.Uint64
	exchangeErrors atomic.Uint64
	exchangeNS     atomic.Uint64
}

type rpcStats struct {
	PoolCapacity        int    `json:"pool_capacity"`
	PoolInUse           int    `json:"pool_in_use"`
	Connections         int    `json:"connections"`
	IdleConnections     int    `json:"idle_connections"`
	Closed              bool   `json:"closed"`
	CallsTotal          uint64 `json:"calls_total"`
	ErrorsTotal         uint64 `json:"errors_total"`
	RetriesTotal        uint64 `json:"retries_total"`
	PoolAcquiresTotal   uint64 `json:"pool_acquires_total"`
	PoolWaitDurationNS  uint64 `json:"pool_wait_duration_ns_total"`
	ExchangesTotal      uint64 `json:"exchanges_total"`
	ExchangeErrorsTotal uint64 `json:"exchange_errors_total"`
	ExchangeDurationNS  uint64 `json:"exchange_duration_ns_total"`
}

func (c *rpcClient) stats() rpcStats {
	c.mu.Lock()
	result := rpcStats{
		PoolCapacity: cap(c.slots), PoolInUse: len(c.slots),
		Connections: len(c.connections), IdleConnections: len(c.idle), Closed: c.closed,
	}
	c.mu.Unlock()
	result.CallsTotal = c.metrics.calls.Load()
	result.ErrorsTotal = c.metrics.errors.Load()
	result.RetriesTotal = c.metrics.retries.Load()
	result.PoolAcquiresTotal = c.metrics.poolAcquires.Load()
	result.PoolWaitDurationNS = c.metrics.poolWaitNS.Load()
	result.ExchangesTotal = c.metrics.exchanges.Load()
	result.ExchangeErrorsTotal = c.metrics.exchangeErrors.Load()
	result.ExchangeDurationNS = c.metrics.exchangeNS.Load()
	return result
}

type engineStats struct {
	WalMode                 string `json:"wal_mode"`
	Keys                    uint64 `json:"keys"`
	AppliedSequence         uint64 `json:"applied_sequence"`
	DurableSequence         uint64 `json:"durable_sequence"`
	WalPendingBytes         uint64 `json:"wal_pending_bytes"`
	WalInflightBytes        uint64 `json:"wal_inflight_bytes"`
	WalQueuedRecords        uint64 `json:"wal_queued_records"`
	WalQueueCapacityBytes   uint64 `json:"wal_queue_capacity_bytes"`
	WalCommitsTotal         uint64 `json:"wal_commits_total"`
	WalCommitFailuresTotal  uint64 `json:"wal_commit_failures_total"`
	WalCommitDurationNS     uint64 `json:"wal_commit_duration_ns_total"`
	WalCommitLastDurationNS uint64 `json:"wal_commit_last_duration_ns"`
	// Optional counters distinguish unavailable fields from a measured zero
	// across engine versions that add wait, asynchronous request and snapshot metrics.
	WalCapacityWaiters                        *uint64 `json:"wal_capacity_waiters,omitempty"`
	WalCapacityWaitsTotal                     *uint64 `json:"wal_capacity_waits_total,omitempty"`
	WalCapacityWaitDurationNS                 *uint64 `json:"wal_capacity_wait_duration_ns_total,omitempty"`
	WalDurableWaiters                         *uint64 `json:"wal_durable_waiters,omitempty"`
	WalDurableWaitsTotal                      *uint64 `json:"wal_durable_waits_total,omitempty"`
	WalDurableWaitDurationNS                  *uint64 `json:"wal_durable_wait_duration_ns_total,omitempty"`
	AsyncRequestsInflight                     *uint64 `json:"async_requests_inflight,omitempty"`
	AsyncRequestsCapacity                     *uint64 `json:"async_requests_capacity,omitempty"`
	AsyncCallbackFailuresTotal                *uint64 `json:"async_callback_failures_total,omitempty"`
	SnapshotSuccessesTotal                    uint64  `json:"snapshot_successes_total"`
	SnapshotFailuresTotal                     uint64  `json:"snapshot_failures_total"`
	SnapshotInProgress                        bool    `json:"snapshot_in_progress"`
	SnapshotSequence                          uint64  `json:"snapshot_sequence"`
	SnapshotCaptureDurationNS                 uint64  `json:"snapshot_capture_duration_ns_total"`
	SnapshotWriteDurationNS                   uint64  `json:"snapshot_write_duration_ns_total"`
	SnapshotCompactDurationNS                 uint64  `json:"snapshot_compact_duration_ns_total"`
	SnapshotCaptureStateLockAcquisitionsTotal *uint64 `json:"snapshot_capture_state_lock_acquisitions_total,omitempty"`
	SnapshotCaptureStateLockDurationNS        *uint64 `json:"snapshot_capture_state_lock_duration_ns_total,omitempty"`
	SnapshotCaptureStateLockMaxDurationNS     *uint64 `json:"snapshot_capture_state_lock_duration_ns_max,omitempty"`
	SnapshotFileWriteCallsTotal               *uint64 `json:"snapshot_file_write_calls_total,omitempty"`
	SnapshotFileWrittenBytesTotal             *uint64 `json:"snapshot_file_written_bytes_total,omitempty"`
	SnapshotFileInstalledBytesTotal           *uint64 `json:"snapshot_file_installed_bytes_total,omitempty"`
	SnapshotCompactWrittenBytesTotal          *uint64 `json:"snapshot_compact_written_bytes_total,omitempty"`
	IOFailed                                  bool    `json:"io_failed"`
	Stopping                                  bool    `json:"stopping"`
}

type serverStats struct {
	Connections                uint64  `json:"connections"`
	ConnectionCapacity         uint64  `json:"connection_capacity"`
	RequestQueueDepth          uint64  `json:"request_queue_depth"`
	RequestQueueCapacity       uint64  `json:"request_queue_capacity"`
	WorkersActive              uint64  `json:"workers_active"`
	WorkersCapacity            uint64  `json:"workers_capacity"`
	RequestsRejectedTotal      uint64  `json:"requests_rejected_total"`
	ConnectionsRejectedTotal   uint64  `json:"connections_rejected_total"`
	RequestsStartedTotal       *uint64 `json:"requests_started_total,omitempty"`
	RequestQueueWaitDurationNS *uint64 `json:"request_queue_wait_duration_ns_total,omitempty"`
	RequestsInflight           *uint64 `json:"requests_inflight,omitempty"`
	RequestsCapacity           *uint64 `json:"requests_capacity,omitempty"`
}

type gatewayStats struct {
	UptimeSeconds float64  `json:"uptime_seconds"`
	RPC           rpcStats `json:"rpc"`
}

type runtimeStats struct {
	SchemaVersion int           `json:"schema_version"`
	Engine        *engineStats  `json:"engine,omitempty"`
	Server        *serverStats  `json:"server,omitempty"`
	Gateway       *gatewayStats `json:"gateway,omitempty"`
	Error         string        `json:"error,omitempty"`
}

// These fields are part of the original stats schema. Newer optional counters
// use pointers above; absent base measurements must never become measured zero.
var requiredStatsFields = map[string][]string{
	"engine": {
		"wal_mode", "keys", "applied_sequence", "durable_sequence", "wal_pending_bytes",
		"wal_inflight_bytes", "wal_queued_records", "wal_queue_capacity_bytes",
		"wal_commits_total", "wal_commit_failures_total", "wal_commit_duration_ns_total",
		"wal_commit_last_duration_ns", "snapshot_successes_total", "snapshot_failures_total",
		"snapshot_in_progress", "snapshot_sequence", "snapshot_capture_duration_ns_total",
		"snapshot_write_duration_ns_total", "snapshot_compact_duration_ns_total", "io_failed", "stopping",
	},
	"server": {
		"connections", "connection_capacity", "request_queue_depth", "request_queue_capacity",
		"workers_active", "workers_capacity", "requests_rejected_total", "connections_rejected_total",
	},
}

func decodeStats(payload string) (runtimeStats, error) {
	data := []byte(payload)
	var result runtimeStats
	if err := json.Unmarshal(data, &result); err != nil {
		return runtimeStats{}, err
	}
	if result.SchemaVersion != 1 || result.Engine == nil || result.Server == nil || result.Error != "" {
		return runtimeStats{}, errors.New("invalid stats envelope")
	}
	var raw struct {
		Engine map[string]json.RawMessage `json:"engine"`
		Server map[string]json.RawMessage `json:"server"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return runtimeStats{}, err
	}
	for section, fields := range map[string]map[string]json.RawMessage{"engine": raw.Engine, "server": raw.Server} {
		for _, name := range requiredStatsFields[section] {
			value := bytes.TrimSpace(fields[name])
			if len(value) == 0 || bytes.Equal(value, []byte("null")) {
				return runtimeStats{}, errors.New("missing or null required stats field")
			}
		}
	}
	engine, server := result.Engine, result.Server
	if (engine.WalMode != "throughput" && engine.WalMode != "reliable") ||
		engine.DurableSequence > engine.AppliedSequence || engine.WalInflightBytes > engine.WalPendingBytes ||
		engine.WalQueueCapacityBytes == 0 || server.WorkersCapacity == 0 || server.RequestQueueCapacity == 0 ||
		server.ConnectionCapacity == 0 || server.WorkersActive > server.WorkersCapacity ||
		server.RequestQueueDepth > server.RequestQueueCapacity || server.Connections > server.ConnectionCapacity {
		return runtimeStats{}, errors.New("invalid stats state")
	}
	// Re-encode only the defined aggregate fields; never forward arbitrary backend
	// JSON or its error text through an operational endpoint.
	result.Gateway = nil
	return result, nil
}

func newStatsHandler(client commandClient, dataClient *rpcClient, started time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		response, err := client.execute(r.Context(), rpcRequest{op: opStats})
		result := runtimeStats{SchemaVersion: 1}
		code := http.StatusOK
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			result.Error, code = "backend_timeout", http.StatusGatewayTimeout
		case err != nil:
			result.Error, code = "backend_unavailable", http.StatusServiceUnavailable
		case response.status == statusBusy || response.status == statusIO:
			result.Error, code = "backend_unavailable", http.StatusServiceUnavailable
		case response.status == statusBad:
			result.Error, code = "stats_unsupported", http.StatusBadGateway
		case response.status != statusValue:
			result.Error, code = "invalid_backend_stats", http.StatusBadGateway
		default:
			result, err = decodeStats(response.value)
			if err != nil {
				result = runtimeStats{SchemaVersion: 1, Error: "invalid_backend_stats"}
				code = http.StatusBadGateway
			}
		}
		result.Gateway = &gatewayStats{UptimeSeconds: time.Since(started).Seconds(), RPC: dataClient.stats()}
		payload, err := json.Marshal(result)
		if err != nil {
			http.Error(w, "Cannot encode runtime stats", http.StatusInternalServerError)
			return
		}
		payload = append(payload, '\n')
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(code)
		_, _ = w.Write(payload)
	}
}
