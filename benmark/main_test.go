package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestClassifyRequiresBothStatusAndProtocol(t *testing.T) {
	tests := []struct {
		op            string
		status        int
		body          string
		success, miss bool
	}{
		{"put", 200, "OK\n", true, false},
		{"put", 200, "ERROR write failed\n", false, false},
		{"get", 503, "NOT_FOUND\n", false, false},
		{"get", 404, "NOT_FOUND\n", false, true},
		{"delete", 404, "NOT_FOUND\n", false, true},
		{"get", 200, "VALUE \n", true, false},
		{"put", 404, "NOT_FOUND\n", false, false},
	}
	for _, test := range tests {
		success, miss := classifyResult(test.op, test.status, test.body)
		if success != test.success || miss != test.miss {
			t.Fatalf("misclassified %#v: success=%v miss=%v", test, success, miss)
		}
	}
}

func TestPreloadOptOutAndFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "disk error", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg := benchConfig{baseURL: server.URL, op: "mixed", keyspace: 1, preloadCount: 1, valueSize: 1}
	client := newHTTPClient(time.Second, 1)
	defer client.CloseIdleConnections()
	if err := preloadData(cfg, client); err != nil {
		t.Fatalf("preload=false still made a request: %v", err)
	}
	cfg.preload = true
	if err := preloadData(cfg, client); err == nil {
		t.Fatal("preload failure was ignored")
	}
}

func TestRunBenchmarkAggregatesAllOutcomes(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		workers  int
		requests int
	}{
		{"serial_get", "get", 1, 5},
		{"parallel_mixed", "mixed", 8, 103},
		{"more_workers_than_requests", "delete", 16, 5},
		{"parallel_put", "put", 8, 103},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var received atomic.Int64
			var putNotFound atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch (received.Add(1) - 1) % 5 {
				case 0:
					if r.Method == http.MethodGet {
						fmt.Fprint(w, "VALUE result\n")
					} else {
						fmt.Fprint(w, "OK\n")
					}
				case 1:
					if r.Method == http.MethodPost {
						putNotFound.Add(1)
					}
					http.Error(w, "NOT_FOUND", http.StatusNotFound)
				case 2:
					http.Error(w, "busy", http.StatusServiceUnavailable)
				case 3:
					fmt.Fprint(w, "invalid response\n")
				case 4:
					// A truncated body fails during ReadAll, so an idempotent request
					// cannot be transparently retried by the HTTP transport.
					w.Header().Set("Content-Length", "100")
					fmt.Fprint(w, "truncated")
				}
			}))
			defer server.Close()
			cfg := benchConfig{
				baseURL: server.URL, workers: test.workers, requests: test.requests,
				op: test.op, keyspace: 23, timeout: 5 * time.Second,
				writeRatio: 30, deleteRatio: 20, seed: 1, valueSize: 128,
			}
			result, err := runBenchmark(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if got := received.Load(); got != int64(test.requests) {
				t.Fatalf("server received %d requests, want %d", got, test.requests)
			}
			if result.total != test.requests || len(result.latencies) != test.requests {
				t.Fatalf("total=%d latencies=%d, want %d each", result.total, len(result.latencies), test.requests)
			}
			for i, latency := range result.latencies {
				if latency <= 0 {
					t.Errorf("request %d has no positive latency: %v", i, latency)
				}
			}
			if result.elapsed <= 0 {
				t.Errorf("elapsed=%v, want positive duration", result.elapsed)
			}

			wantSuccesses := int64((test.requests + 4) / 5)
			wantNotFound := int64((test.requests + 3) / 5)
			wantUnavailable := int64((test.requests + 2) / 5)
			wantMalformed := int64((test.requests + 1) / 5)
			wantErrors := int64(test.requests / 5)
			wantMisses := wantNotFound - putNotFound.Load()
			wantFailures := wantUnavailable + wantMalformed + wantErrors + putNotFound.Load()
			if result.successes != wantSuccesses || result.logicalMisses != wantMisses ||
				result.failures != wantFailures || result.errors != wantErrors {
				t.Errorf("success/miss/failure/error = %d/%d/%d/%d, want %d/%d/%d/%d",
					result.successes, result.logicalMisses, result.failures, result.errors,
					wantSuccesses, wantMisses, wantFailures, wantErrors)
			}
			if result.successes+result.logicalMisses+result.failures != int64(result.total) {
				t.Error("outcome counts do not add up to total requests")
			}
			wantStatuses := map[int]int64{
				http.StatusOK:                 wantSuccesses + wantMalformed,
				http.StatusNotFound:           wantNotFound,
				http.StatusServiceUnavailable: wantUnavailable,
			}
			if !reflect.DeepEqual(result.statusCount, wantStatuses) {
				t.Errorf("HTTP statuses=%v, want %v", result.statusCount, wantStatuses)
			}
		})
	}
}
