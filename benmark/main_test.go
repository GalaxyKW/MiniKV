package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
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
	client := newHTTPClient(benchConfig{timeout: time.Second, workers: 1, requests: 1})
	defer client.CloseIdleConnections()
	if err := preloadData(cfg, client); err != nil {
		t.Fatalf("preload=false still made a request: %v", err)
	}
	cfg.preload = true
	if err := preloadData(cfg, client); err == nil {
		t.Fatal("preload failure was ignored")
	}
}

func TestRunBoundsWorkersByRequests(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	// The first count passed the old config checks but its result array could
	// not fit in an int-sized address space. It must never be allocated here.
	for _, workers := range []int{maxInt/int(unsafe.Sizeof(benchResult{})) + 1, maxInt / 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			var received atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received.Add(1)
				fmt.Fprint(w, "VALUE result\n")
			}))
			defer server.Close()
			defer func() {
				if failure := recover(); failure != nil {
					t.Fatalf("one-request benchmark allocated resources for %d workers: %v", workers, failure)
				}
			}()
			var stdout, stderr bytes.Buffer
			code := run([]string{"-url", server.URL, "-op", "get", "-workers", strconv.Itoa(workers),
				"-requests", "1", "-preload=false", "-format", "json"}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("one-request benchmark exited %d: %s", code, stderr.String())
			}
			report := decodeOnlyReport(t, stdout.Bytes())
			if report.Config.Workers != workers || report.Config.Requests != 1 {
				t.Errorf("requested configuration was changed: %#v", report.Config)
			}
			if received.Load() != 1 || report.Outcomes != (outcomeReport{Requests: 1, Successes: 1}) ||
				report.LatencyNS.Samples != 1 || report.Operations["get"] != 1 {
				t.Errorf("incorrect single-request measurement: received=%d report=%#v", received.Load(), report)
			}
		})
	}
}

func TestHTTPConnectionPoolIsBoundedByRequestCount(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, test := range []struct {
		workers, requests, connections int
	}{
		{1, 10, 4}, {3, 10, 12}, {50, 2, 8}, {maxInt / 4, 1, 4},
	} {
		client := newHTTPClient(benchConfig{timeout: time.Second, workers: test.workers, requests: test.requests})
		transport := client.Transport.(*http.Transport)
		if transport.MaxIdleConns != test.connections || transport.MaxIdleConnsPerHost != test.connections {
			t.Errorf("workers=%d requests=%d: connection pool limits=%d/%d, want %d",
				test.workers, test.requests, transport.MaxIdleConns, transport.MaxIdleConnsPerHost, test.connections)
		}
		if client.Timeout != time.Second {
			t.Errorf("client timeout changed: %v", client.Timeout)
		}
		client.CloseIdleConnections()
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
			if result.errors+result.httpFailures+result.protocolFailures != result.failures || result.timeouts != 0 ||
				result.httpFailures != wantUnavailable+putNotFound.Load() || result.protocolFailures != wantMalformed {
				t.Errorf("failure categories do not preserve their distinct causes: %#v", result)
			}
			if result.operations["put"]+result.operations["get"]+result.operations["delete"] != int64(test.requests) {
				t.Errorf("operation counts do not add up to total requests: %v", result.operations)
			}
			wantStatuses := map[int]int64{
				// Body truncation is a transport failure after HTTP 200 was seen.
				http.StatusOK:                 wantSuccesses + wantMalformed + wantErrors,
				http.StatusNotFound:           wantNotFound,
				http.StatusServiceUnavailable: wantUnavailable,
			}
			if !reflect.DeepEqual(result.statusCount, wantStatuses) {
				t.Errorf("HTTP statuses=%v, want %v", result.statusCount, wantStatuses)
			}
		})
	}
}
