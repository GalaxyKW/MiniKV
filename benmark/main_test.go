package main

import (
	"net/http"
	"net/http/httptest"
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
