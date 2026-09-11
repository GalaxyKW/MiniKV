package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type fakeClient struct {
	request  rpcRequest
	response rpcResponse
	err      error
	calls    int
}

func (client *fakeClient) execute(_ context.Context, request rpcRequest) (rpcResponse, error) {
	client.request = request
	client.calls++
	return client.response, client.err
}

func TestHTTPPreservesValueBytes(t *testing.T) {
	key, value := "tenant:1 \n", " \nhello\x00\t\nDEL victim\n "
	client := &fakeClient{response: rpcResponse{status: statusOK}}
	body, _ := json.Marshal(KVRequest{Key: key, Value: value})
	response := httptest.NewRecorder()
	newKVHandler(client).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/kv", strings.NewReader(string(body))))
	if response.Code != http.StatusOK || client.request.key != key || client.request.value != value {
		t.Fatalf("POST changed data: code=%d request=%#v", response.Code, client.request)
	}
	client.response = rpcResponse{status: statusValue, value: value}
	response = httptest.NewRecorder()
	newKVHandler(client).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/kv?key="+url.QueryEscape(key), nil))
	if response.Code != http.StatusOK || response.Body.String() != "VALUE "+value+"\n" {
		t.Fatalf("GET trimmed or changed value: %q", response.Body.String())
	}
}

func TestHTTPValidationAndStatusMapping(t *testing.T) {
	tests := []struct {
		name, method, target, body string
		response                   rpcResponse
		err                        error
		code, calls                int
	}{
		{name: "missing key", method: "GET", target: "/kv", code: 400},
		{name: "unknown JSON field", method: "POST", target: "/kv", body: `{"key":"k","extra":1}`, code: 400},
		{name: "trailing JSON", method: "POST", target: "/kv", body: `{"key":"k"}{"key":"other"}`, code: 400},
		{name: "oversized value", method: "POST", target: "/kv", body: `{"key":"k","value":"` + strings.Repeat("x", maxValueSize+1) + `"}`, code: 413},
		{name: "unsupported method", method: "PATCH", target: "/kv", code: 405},
		{name: "missing value", method: "GET", target: "/kv?key=k", response: rpcResponse{status: statusMiss}, code: 404, calls: 1},
		{name: "disk failure", method: "POST", target: "/kv", body: `{"key":"k"}`, response: rpcResponse{status: statusIO}, code: 503, calls: 1},
		{name: "overload", method: "GET", target: "/kv?key=k", response: rpcResponse{status: statusBusy}, code: 503, calls: 1},
		{name: "RPC timeout", method: "GET", target: "/kv?key=k", err: context.DeadlineExceeded, code: 504, calls: 1},
		{name: "RPC failure", method: "GET", target: "/kv?key=k", err: errors.New("broken pipe"), code: 502, calls: 1},
		{name: "unexpected response", method: "GET", target: "/kv?key=k", response: rpcResponse{status: statusOK}, code: 502, calls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{response: test.response, err: test.err}
			response := httptest.NewRecorder()
			newKVHandler(client).ServeHTTP(response, httptest.NewRequest(test.method, test.target, strings.NewReader(test.body)))
			if response.Code != test.code || client.calls != test.calls {
				t.Fatalf("code=%d calls=%d, want code=%d calls=%d", response.Code, client.calls, test.code, test.calls)
			}
		})
	}
}
