package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestHTTPRejectsMalformedQueryBeforeBackendCall(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		for _, query := range []string{
			"key=%ZZ&key=victim",
			"key=victim&other=%",
			"key=victim&other=x;y",
		} {
			t.Run(method+"/"+query, func(t *testing.T) {
				client := &fakeClient{response: rpcResponse{status: statusMiss}}
				response := httptest.NewRecorder()
				newKVHandler(client).ServeHTTP(response, httptest.NewRequest(method, "/kv?"+query, nil))
				if response.Code != http.StatusBadRequest || client.calls != 0 {
					t.Fatalf("malformed query reached backend: code=%d calls=%d key=%q", response.Code, client.calls, client.request.key)
				}
			})
		}
		// Escaped query separators and percent signs remain valid key bytes.
		client := &fakeClient{response: rpcResponse{status: statusMiss}}
		response := httptest.NewRecorder()
		newKVHandler(client).ServeHTTP(response, httptest.NewRequest(method, "/kv?key=a%3Bb%26c%25d%2Be", nil))
		if response.Code != http.StatusNotFound || client.calls != 1 || client.request.key != "a;b&c%d+e" {
			t.Fatalf("valid escaped key changed: code=%d calls=%d key=%q", response.Code, client.calls, client.request.key)
		}
	}
}

// Serve a single HTTP connection without requiring a listening socket.
type httpPipeListener struct {
	conn      net.Conn
	accepted  sync.Once
	closeOnce sync.Once
	closed    chan struct{}
}

func (l *httpPipeListener) Accept() (net.Conn, error) {
	var conn net.Conn
	l.accepted.Do(func() { conn = l.conn })
	if conn != nil {
		return conn, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *httpPipeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *httpPipeListener) Addr() net.Addr { return l.conn.LocalAddr() }

func TestHTTPSlowValidUploadReceivesWriteAcknowledgement(t *testing.T) {
	backend := &fakeClient{response: rpcResponse{status: statusOK}}
	handlerDone := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		newKVHandler(backend).ServeHTTP(w, r)
	})
	serverConn, clientConn := net.Pipe()
	listener := &httpPipeListener{conn: serverConn, closed: make(chan struct{})}
	server := newHTTPServer("unused", handler, 2*time.Second)
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = server.Close()
		if err := <-serverDone; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("HTTP server shutdown: %v", err)
		}
	})
	if err := clientConn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	body := `{"key":"slow-upload","value":"stored"}`
	if _, err := fmt.Fprintf(clientConn, "POST /kv HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", len(body)); err != nil {
		t.Fatal(err)
	}
	// The old seven-second write deadline expired while this upload was still
	// within the fifteen-second read budget, even though the write committed.
	time.Sleep(8 * time.Second)
	if _, err := io.WriteString(clientConn, body); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	if err != nil {
		t.Fatalf("valid upload lost its write acknowledgement: %v", err)
	}
	defer response.Body.Close()
	result, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || string(result) != "OK\n" {
		t.Fatalf("response code=%d body=%q error=%v", response.StatusCode, result, err)
	}
	<-handlerDone
	if backend.calls != 1 || backend.request != (rpcRequest{op: opPut, key: "slow-upload", value: "stored"}) {
		t.Fatalf("unexpected backend write: calls=%d request=%#v", backend.calls, backend.request)
	}
}
