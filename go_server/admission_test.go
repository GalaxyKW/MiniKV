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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type admissionCommandFunc func(context.Context, rpcRequest) (rpcResponse, error)

func (f admissionCommandFunc) execute(ctx context.Context, request rpcRequest) (rpcResponse, error) {
	return f(ctx, request)
}

type admissionObservedBody struct {
	reader  io.Reader
	reads   atomic.Int32
	closes  atomic.Int32
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *admissionObservedBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	if b.started != nil {
		b.once.Do(func() { close(b.started) })
		<-b.release
	}
	return b.reader.Read(p)
}

func (b *admissionObservedBody) Close() error {
	b.closes.Add(1)
	return nil
}

func admissionWait(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestAdmissionRejectsBeforeReadingBody(t *testing.T) {
	admission := newRequestAdmission(1)
	var calls atomic.Int32
	backend := admissionCommandFunc(func(context.Context, rpcRequest) (rpcResponse, error) {
		calls.Add(1)
		return rpcResponse{status: statusOK}, nil
	})
	handler := admission.wrap(newKVHandler(backend))
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	body := &admissionObservedBody{
		reader:  strings.NewReader(`{"key":"upload","value":"value"}`),
		started: make(chan struct{}), release: release,
	}
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/kv", body))
	}()
	admissionWait(t, body.started, "admitted upload body read")
	if stats := admission.stats(); stats.Capacity != 1 || stats.Inflight != 1 || stats.RejectedTotal != 0 {
		t.Fatalf("upload is not counted while its body is blocked: %#v", stats)
	}

	rejectedBody := &admissionObservedBody{reader: strings.NewReader(`{"key":"rejected"}`)}
	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, httptest.NewRequest(http.MethodPost, "/kv", rejectedBody))
	if rejected.Code != http.StatusServiceUnavailable || rejected.Header().Get("Connection") != "close" ||
		rejectedBody.reads.Load() != 0 || rejectedBody.closes.Load() != 0 || calls.Load() != 0 {
		t.Fatalf("rejection touched the body or dispatched a request: code=%d connection=%q reads=%d closes=%d calls=%d",
			rejected.Code, rejected.Header().Get("Connection"), rejectedBody.reads.Load(), rejectedBody.closes.Load(), calls.Load())
	}
	if stats := admission.stats(); stats.Inflight != 1 || stats.RejectedTotal != 1 {
		t.Fatalf("rejection changed the active upload count: %#v", stats)
	}
	releaseOnce.Do(func() { close(release) })
	admissionWait(t, done, "upload completion")
	if response.Code != http.StatusOK || calls.Load() != 1 || admission.stats().Inflight != 0 {
		t.Fatalf("upload did not release admission: code=%d calls=%d stats=%#v", response.Code, calls.Load(), admission.stats())
	}
}

func TestAdmissionHTTPRejectsWithoutWaitingForBody(t *testing.T) {
	for _, test := range []struct{ name, headers string }{
		{"content_length", "Content-Length: 32\r\n"},
		{"chunked", "Transfer-Encoding: chunked\r\n"},
		{"expect_continue", "Content-Length: 32\r\nExpect: 100-continue\r\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			admission := newRequestAdmission(1)
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			handler := admission.wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				close(started)
				<-release
			}))
			go func() {
				defer close(done)
				handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/kv", nil))
			}()
			admissionWait(t, started, "occupied admission slot")

			serverConn, clientConn := net.Pipe()
			listener := &httpPipeListener{conn: serverConn, closed: make(chan struct{})}
			server := newHTTPServer("unused", handler, time.Second)
			serverDone := make(chan error, 1)
			go func() { serverDone <- server.Serve(listener) }()
			t.Cleanup(func() {
				_ = clientConn.Close()
				_ = server.Close()
				select {
				case err := <-serverDone:
					if !errors.Is(err, http.ErrServerClosed) {
						t.Errorf("HTTP server shutdown: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Error("HTTP server did not shut down")
				}
			})
			if err := clientConn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			// Send no body bytes. Reading or explicitly closing the body in the
			// rejection path would wait for the longer server read timeout.
			if _, err := fmt.Fprintf(clientConn, "POST /kv HTTP/1.1\r\nHost: localhost\r\n%s\r\n", test.headers); err != nil {
				t.Fatal(err)
			}
			response, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
			if err != nil {
				t.Fatalf("rejection waited for an unsent body: %v", err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != http.StatusServiceUnavailable || !response.Close ||
				string(body) != "Gateway request capacity exhausted\n" {
				t.Fatalf("unexpected immediate response: code=%d close=%v body=%q error=%v", response.StatusCode, response.Close, body, err)
			}
			// Connection cleanup may still await net/http's read deadline; only
			// the complete 503 response must arrive without reading the upload.
			releaseOnce.Do(func() { close(release) })
			admissionWait(t, done, "admitted handler completion")
		})
	}
}

func TestAdmissionBoundsWaitersBehindRPCPool(t *testing.T) {
	const capacity, rejectedCount = 3, 64
	admission := newRequestAdmission(capacity)
	client := newRPCClient("unused", 1, 10*time.Second)
	defer client.Close()
	backendStarted := make(chan struct{})
	backendDone := make(chan struct{})
	client.dial = func(context.Context) (net.Conn, error) {
		local, remote := net.Pipe()
		go func() {
			defer close(backendDone)
			defer remote.Close()
			if _, err := readTestRequest(remote); err != nil {
				return
			}
			close(backendStarted)
			var byte [1]byte
			_, _ = remote.Read(byte[:]) // Wait for cancellation, sending no reply.
		}()
		return local, nil
	}
	handler := admission.wrap(newKVHandler(client))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan int, capacity)
	for i := 0; i < capacity; i++ {
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/kv", strings.NewReader(`{"key":"queued","value":"value"}`)).WithContext(ctx))
			finished <- response.Code
		}()
	}
	admissionWait(t, backendStarted, "blocked real RPC")
	deadline := time.Now().Add(3 * time.Second)
	for client.stats().PoolAcquiresTotal != capacity {
		if time.Now().After(deadline) {
			t.Fatalf("admitted calls did not reach the RPC pool: %#v", client.stats())
		}
		time.Sleep(time.Millisecond)
	}
	if stats := client.stats(); stats.PoolInUse != 1 || stats.ExchangesTotal != 1 || admission.stats().Inflight != capacity {
		t.Fatalf("expected one RPC and two bounded waiters: rpc=%#v admission=%#v", stats, admission.stats())
	}
	var reads atomic.Int32
	rejections := make(chan int, rejectedCount)
	for i := 0; i < rejectedCount; i++ {
		go func() {
			body := &admissionObservedBody{reader: strings.NewReader(`{"key":"overflow"}`)}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/kv", body))
			reads.Add(body.reads.Load())
			rejections <- response.Code
		}()
	}
	for i := 0; i < rejectedCount; i++ {
		select {
		case code := <-rejections:
			if code != http.StatusServiceUnavailable {
				t.Fatalf("overflow request returned %d", code)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("overflow request queued behind the RPC pool")
		}
	}
	if stats := client.stats(); stats.CallsTotal != capacity || stats.PoolAcquiresTotal != capacity || reads.Load() != 0 {
		t.Fatalf("overflow was decoded or queued: rpc=%#v body_reads=%d", stats, reads.Load())
	}
	if stats := admission.stats(); stats.Inflight != capacity || stats.RejectedTotal != rejectedCount {
		t.Fatalf("concurrent rejection accounting: %#v", stats)
	}
	cancel()
	for i := 0; i < capacity; i++ {
		select {
		case code := <-finished:
			if code != http.StatusBadGateway {
				t.Fatalf("canceled RPC returned %d", code)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("cancellation did not release an admitted RPC waiter")
		}
	}
	admissionWait(t, backendDone, "canceled backend connection")
	if admission.stats().Inflight != 0 || client.stats().PoolInUse != 0 {
		t.Fatalf("cancellation leaked capacity: admission=%#v rpc=%#v", admission.stats(), client.stats())
	}
}

type admissionBlockedWriter struct {
	*httptest.ResponseRecorder
	started chan struct{}
	release <-chan struct{}
}

func (w *admissionBlockedWriter) Write(body []byte) (int, error) {
	close(w.started)
	<-w.release
	return w.ResponseRecorder.Write(body)
}

// Prevent io.WriteString from bypassing the blocking Write method through the
// embedded recorder's StringWriter implementation.
func (w *admissionBlockedWriter) WriteString(body string) (int, error) {
	return w.Write([]byte(body))
}

func TestAdmissionRetainsSlotDuringResponseWrite(t *testing.T) {
	admission := newRequestAdmission(1)
	var calls atomic.Int32
	handler := admission.wrap(newKVHandler(admissionCommandFunc(func(context.Context, rpcRequest) (rpcResponse, error) {
		calls.Add(1)
		return rpcResponse{status: statusValue, value: "response"}, nil
	})))
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	response := &admissionBlockedWriter{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{}), release: release}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/kv?key=k", nil))
	}()
	admissionWait(t, response.started, "blocked response write")
	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/kv?key=other", nil))
	if rejected.Code != http.StatusServiceUnavailable || calls.Load() != 1 || admission.stats().Inflight != 1 {
		t.Fatalf("response buffer outlived its admission slot: code=%d calls=%d stats=%#v", rejected.Code, calls.Load(), admission.stats())
	}
	releaseOnce.Do(func() { close(release) })
	admissionWait(t, done, "response write completion")
	if response.Body.String() != "VALUE response\n" || admission.stats().Inflight != 0 {
		t.Fatalf("response did not release admission: body=%q stats=%#v", response.Body.String(), admission.stats())
	}
	next := httptest.NewRecorder()
	handler.ServeHTTP(next, httptest.NewRequest(http.MethodGet, "/kv?key=next", nil))
	if next.Code != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("released slot was not reusable: code=%d calls=%d", next.Code, calls.Load())
	}
}

func TestAdmissionReleasesSlotAfterPanic(t *testing.T) {
	admission := newRequestAdmission(1)
	marker := errors.New("handler panic")
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		admission.wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic(marker)
		})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/kv", nil))
	}()
	if recovered != marker || admission.stats().Inflight != 0 {
		t.Fatalf("panic was swallowed or admission leaked: recovered=%v stats=%#v", recovered, admission.stats())
	}
	response := httptest.NewRecorder()
	admission.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/kv", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("slot was not reusable after panic: %d", response.Code)
	}
}

func TestAdmissionCancellationWaitsForHandlerReturn(t *testing.T) {
	admission := newRequestAdmission(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, canceled, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	handler := admission.wrap(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
		<-release
	}))
	go func() {
		defer close(done)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/kv", nil).WithContext(ctx))
	}()
	admissionWait(t, started, "cancelable handler")
	cancel()
	admissionWait(t, canceled, "handler cancellation")
	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/kv", nil))
	if rejected.Code != http.StatusServiceUnavailable || admission.stats().Inflight != 1 {
		t.Fatalf("cancellation freed a still-running handler: code=%d stats=%#v", rejected.Code, admission.stats())
	}
	releaseOnce.Do(func() { close(release) })
	admissionWait(t, done, "canceled handler return")
	if admission.stats().Inflight != 0 {
		t.Fatalf("canceled handler leaked admission: %#v", admission.stats())
	}
}

func TestGatewayAdmissionKeepsStatsAndDataIndependent(t *testing.T) {
	dataStarted, dataRelease := make(chan struct{}), make(chan struct{})
	statsStarted, statsRelease := make(chan struct{}), make(chan struct{})
	var dataStartOnce, dataReleaseOnce, statsReleaseOnce sync.Once
	defer dataReleaseOnce.Do(func() { close(dataRelease) })
	defer statsReleaseOnce.Do(func() { close(statsRelease) })
	data := newStatsPipeClient(t, 1, func(rpcRequest) (rpcResponse, bool) {
		dataStartOnce.Do(func() { close(dataStarted) })
		<-dataRelease
		return rpcResponse{status: statusOK}, true
	})
	var statsCalls atomic.Int32
	statsClient := admissionCommandFunc(func(context.Context, rpcRequest) (rpcResponse, error) {
		if statsCalls.Add(1) == 2 {
			close(statsStarted)
			<-statsRelease
		}
		return rpcResponse{status: statusValue, value: validStatsPayload}, nil
	})
	handler := newGatewayHandler(data, statsClient, time.Now(), 1)
	dataResponse := httptest.NewRecorder()
	dataDone := make(chan struct{})
	go func() {
		defer close(dataDone)
		handler.ServeHTTP(dataResponse, httptest.NewRequest(http.MethodPost, "/kv", strings.NewReader(`{"key":"blocked"}`)))
	}()
	admissionWait(t, dataStarted, "saturated data route")

	type observedStats struct {
		Error   string `json:"error"`
		Gateway struct {
			HTTP struct {
				Data  admissionStats `json:"data"`
				Stats admissionStats `json:"stats"`
			} `json:"http"`
		} `json:"gateway"`
	}
	decode := func(response *httptest.ResponseRecorder) observedStats {
		t.Helper()
		var result observedStats
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatalf("invalid stats JSON %q: %v", response.Body.String(), err)
		}
		return result
	}
	statsResponse := httptest.NewRecorder()
	handler.ServeHTTP(statsResponse, httptest.NewRequest(http.MethodGet, "/stats", nil))
	result := decode(statsResponse)
	if statsResponse.Code != http.StatusOK || result.Gateway.HTTP.Data.Capacity != 1 || result.Gateway.HTTP.Data.Inflight != 1 ||
		result.Gateway.HTTP.Stats.Capacity != 1 || result.Gateway.HTTP.Stats.Inflight != 1 {
		t.Fatalf("stats did not bypass saturated data admission: code=%d result=%#v", statsResponse.Code, result)
	}
	blockedStats := httptest.NewRecorder()
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		handler.ServeHTTP(blockedStats, httptest.NewRequest(http.MethodGet, "/stats", nil))
	}()
	admissionWait(t, statsStarted, "saturated stats route")
	overloadedStats := httptest.NewRecorder()
	handler.ServeHTTP(overloadedStats, httptest.NewRequest(http.MethodGet, "/stats", nil))
	result = decode(overloadedStats)
	if overloadedStats.Code != http.StatusServiceUnavailable || overloadedStats.Header().Get("Connection") != "close" ||
		result.Error != "gateway_overloaded" || result.Gateway.HTTP.Stats.Inflight != 1 ||
		result.Gateway.HTTP.Stats.RejectedTotal != 1 || statsCalls.Load() != 2 {
		t.Fatalf("stats overflow was queued or lost local metrics: code=%d calls=%d result=%#v", overloadedStats.Code, statsCalls.Load(), result)
	}
	dataReleaseOnce.Do(func() { close(dataRelease) })
	admissionWait(t, dataDone, "initial data request")
	nextData := httptest.NewRecorder()
	handler.ServeHTTP(nextData, httptest.NewRequest(http.MethodPost, "/kv", strings.NewReader(`{"key":"independent"}`)))
	if nextData.Code != http.StatusOK || dataResponse.Code != http.StatusOK {
		t.Fatalf("saturated stats admission blocked data: first=%d next=%d", dataResponse.Code, nextData.Code)
	}
	statsReleaseOnce.Do(func() { close(statsRelease) })
	admissionWait(t, statsDone, "blocked stats completion")
	if blockedStats.Code != http.StatusOK {
		t.Fatalf("admitted stats failed after release: %d", blockedStats.Code)
	}
	finalStats := httptest.NewRecorder()
	handler.ServeHTTP(finalStats, httptest.NewRequest(http.MethodGet, "/stats", nil))
	result = decode(finalStats)
	if finalStats.Code != http.StatusOK || result.Gateway.HTTP.Data.Inflight != 0 || result.Gateway.HTTP.Stats.Inflight != 1 ||
		result.Gateway.HTTP.Stats.RejectedTotal != 1 || statsCalls.Load() != 3 {
		t.Fatalf("route admission did not recover after saturation: code=%d calls=%d result=%#v", finalStats.Code, statsCalls.Load(), result)
	}
}
