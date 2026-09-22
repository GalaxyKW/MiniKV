package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests of backend decoding supply local metrics independently of HTTP routing.
// admission_test.go exercises the real gateway admission and stats wiring.
func statsHandlerForTest(client commandClient, data *rpcClient, started time.Time) http.HandlerFunc {
	return newStatsHandler(client, func() gatewayStats {
		return gatewayStats{UptimeSeconds: time.Since(started).Seconds(), RPC: data.stats()}
	})
}

const validStatsPayload = `{"schema_version":1,"engine":{
"wal_mode":"reliable","keys":7,"applied_sequence":12,"durable_sequence":11,
"wal_pending_bytes":128,"wal_inflight_bytes":64,"wal_queued_records":1,"wal_queue_capacity_bytes":1024,
"wal_commits_total":0,"wal_commit_failures_total":0,"wal_commit_duration_ns_total":0,"wal_commit_last_duration_ns":0,
"snapshot_successes_total":0,"snapshot_failures_total":0,"snapshot_in_progress":false,"snapshot_sequence":0,
"snapshot_capture_duration_ns_total":0,"snapshot_write_duration_ns_total":0,"snapshot_compact_duration_ns_total":0,
"io_failed":false,"stopping":false},"server":{
"connections":3,"connection_capacity":8,"request_queue_depth":1,"request_queue_capacity":16,
"workers_active":2,"workers_capacity":4,"requests_rejected_total":0,"connections_rejected_total":0}}`

func TestStatsRequestHasNoKeyOrValue(t *testing.T) {
	frame, err := encodeRequest(rpcRequest{op: opStats})
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte("MKV1"), opStats, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	if !bytes.Equal(frame, want) {
		t.Fatalf("stats request=%x, want %x", frame, want)
	}
	for _, request := range []rpcRequest{
		{op: opStats, key: "key"}, {op: opStats, value: "value"},
		{op: opStats, key: "key", value: "value"},
		{op: opPut}, {op: opGet}, {op: opDelete},
	} {
		if _, err := encodeRequest(request); err == nil {
			t.Errorf("accepted invalid request: %#v", request)
		}
	}
}

func TestStatsHTTPReturnsOnlyDefinedAggregates(t *testing.T) {
	var backend map[string]any
	if err := json.Unmarshal([]byte(validStatsPayload), &backend); err != nil {
		t.Fatal(err)
	}
	backend["secret"] = "SENSITIVE_TOP_LEVEL"
	backend["engine"].(map[string]any)["keys_and_values"] = "SENSITIVE_ENGINE"
	backend["server"].(map[string]any)["client_addresses"] = "SENSITIVE_SERVER"
	backend["gateway"] = map[string]any{"uptime_seconds": 999999, "secret": "SENSITIVE_GATEWAY"}
	payload, err := json.Marshal(backend)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeClient{response: rpcResponse{status: statusValue, value: string(payload)}}
	data := newRPCClient("unused", 3, time.Second)
	defer data.Close()
	if _, err := data.execute(context.Background(), rpcRequest{op: opGet}); err == nil {
		t.Fatal("expected invalid data request to be counted as a call failure")
	}
	response := httptest.NewRecorder()
	statsHandlerForTest(client, data, time.Now().Add(-time.Second)).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if response.Code != http.StatusOK || client.calls != 1 || client.request != (rpcRequest{op: opStats}) {
		t.Fatalf("code=%d calls=%d request=%#v", response.Code, client.calls, client.request)
	}
	if response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Cache-Control") != "no-store" ||
		response.Header().Get("Content-Length") != strconv.Itoa(response.Body.Len()) {
		t.Fatalf("unexpected stats headers: %v", response.Header())
	}
	if strings.Contains(response.Body.String(), "SENSITIVE") {
		t.Fatalf("backend metadata leaked: %s", response.Body.String())
	}
	var result runtimeStats
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.SchemaVersion != 1 || result.Error != "" || result.Engine == nil || result.Server == nil || result.Gateway == nil {
		t.Fatalf("incomplete stats envelope: %#v", result)
	}
	if result.Engine.WalMode != "reliable" || result.Engine.Keys != 7 || result.Engine.AppliedSequence != 12 ||
		result.Engine.DurableSequence != 11 || result.Server.Connections != 3 || result.Server.WorkersCapacity != 4 {
		t.Fatalf("backend aggregates changed: engine=%#v server=%#v", result.Engine, result.Server)
	}
	if result.Gateway.RPC != data.stats() || result.Gateway.RPC.CallsTotal != 1 || result.Gateway.RPC.ErrorsTotal != 1 ||
		result.Gateway.UptimeSeconds < 1 || result.Gateway.UptimeSeconds >= 999999 {
		t.Fatalf("gateway stats were not locally collected: %#v", result.Gateway)
	}
}

func TestStatsHTTPRejectsMissingOrNullBaseMeasurements(t *testing.T) {
	var baseline map[string]json.RawMessage
	if err := json.Unmarshal([]byte(validStatsPayload), &baseline); err != nil {
		t.Fatal(err)
	}
	data := newRPCClient("unused", 2, time.Second)
	defer data.Close()
	// Exercise every original field from the complete wire fixture, including
	// legitimate zero counters and false health flags. Optional counters are
	// intentionally absent here and retain their separate compatibility tests.
	for _, section := range []string{"engine", "server"} {
		var original map[string]json.RawMessage
		if err := json.Unmarshal(baseline[section], &original); err != nil {
			t.Fatal(err)
		}
		for field := range original {
			for _, missing := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/missing=%t", section, field, missing), func(t *testing.T) {
					var backend map[string]map[string]json.RawMessage
					if err := json.Unmarshal([]byte(`{"engine":`+string(baseline["engine"])+`,"server":`+string(baseline["server"])+`}`), &backend); err != nil {
						t.Fatal(err)
					}
					if missing {
						delete(backend[section], field)
					} else {
						backend[section][field] = json.RawMessage(`null`)
					}
					payload, err := json.Marshal(map[string]any{"schema_version": 1, "engine": backend["engine"], "server": backend["server"]})
					if err != nil {
						t.Fatal(err)
					}
					client := &fakeClient{response: rpcResponse{status: statusValue, value: string(payload)}}
					response := httptest.NewRecorder()
					statsHandlerForTest(client, data, time.Now()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/stats", nil))
					var result runtimeStats
					if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
						t.Fatal(err)
					}
					if response.Code != http.StatusBadGateway || result.Error != "invalid_backend_stats" ||
						result.Engine != nil || result.Server != nil || result.Gateway == nil {
						t.Fatalf("missing measurement became valid state: code=%d body=%s", response.Code, response.Body.String())
					}
				})
			}
		}
	}
}

var optionalStatsFields = map[string][]string{
	"engine": {
		"data_bytes", "data_capacity_bytes", "data_rejections_total",
		"wal_capacity_waiters", "wal_capacity_waits_total", "wal_capacity_wait_duration_ns_total",
		"wal_durable_waiters", "wal_durable_waits_total", "wal_durable_wait_duration_ns_total",
		"async_requests_inflight", "async_requests_capacity", "async_callback_failures_total",
		"snapshot_capture_state_lock_acquisitions_total", "snapshot_capture_state_lock_duration_ns_total",
		"snapshot_capture_state_lock_duration_ns_max", "snapshot_file_write_calls_total",
		"snapshot_file_written_bytes_total", "snapshot_file_installed_bytes_total", "snapshot_compact_written_bytes_total",
	},
	"server": {"requests_started_total", "request_queue_wait_duration_ns_total", "requests_inflight", "requests_capacity"},
}

func statsPayloadWithOptionalFields(t *testing.T, fields map[string]map[string]json.RawMessage) string {
	t.Helper()
	var backend map[string]json.RawMessage
	if err := json.Unmarshal([]byte(validStatsPayload), &backend); err != nil {
		t.Fatal(err)
	}
	backend["future_metadata"] = json.RawMessage(`"SENSITIVE_TOP_LEVEL"`)
	for _, section := range []string{"engine", "server"} {
		var values map[string]json.RawMessage
		if err := json.Unmarshal(backend[section], &values); err != nil {
			t.Fatal(err)
		}
		values["wait_request_details"] = json.RawMessage(`{"key":"SENSITIVE_REQUEST_KEY"}`)
		values["async_request_details"] = json.RawMessage(`{"key":"SENSITIVE_ASYNC_KEY"}`)
		values["snapshot_record_details"] = json.RawMessage(`{"key":"SENSITIVE_SNAPSHOT_KEY"}`)
		for field, value := range fields[section] {
			values[field] = value
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		backend[section] = encoded
	}
	payload, err := json.Marshal(backend)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func TestStatsHTTPOptionalFieldsPreserveMissingZeroAndUint64Values(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]map[string]json.RawMessage
	}{
		{name: "older_engine_without_wait_fields"},
		{name: "data_within_capacity", fields: map[string]map[string]json.RawMessage{
			"engine": {"data_bytes": json.RawMessage(`13`), "data_capacity_bytes": json.RawMessage(`64`),
				"data_rejections_total": json.RawMessage(`7`)},
		}},
		{name: "data_at_capacity", fields: map[string]map[string]json.RawMessage{
			"engine": {"data_bytes": json.RawMessage(`64`), "data_capacity_bytes": json.RawMessage(`64`)},
		}},
		{name: "data_unlimited", fields: map[string]map[string]json.RawMessage{
			"engine": {"data_bytes": json.RawMessage(`18446744073709551615`), "data_capacity_bytes": json.RawMessage(`0`)},
		}},
		{name: "data_without_capacity", fields: map[string]map[string]json.RawMessage{
			"engine": {"data_bytes": json.RawMessage(`18446744073709551615`)},
		}},
		{name: "capacity_without_data", fields: map[string]map[string]json.RawMessage{
			"engine": {"data_capacity_bytes": json.RawMessage(`1`)},
		}},
		{name: "partial_engine_fields", fields: map[string]map[string]json.RawMessage{
			"engine": {"wal_capacity_waiters": json.RawMessage(`0`), "wal_capacity_waits_total": json.RawMessage(`7`)},
		}},
		{name: "partial_server_fields", fields: map[string]map[string]json.RawMessage{
			"server": {"requests_started_total": json.RawMessage(`0`)},
		}},
		{name: "partial_async_fields", fields: map[string]map[string]json.RawMessage{
			"engine": {"async_requests_inflight": json.RawMessage(`1`)},
			"server": {"requests_capacity": json.RawMessage(`512`)},
		}},
		{name: "partial_snapshot_fields", fields: map[string]map[string]json.RawMessage{
			"engine": {
				"snapshot_capture_state_lock_acquisitions_total": json.RawMessage(`3`),
				"snapshot_file_written_bytes_total":              json.RawMessage(`0`),
			},
		}},
		{name: "all_async_zeros_are_present", fields: map[string]map[string]json.RawMessage{
			"engine": {
				"async_requests_inflight": json.RawMessage(`0`), "async_requests_capacity": json.RawMessage(`0`),
				"async_callback_failures_total": json.RawMessage(`0`),
			},
			"server": {"requests_inflight": json.RawMessage(`0`), "requests_capacity": json.RawMessage(`0`)},
		}},
		{name: "null_is_unavailable", fields: map[string]map[string]json.RawMessage{
			"engine": {
				"wal_durable_waiters": json.RawMessage(`null`), "async_requests_inflight": json.RawMessage(`null`),
				"async_requests_capacity": json.RawMessage(`null`), "async_callback_failures_total": json.RawMessage(`null`),
			},
			"server": {
				"requests_started_total": json.RawMessage(`null`), "requests_inflight": json.RawMessage(`null`),
				"requests_capacity": json.RawMessage(`null`),
			},
		}},
		{name: "complete_current_engine", fields: map[string]map[string]json.RawMessage{
			"engine": {
				"wal_capacity_waiters":                           json.RawMessage(`0`),
				"wal_capacity_waits_total":                       json.RawMessage(`9`),
				"wal_capacity_wait_duration_ns_total":            json.RawMessage(`18446744073709551615`),
				"wal_durable_waiters":                            json.RawMessage(`2`),
				"wal_durable_waits_total":                        json.RawMessage(`9007199254740993`),
				"wal_durable_wait_duration_ns_total":             json.RawMessage(`123456789`),
				"async_requests_inflight":                        json.RawMessage(`9007199254740993`),
				"async_requests_capacity":                        json.RawMessage(`18446744073709551615`),
				"async_callback_failures_total":                  json.RawMessage(`19`),
				"snapshot_capture_state_lock_acquisitions_total": json.RawMessage(`8`),
				"snapshot_capture_state_lock_duration_ns_total":  json.RawMessage(`9007199254740993`),
				"snapshot_capture_state_lock_duration_ns_max":    json.RawMessage(`123456789`),
				"snapshot_file_write_calls_total":                json.RawMessage(`31`),
				"snapshot_file_written_bytes_total":              json.RawMessage(`18446744073709551615`),
				"snapshot_file_installed_bytes_total":            json.RawMessage(`9007199254740993`),
				"snapshot_compact_written_bytes_total":           json.RawMessage(`0`),
			},
			"server": {
				"requests_started_total":               json.RawMessage(`18446744073709551615`),
				"request_queue_wait_duration_ns_total": json.RawMessage(`0`),
				"requests_inflight":                    json.RawMessage(`0`),
				"requests_capacity":                    json.RawMessage(`18446744073709551615`),
			},
		}},
	}
	// Exercise every optional counter without passing through float64, including
	// a real zero, values above JavaScript's exact range, and the full uint64 range.
	for _, value := range []string{`0`, `9007199254740993`, `18446744073709551615`, `null`} {
		fields := make(map[string]map[string]json.RawMessage)
		for section, names := range optionalStatsFields {
			fields[section] = make(map[string]json.RawMessage)
			for _, name := range names {
				fields[section][name] = json.RawMessage(value)
			}
		}
		tests = append(tests, struct {
			name   string
			fields map[string]map[string]json.RawMessage
		}{name: "all_optional_fields_" + value, fields: fields})
	}
	data := newRPCClient("unused", 2, time.Second)
	defer data.Close()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{response: rpcResponse{status: statusValue, value: statsPayloadWithOptionalFields(t, test.fields)}}
			response := httptest.NewRecorder()
			statsHandlerForTest(client, data, time.Now()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/stats", nil))
			if response.Code != http.StatusOK {
				t.Fatalf("code=%d body=%s", response.Code, response.Body.String())
			}
			var result map[string]json.RawMessage
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if string(result["schema_version"]) != "1" || strings.Contains(response.Body.String(), "SENSITIVE") {
				t.Fatalf("version changed or unknown metadata leaked: %s", response.Body.String())
			}
			for section, names := range optionalStatsFields {
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(result[section], &fields); err != nil {
					t.Fatal(err)
				}
				for _, name := range names {
					want, exists := test.fields[section][name]
					got, present := fields[name]
					if !exists || string(want) == "null" {
						if present {
							t.Errorf("unavailable %s.%s became %s", section, name, got)
						}
					} else if !present || !bytes.Equal(got, want) {
						t.Errorf("%s.%s=%s (present=%t), want %s", section, name, got, present, want)
					}
				}
			}
		})
	}
}

func TestStatsHTTPRejectsInvalidOptionalFieldValues(t *testing.T) {
	data := newRPCClient("unused", 1, time.Second)
	defer data.Close()
	for section, names := range optionalStatsFields {
		for _, name := range names {
			for _, invalid := range []string{`-1`, `18446744073709551616`, `1.5`, `true`, `"7"`, `{}`, `[]`} {
				t.Run(section+"/"+name+"/"+invalid, func(t *testing.T) {
					fields := map[string]map[string]json.RawMessage{section: {name: json.RawMessage(invalid)}}
					client := &fakeClient{response: rpcResponse{status: statusValue, value: statsPayloadWithOptionalFields(t, fields)}}
					response := httptest.NewRecorder()
					statsHandlerForTest(client, data, time.Now()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/stats", nil))
					var result runtimeStats
					if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
						t.Fatal(err)
					}
					if response.Code != http.StatusBadGateway || result.Error != "invalid_backend_stats" ||
						result.SchemaVersion != 1 || result.Engine != nil || result.Server != nil || result.Gateway == nil {
						t.Fatalf("invalid optional field was accepted or lost gateway state: code=%d result=%#v", response.Code, result)
					}
					if strings.Contains(response.Body.String(), "SENSITIVE") {
						t.Fatalf("backend metadata leaked through validation error: %s", response.Body.String())
					}
				})
			}
		}
	}
}

func TestStatsHTTPRejectsDataAboveCapacity(t *testing.T) {
	data := newRPCClient("unused", 1, time.Second)
	defer data.Close()
	for _, test := range []struct{ used, capacity string }{
		{`65`, `64`},
		{`18446744073709551615`, `18446744073709551614`},
	} {
		t.Run(test.used+"/"+test.capacity, func(t *testing.T) {
			fields := map[string]map[string]json.RawMessage{
				"engine": {"data_bytes": json.RawMessage(test.used), "data_capacity_bytes": json.RawMessage(test.capacity)},
			}
			client := &fakeClient{response: rpcResponse{status: statusValue, value: statsPayloadWithOptionalFields(t, fields)}}
			response := httptest.NewRecorder()
			statsHandlerForTest(client, data, time.Now()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/stats", nil))
			var result runtimeStats
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if response.Code != http.StatusBadGateway || result.Error != "invalid_backend_stats" ||
				result.SchemaVersion != 1 || result.Engine != nil || result.Server != nil || result.Gateway == nil {
				t.Fatalf("data above capacity was accepted or lost gateway stats: code=%d result=%#v", response.Code, result)
			}
		})
	}
}

func TestStatsHTTPFailuresPreserveGatewayStats(t *testing.T) {
	tests := []struct {
		name     string
		response rpcResponse
		err      error
		code     int
		category string
	}{
		{"deadline", rpcResponse{}, fmt.Errorf("SENSITIVE: %w", context.DeadlineExceeded), 504, "backend_timeout"},
		{"canceled", rpcResponse{}, context.Canceled, 503, "backend_unavailable"},
		{"closed", rpcResponse{}, errClientClosed, 503, "backend_unavailable"},
		{"transport", rpcResponse{}, errors.New("SENSITIVE_ADDRESS"), 503, "backend_unavailable"},
		{"busy", rpcResponse{status: statusBusy, value: "SENSITIVE_BUSY"}, nil, 503, "backend_unavailable"},
		{"io", rpcResponse{status: statusIO, value: "SENSITIVE_PATH"}, nil, 503, "backend_unavailable"},
		{"unsupported", rpcResponse{status: statusBad, value: "SENSITIVE_VERSION"}, nil, 502, "stats_unsupported"},
		{"unexpected_ok", rpcResponse{status: statusOK}, nil, 502, "invalid_backend_stats"},
		{"unexpected_miss", rpcResponse{status: statusMiss}, nil, 502, "invalid_backend_stats"},
		{"unexpected_status", rpcResponse{status: 99}, nil, 502, "invalid_backend_stats"},
		{"invalid_json", rpcResponse{status: statusValue, value: "SENSITIVE_NOT_JSON"}, nil, 502, "invalid_backend_stats"},
		{"missing_state", rpcResponse{status: statusValue, value: `{"schema_version":1}`}, nil, 502, "invalid_backend_stats"},
		{"wrong_version", rpcResponse{status: statusValue, value: strings.Replace(validStatsPayload, `"schema_version":1`, `"schema_version":2`, 1)}, nil, 502, "invalid_backend_stats"},
		{"invalid_sequence", rpcResponse{status: statusValue, value: strings.Replace(validStatsPayload, `"durable_sequence":11`, `"durable_sequence":13`, 1)}, nil, 502, "invalid_backend_stats"},
		{"invalid_capacity", rpcResponse{status: statusValue, value: strings.Replace(validStatsPayload, `"workers_capacity":4`, `"workers_capacity":0`, 1)}, nil, 502, "invalid_backend_stats"},
		{"backend_error", rpcResponse{status: statusValue, value: strings.Replace(validStatsPayload, `"schema_version":1`, `"schema_version":1,"error":"SENSITIVE_ERROR"`, 1)}, nil, 502, "invalid_backend_stats"},
	}
	data := newRPCClient("unused", 2, time.Second)
	defer data.Close()
	_, _ = data.execute(context.Background(), rpcRequest{op: opGet})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{response: test.response, err: test.err}
			response := httptest.NewRecorder()
			statsHandlerForTest(client, data, time.Now()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/stats", nil))
			var result runtimeStats
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if response.Code != test.code || result.Error != test.category || result.SchemaVersion != 1 || client.calls != 1 {
				t.Fatalf("code=%d calls=%d result=%#v", response.Code, client.calls, result)
			}
			if result.Engine != nil || result.Server != nil || result.Gateway == nil || result.Gateway.RPC != data.stats() {
				t.Fatalf("failure lost gateway stats or exposed backend state: %#v", result)
			}
			if response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Cache-Control") != "no-store" ||
				strings.Contains(response.Body.String(), "SENSITIVE") {
				t.Fatalf("unsafe failure response: %v %s", response.Header(), response.Body.String())
			}
		})
	}
}

func TestStatsHTTPRejectsNonGETWithoutBackendCall(t *testing.T) {
	data := newRPCClient("unused", 1, time.Second)
	defer data.Close()
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodHead, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			client := &fakeClient{}
			response := httptest.NewRecorder()
			statsHandlerForTest(client, data, time.Now()).ServeHTTP(response, httptest.NewRequest(method, "/stats", nil))
			if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet || client.calls != 0 {
				t.Fatalf("code=%d Allow=%q calls=%d", response.Code, response.Header().Get("Allow"), client.calls)
			}
		})
	}
}

// Pipe servers keep connections alive, exercising real RPC framing and reuse
// without listening on a port. A false response flag models a dropped exchange.
func newStatsPipeClient(t *testing.T, capacity int, respond func(rpcRequest) (rpcResponse, bool)) *rpcClient {
	t.Helper()
	client := newRPCClient("unused", capacity, 5*time.Second)
	var servers sync.WaitGroup
	client.dial = func(context.Context) (net.Conn, error) {
		local, remote := net.Pipe()
		servers.Add(1)
		go func() {
			defer servers.Done()
			defer remote.Close()
			for {
				request, err := readTestRequest(remote)
				if err != nil {
					return
				}
				response, ok := respond(request)
				if !ok || writeAll(remote, responseFrame(response.status, response.value)) != nil {
					return
				}
			}
		}()
		return local, nil
	}
	t.Cleanup(func() {
		client.Close()
		servers.Wait()
	})
	return client
}

func TestStatsUsesIndependentPoolWhileDataRPCIsBlocked(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	data := newStatsPipeClient(t, 1, func(rpcRequest) (rpcResponse, bool) {
		close(started)
		<-release
		return rpcResponse{status: statusOK}, true
	})
	done := make(chan error, 1)
	go func() {
		_, err := data.execute(context.Background(), rpcRequest{op: opPut, key: "key"})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("data RPC did not start")
	}
	statsClient := newStatsPipeClient(t, 1, func(request rpcRequest) (rpcResponse, bool) {
		if request != (rpcRequest{op: opStats}) {
			t.Errorf("unexpected stats request: %#v", request)
		}
		return rpcResponse{status: statusValue, value: validStatsPayload}, true
	})
	response := httptest.NewRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	statsHandlerForTest(statsClient, data, time.Now()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/stats", nil).WithContext(ctx))
	var result runtimeStats
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || result.Gateway == nil || result.Gateway.RPC.PoolInUse != 1 ||
		result.Gateway.RPC.CallsTotal != 1 || result.Gateway.RPC.ExchangesTotal != 1 || statsClient.stats().CallsTotal != 1 {
		t.Fatalf("stats could not bypass saturated data pool: code=%d result=%#v", response.Code, result)
	}
	select {
	case err := <-done:
		t.Fatalf("data RPC completed before release: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRPCStatsCountCallsAttemptsAndTransportFailures(t *testing.T) {
	tests := []struct {
		name                                         string
		op                                           byte
		failures                                     uint64
		dialFailure                                  bool
		status                                       byte
		acquires, exchanges, exchangeErrors, retries uint64
		errors                                       uint64
	}{
		{"get_recovers", opGet, 1, false, statusMiss, 2, 2, 1, 1, 0},
		{"stats_recovers", opStats, 1, false, statusValue, 2, 2, 1, 1, 0},
		{"get_exhausts_retry", opGet, 2, false, statusMiss, 2, 2, 2, 1, 1},
		{"put_is_not_retried", opPut, 1, false, statusOK, 1, 1, 1, 0, 1},
		{"delete_is_not_retried", opDelete, 1, false, statusOK, 1, 1, 1, 0, 1},
		{"dial_failure_is_not_exchange", opGet, 1, true, statusMiss, 2, 1, 0, 1, 0},
		{"backend_io_status_is_transport_success", opPut, 0, false, statusIO, 1, 1, 0, 0, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var received atomic.Uint64
			client := newStatsPipeClient(t, 1, func(rpcRequest) (rpcResponse, bool) {
				if received.Add(1) <= test.failures && !test.dialFailure {
					return rpcResponse{}, false
				}
				value := ""
				if test.op == opStats {
					value = validStatsPayload
				}
				return rpcResponse{status: test.status, value: value}, true
			})
			if test.dialFailure {
				dial := client.dial
				var attempts atomic.Uint64
				client.dial = func(ctx context.Context) (net.Conn, error) {
					if attempts.Add(1) <= test.failures {
						return nil, errors.New("dial failed")
					}
					return dial(ctx)
				}
			}
			request := rpcRequest{op: test.op, key: "key"}
			if test.op == opStats {
				request.key = ""
			}
			response, err := client.execute(context.Background(), request)
			if (err != nil) != (test.errors != 0) || (err == nil && response.status != test.status) {
				t.Fatalf("response=%#v error=%v", response, err)
			}
			stats := client.stats()
			if stats.CallsTotal != 1 || stats.ErrorsTotal != test.errors || stats.PoolAcquiresTotal != test.acquires ||
				stats.ExchangesTotal != test.exchanges || stats.ExchangeErrorsTotal != test.exchangeErrors || stats.RetriesTotal != test.retries {
				t.Fatalf("unexpected counters: %#v", stats)
			}
			if stats.PoolInUse != 0 || stats.PoolWaitDurationNS == 0 || stats.ExchangeDurationNS == 0 {
				t.Fatalf("slot leaked or attempt duration was omitted: %#v", stats)
			}
		})
	}
}

func TestRPCStatsSeparatePoolTimeoutFromCanceledExchange(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	client := newStatsPipeClient(t, 1, func(rpcRequest) (rpcResponse, bool) {
		close(started)
		<-release
		return rpcResponse{}, false
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.execute(ctx, rpcRequest{op: opGet, key: "blocked"})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first exchange did not start")
	}
	waitCtx, stopWaiting := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stopWaiting()
	_, err := client.execute(waitCtx, rpcRequest{op: opGet, key: "waiting"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool waiter returned %v", err)
	}
	waitStats := client.stats()
	if waitStats.CallsTotal != 2 || waitStats.ErrorsTotal != 1 || waitStats.PoolAcquiresTotal != 2 ||
		waitStats.ExchangesTotal != 1 || waitStats.ExchangeErrorsTotal != 0 || waitStats.RetriesTotal != 0 ||
		waitStats.PoolInUse != 1 || waitStats.PoolWaitDurationNS == 0 {
		t.Fatalf("pool wait was counted as an exchange: %#v", waitStats)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active exchange returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled exchange did not finish")
	}
	stats := client.stats()
	if stats.ErrorsTotal != 2 || stats.ExchangesTotal != 1 || stats.ExchangeErrorsTotal != 1 || stats.RetriesTotal != 0 ||
		stats.PoolInUse != 0 || stats.Connections != 0 || stats.ExchangeDurationNS == 0 {
		t.Fatalf("canceled exchange counters or resources are incorrect: %#v", stats)
	}
}

func TestRPCStatsPreflightFailuresDoNotStartExchanges(t *testing.T) {
	for _, failure := range []string{"invalid", "canceled", "closed"} {
		t.Run(failure, func(t *testing.T) {
			client := newRPCClient("unused", 1, time.Second)
			defer client.Close()
			client.dial = func(context.Context) (net.Conn, error) {
				t.Error("preflight failure reached dial")
				return nil, errors.New("unexpected dial")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := rpcRequest{op: opGet, key: "key"}
			wantAcquires := uint64(1)
			switch failure {
			case "invalid":
				request.key = ""
				wantAcquires = 0
			case "canceled":
				cancel()
			case "closed":
				client.Close()
			}
			if _, err := client.execute(ctx, request); err == nil {
				t.Fatal("expected failed execute")
			}
			stats := client.stats()
			if stats.CallsTotal != 1 || stats.ErrorsTotal != 1 || stats.PoolAcquiresTotal != wantAcquires ||
				stats.ExchangesTotal != 0 || stats.ExchangeErrorsTotal != 0 || stats.ExchangeDurationNS != 0 ||
				stats.RetriesTotal != 0 || stats.PoolInUse != 0 || stats.Connections != 0 {
				t.Fatalf("preflight failure counters: %#v", stats)
			}
		})
	}
}

func TestRPCStatsCanBeReadDuringConcurrentCallsAndAfterClose(t *testing.T) {
	client := newStatsPipeClient(t, 4, func(rpcRequest) (rpcResponse, bool) {
		return rpcResponse{status: statusOK}, true
	})
	const requests = 64
	var calls sync.WaitGroup
	for i := 0; i < requests; i++ {
		calls.Add(1)
		go func() {
			defer calls.Done()
			if _, err := client.execute(context.Background(), rpcRequest{op: opPut, key: "key"}); err != nil {
				t.Errorf("concurrent RPC: %v", err)
			}
		}()
	}
	done := make(chan struct{})
	go func() { calls.Wait(); close(done) }()
	for running := true; running; {
		stats := client.stats()
		if stats.PoolCapacity != 4 || stats.PoolInUse > 4 || stats.Connections > 4 || stats.IdleConnections > 4 {
			t.Errorf("pool gauge exceeded capacity: %#v", stats)
		}
		select {
		case <-done:
			running = false
		default:
			runtime.Gosched()
		}
	}
	stats := client.stats()
	if stats.CallsTotal != requests || stats.ExchangesTotal != requests || stats.PoolAcquiresTotal != requests ||
		stats.ErrorsTotal != 0 || stats.ExchangeErrorsTotal != 0 || stats.RetriesTotal != 0 || stats.PoolInUse != 0 ||
		stats.Connections == 0 || stats.IdleConnections != stats.Connections {
		t.Fatalf("concurrent calls lost metrics or idle connections: %#v", stats)
	}
	client.Close()
	closed := client.stats()
	if !closed.Closed || closed.PoolInUse != 0 || closed.Connections != 0 || closed.IdleConnections != 0 ||
		closed.CallsTotal != stats.CallsTotal || closed.ExchangesTotal != stats.ExchangesTotal {
		t.Fatalf("closed client retained live gauges or lost counters: %#v", closed)
	}
}
