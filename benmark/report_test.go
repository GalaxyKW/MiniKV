package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func decodeOnlyReport(t *testing.T, output []byte) benchmarkReport {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(output))
	var report benchmarkReport
	if err := decoder.Decode(&report); err != nil {
		t.Fatalf("stdout is not a JSON report: %v, output=%q", err, output)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("stdout contains extra content after the report: error=%v extra=%#v", err, trailing)
	}
	if report.SchemaVersion != 1 {
		t.Fatalf("schema version=%d, want 1", report.SchemaVersion)
	}
	return report
}

func TestRunJSONSeparatesProgressAndRecordsActualExperiment(t *testing.T) {
	var posts, gets, deletes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv" {
			t.Errorf("request reached unexpected path %q", r.URL.Path)
		}
		switch r.Method {
		case http.MethodPost:
			posts.Add(1)
			fmt.Fprint(w, "OK\n")
		case http.MethodGet:
			gets.Add(1)
			fmt.Fprint(w, "VALUE test\n")
		case http.MethodDelete:
			deletes.Add(1)
			fmt.Fprint(w, "OK\n")
		default:
			t.Errorf("unexpected HTTP method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	args := []string{
		"-url", server.URL + "/kv", "-format", "json", "-workers", "3", "-requests", "17",
		"-op", "MIXED", "-keyspace", "5", "-timeout", "750ms", "-write-ratio", "35",
		"-delete-ratio", "15", "-preload=true", "-preload-count", "2", "-seed", "-19", "-value-size", "8",
	}
	before := time.Now()
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code=%d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	after := time.Now()
	report := decodeOnlyReport(t, stdout.Bytes())
	if !strings.Contains(stderr.String(), "预热 2 个 key") || !strings.Contains(stderr.String(), "预热完成") {
		t.Fatalf("JSON progress was not sent to stderr: %q", stderr.String())
	}
	wantConfig := reportConfig{
		URL: server.URL + "/kv", Workers: 3, Requests: 17, Operation: "mixed", Keyspace: 5,
		TimeoutNS: int64(750 * time.Millisecond), WriteRatio: 35, DeleteRatio: 15,
		Preload: true, PreloadCount: 2, Seed: -19, ValueSize: 8,
	}
	if report.Config != wantConfig {
		t.Errorf("reported config=%#v, want %#v", report.Config, wantConfig)
	}
	if !report.Complete || report.Error != "" || report.LoadModel != "closed_loop" || report.WorkloadGenerator == "" {
		t.Errorf("experiment identity or completion missing: %#v", report)
	}
	if report.ClientBuild.GoVersion != runtime.Version() || report.ClientBuild.GOOS != runtime.GOOS || report.ClientBuild.GOARCH != runtime.GOARCH {
		t.Errorf("client build does not describe the running client: %#v", report.ClientBuild)
	}
	started, err := time.Parse(time.RFC3339Nano, report.StartedAt)
	if err != nil || started.Before(before) || started.After(after) {
		t.Errorf("invalid experiment start %q: %v", report.StartedAt, err)
	}
	measured, err := time.Parse(time.RFC3339Nano, report.MeasurementStartedAt)
	if err != nil || measured.Before(started) || measured.After(after) {
		t.Errorf("invalid measurement start %q: %v", report.MeasurementStartedAt, err)
	}
	if report.Preload.TargetKeys != 2 || report.Preload.CompletedKeys != 2 || report.Preload.ElapsedNS <= 0 {
		t.Errorf("preload progress was not recorded: %#v", report.Preload)
	}
	wantOperations := map[string]int64{"put": posts.Load() - 2, "get": gets.Load(), "delete": deletes.Load()}
	if !reflect.DeepEqual(report.Operations, wantOperations) {
		t.Errorf("operations=%v, observed measurement operations=%v", report.Operations, wantOperations)
	}
	if posts.Load()+gets.Load()+deletes.Load() != 19 || report.Outcomes != (outcomeReport{Requests: 17, Successes: 17}) {
		t.Errorf("preload leaked into measured outcomes: requests=%d outcomes=%#v", posts.Load()+gets.Load()+deletes.Load(), report.Outcomes)
	}
	if !reflect.DeepEqual(report.HTTPStatus, map[int]int64{200: 17}) || report.LatencyNS.Samples != 17 || report.LatencyNS.Min <= 0 ||
		report.ElapsedNS <= 0 || report.QPSTotal <= 0 || report.QPSSuccessful != report.QPSTotal || report.SystemSuccessRatePct != 100 {
		t.Errorf("incomplete measurements: %#v", report)
	}
}

func TestRunReportPreservesFailureCausesAndReceivedHTTPStatus(t *testing.T) {
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch received.Add(1) {
		case 1:
			fmt.Fprint(w, "VALUE test\n")
		case 2:
			http.Error(w, "NOT_FOUND", http.StatusNotFound)
		case 3:
			http.Error(w, "busy", http.StatusServiceUnavailable)
		case 4:
			fmt.Fprint(w, "malformed response\n")
		case 5:
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusTeapot)
			fmt.Fprint(w, "truncated")
		case 6:
			// Publish the status, then let the client's body-read timeout expire.
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
				t.Error("timed out client did not cancel its request")
			}
		default:
			t.Error("client retried an unexpected request")
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-url", server.URL, "-format", "json", "-op", "get", "-workers", "1", "-requests", "6", "-preload=false", "-timeout", "500ms"}, &stdout, &stderr); code != 1 {
		t.Fatalf("measurement with failures exited %d; stderr=%q", code, stderr.String())
	}
	report := decodeOnlyReport(t, stdout.Bytes())
	want := outcomeReport{Requests: 6, Successes: 1, LogicalMisses: 1, Failures: 4,
		NetworkErrors: 2, Timeouts: 1, TransportErrors: 1, HTTPFailures: 1, ProtocolFailures: 1}
	if report.Outcomes != want || !report.Complete || report.Error != "" {
		t.Errorf("completed experiment outcomes=%#v complete=%v error=%q, want %#v", report.Outcomes, report.Complete, report.Error, want)
	}
	if !reflect.DeepEqual(report.HTTPStatus, map[int]int64{200: 3, 404: 1, 503: 1, 418: 1}) {
		t.Errorf("status was lost after response body failed: %v", report.HTTPStatus)
	}
	if report.LatencyNS.Samples != 6 || report.LatencyNS.Min <= 0 || report.Operations["get"] != 6 || received.Load() != 6 {
		t.Errorf("failed requests were omitted or repeated: report=%#v received=%d", report, received.Load())
	}
	if math.Abs(report.QPSSuccessful*3-report.QPSTotal) > report.QPSTotal*1e-12 || math.Abs(report.SystemSuccessRatePct-100.0/3) > 1e-12 {
		t.Errorf("logical misses were not included in successful throughput: total=%f successful=%f rate=%f", report.QPSTotal, report.QPSSuccessful, report.SystemSuccessRatePct)
	}
}

func TestRunPreloadFailureReportsNoMeasurementAndPartialProgress(t *testing.T) {
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("measurement started after preload failure: method=%s", r.Method)
		}
		if received.Add(1) <= 2 {
			fmt.Fprint(w, "OK\n")
		} else {
			http.Error(w, "private backend error", http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-url", server.URL, "-format", "json", "-op", "get", "-workers", "4", "-requests", "11", "-keyspace", "5", "-preload-count", "5"}, &stdout, &stderr); code != 1 {
		t.Fatalf("preload failure exited %d; stderr=%q", code, stderr.String())
	}
	report := decodeOnlyReport(t, stdout.Bytes())
	if report.Complete || report.Error != "preload_failed" || report.MeasurementStartedAt != "" || report.Outcomes != (outcomeReport{}) || report.LatencyNS != (latencyReport{}) {
		t.Errorf("preload failure was reported as a measured experiment: %#v", report)
	}
	if report.Preload.TargetKeys != 5 || report.Preload.CompletedKeys != 2 || report.Preload.ElapsedNS <= 0 || received.Load() != 3 {
		t.Errorf("partial preload progress incorrect: %#v received=%d", report.Preload, received.Load())
	}
	if report.ElapsedNS != 0 || report.QPSTotal != 0 || report.QPSSuccessful != 0 || len(report.HTTPStatus) != 0 ||
		!reflect.DeepEqual(report.Operations, map[string]int64{"put": 0, "get": 0, "delete": 0}) {
		t.Errorf("preload polluted measurement counters: %#v", report)
	}
	if !strings.Contains(stderr.String(), "private backend error") || strings.Contains(stdout.String(), "private backend error") {
		t.Errorf("diagnostics were not separated from the structured error code: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunHelpAndInvalidArgumentsDoNotStartExperiment(t *testing.T) {
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	for _, test := range []struct {
		args []string
		code int
	}{
		{[]string{"-h"}, 0}, {[]string{"-help"}, 0},
		{[]string{"-workers", "0"}, 2}, {[]string{"-format", "yaml"}, 2}, {[]string{"extra"}, 2},
	} {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := append([]string{"-url", server.URL, "-format", "json"}, test.args...)
			if code := run(args, &stdout, &stderr); code != test.code || stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("exit=%d stdout=%q stderr=%q, want exit=%d and diagnostics only", code, stdout.String(), stderr.String(), test.code)
			}
		})
	}
	if received.Load() != 0 {
		t.Fatalf("help or invalid parameters issued %d HTTP requests", received.Load())
	}
}

type reportErrorWriter struct{ err error }

func (w reportErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestReportWriterErrorsReachCallerAndExitStatus(t *testing.T) {
	failure := errors.New("test output failure")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "VALUE test\n")
	}))
	defer server.Close()
	for _, format := range []string{"json", "text"} {
		t.Run(format, func(t *testing.T) {
			if err := writeReport(reportErrorWriter{failure}, benchConfig{format: format}, benchResult{}, nil); !errors.Is(err, failure) {
				t.Errorf("writer failure was lost: %v", err)
			}
			var stderr bytes.Buffer
			code := run([]string{"-url", server.URL, "-format", format, "-op", "get", "-requests", "1", "-workers", "1", "-preload=false"}, reportErrorWriter{failure}, &stderr)
			if code != 1 || !strings.Contains(stderr.String(), failure.Error()) || !strings.Contains(stderr.String(), "Cannot write benchmark report") {
				t.Errorf("writer failure exit=%d stderr=%q", code, stderr.String())
			}
		})
	}
}

func TestReportLatencyUsesExactNearestRanks(t *testing.T) {
	latencies := make([]time.Duration, 1000)
	for i := range latencies {
		latencies[i] = time.Duration(1000 - i)
	}
	report := makeReport(benchConfig{}, benchResult{latencies: latencies}, nil)
	want := latencyReport{Samples: 1000, Mean: 500, Min: 1, P50: 500, P95: 950, P99: 990, P999: 999, Max: 1000}
	if report.LatencyNS != want {
		t.Errorf("latency=%#v, want exact nearest ranks %#v", report.LatencyNS, want)
	}
	for _, test := range []struct {
		values                 []time.Duration
		numerator, denominator int
		want                   time.Duration
	}{
		{nil, 99, 100, 0}, {nil, 0, 0, 0}, {[]time.Duration{7}, 999, 1000, 7},
		{[]time.Duration{10, 20, 30}, 50, 100, 20}, {[]time.Duration{10, 20, 30}, 95, 100, 30},
		{[]time.Duration{10, 20, 30}, 1, 3, 10}, {[]time.Duration{10, 20, 30}, 2, 3, 20},
		{[]time.Duration{10, 20, 30}, 0, 100, 10}, {[]time.Duration{10, 20, 30}, math.MinInt, 100, 10},
		{[]time.Duration{10, 20, 30}, 100, 100, 30}, {[]time.Duration{10, 20, 30}, math.MaxInt, 100, 30},
		{[]time.Duration{10, 20, 30}, 1, math.MaxInt, 10},
		// The first product exceeds uint64 on 64-bit targets; both products
		// exceed int on 32/64-bit targets, while their ranks remain valid.
		{[]time.Duration{10, 20, 30}, math.MaxInt - 1, math.MaxInt, 30},
		{[]time.Duration{10, 20, 30}, math.MaxInt / 2, math.MaxInt, 20},
	} {
		if got := percentile(test.values, test.numerator, test.denominator); got != test.want {
			t.Errorf("percentile(%v, %d/%d)=%v, want %v", test.values, test.numerator, test.denominator, got, test.want)
		}
	}
}

func TestReportPercentileRejectsNonpositiveDenominators(t *testing.T) {
	for _, denominator := range []int{0, -1, math.MinInt} {
		t.Run(strconv.Itoa(denominator), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("nonempty percentile accepted a nonpositive denominator")
				}
			}()
			percentile([]time.Duration{7}, 0, denominator)
		})
	}
}

func TestReportMeanDoesNotOverflowOrLoseIntegerPrecision(t *testing.T) {
	for _, values := range [][]time.Duration{
		nil, {0}, {1, 2}, {math.MaxInt64}, {math.MaxInt64, math.MaxInt64},
		{math.MaxInt64, math.MaxInt64 - 2, 0}, {math.MaxInt64, 1, 2, 3, math.MaxInt64 / 2},
		{1000000000000000001, 1000000000000000002, 1000000000000000003},
	} {
		var total big.Int
		for _, value := range values {
			total.Add(&total, big.NewInt(int64(value)))
		}
		if len(values) > 0 {
			total.Quo(&total, big.NewInt(int64(len(values))))
		}
		if got := avgDuration(values); int64(got) != total.Int64() {
			t.Errorf("mean(%v)=%d, exact mean=%s", values, got, total.String())
		}
	}
}

func TestRunHTTPWorkloadMultisetIsIndependentOfWorkerCount(t *testing.T) {
	type observedRequest struct{ method, key, value string }
	var reference map[observedRequest]int
	for _, workers := range []int{1, 7, 32} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			var mutex sync.Mutex
			observed := make(map[observedRequest]int)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request := observedRequest{method: r.Method, key: r.URL.Query().Get("key")}
				if r.Method == http.MethodPost {
					var body struct{ Key, Value string }
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("invalid PUT JSON: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					request.key, request.value = body.Key, body.Value
				}
				mutex.Lock()
				observed[request]++
				mutex.Unlock()
				if r.Method == http.MethodGet {
					fmt.Fprint(w, "VALUE test\n")
				} else {
					fmt.Fprint(w, "OK\n")
				}
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			code := run([]string{"-url", server.URL, "-format", "json", "-op", "mixed", "-workers", strconv.Itoa(workers),
				"-requests", "257", "-keyspace", "17", "-seed", "-19", "-write-ratio", "35", "-delete-ratio", "15",
				"-value-size", "16", "-preload=false", "-timeout", "5s"}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("workload exited %d: %s", code, stderr.String())
			}
			report := decodeOnlyReport(t, stdout.Bytes())
			mutex.Lock()
			defer mutex.Unlock()
			actualOperations := map[string]int64{"put": 0, "get": 0, "delete": 0}
			for request, count := range observed {
				operation := map[string]string{http.MethodPost: "put", http.MethodGet: "get", http.MethodDelete: "delete"}[request.method]
				if operation == "" || request.key == "" || (operation == "put" && len(request.value) != 16) || (operation != "put" && request.value != "") {
					t.Errorf("invalid request reached the server: %#v", request)
				}
				actualOperations[operation] += int64(count)
			}
			if !reflect.DeepEqual(report.Operations, actualOperations) || report.Outcomes.Successes != 257 {
				t.Errorf("reported operations differ from requests on the wire: report=%v observed=%v", report.Operations, actualOperations)
			}
			for operation, count := range actualOperations {
				if count == 0 {
					t.Errorf("trace did not exercise %s", operation)
				}
			}
			if reference == nil {
				reference = observed
			} else if !reflect.DeepEqual(observed, reference) {
				t.Fatalf("same seed produced a different HTTP request multiset with %d workers", workers)
			}
		})
	}
}

func TestRunReportsRedirectsAndResponseSizeBoundaries(t *testing.T) {
	for _, name := range []string{"redirect", "maximum_value", "oversized_value"} {
		t.Run(name, func(t *testing.T) {
			var requests, redirected atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path == "/redirected" {
					redirected.Add(1)
					fmt.Fprint(w, "VALUE redirected\n")
					return
				}
				if name == "redirect" {
					http.Redirect(w, r, "/redirected", http.StatusFound)
					return
				}
				size := 1024 * 1024
				if name == "oversized_value" {
					size++
				}
				fmt.Fprint(w, "VALUE "+strings.Repeat("x", size)+"\n")
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			code := run([]string{"-url", server.URL + "/kv", "-format", "json", "-op", "get", "-workers", "1", "-requests", "1", "-preload=false", "-timeout", "5s"}, &stdout, &stderr)
			report := decodeOnlyReport(t, stdout.Bytes())
			want := outcomeReport{Requests: 1}
			status, wantCode := 200, 1
			switch name {
			case "redirect":
				want.Failures, want.HTTPFailures, status = 1, 1, 302
			case "maximum_value":
				want.Successes, wantCode = 1, 0
			case "oversized_value":
				want.Failures, want.ProtocolFailures = 1, 1
			}
			if code != wantCode || report.Outcomes != want || !reflect.DeepEqual(report.HTTPStatus, map[int]int64{status: 1}) || requests.Load() != 1 || redirected.Load() != 0 {
				t.Errorf("response boundary incorrect: code=%d outcomes=%#v statuses=%v requests=%d redirected=%d", code, report.Outcomes, report.HTTPStatus, requests.Load(), redirected.Load())
			}
		})
	}
}
