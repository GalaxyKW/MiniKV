package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestParseConfigPreservesDefaults(t *testing.T) {
	want := benchConfig{
		baseURL: "http://127.0.0.1:8080/kv", workers: 50, requests: 200000,
		op: "mixed", keyspace: 20000, timeout: 2 * time.Second,
		writeRatio: 20, deleteRatio: 5, preload: true, preloadCount: 20000,
		seed: 1, valueSize: 128, format: "text",
	}
	var output bytes.Buffer
	cfg, err := parseConfig(nil, &output)
	if err != nil || cfg != want || output.Len() != 0 {
		t.Fatalf("config=%#v error=%v output=%q, want %#v", cfg, err, output.String(), want)
	}
}

func TestParseConfigAcceptsBoundariesWithoutChangingRequestedLoad(t *testing.T) {
	args := []string{
		"-url", "https://[::1]:8443/kv%3Fname%23value", "-workers", "1", "-requests", "1",
		"-keyspace", "1", "-timeout", "1ns", "-op", "GET", "-write-ratio", "0",
		"-delete-ratio", "100", "-value-size", "0", "-preload=false", "-preload-count", "0",
		"-seed", "-9223372036854775808", "-format", "json",
	}
	want := benchConfig{
		baseURL: "https://[::1]:8443/kv%3Fname%23value", workers: 1, requests: 1,
		op: "get", keyspace: 1, timeout: time.Nanosecond, writeRatio: 0, deleteRatio: 100,
		preload: false, preloadCount: 0, seed: -9223372036854775808, valueSize: 0, format: "json",
	}
	cfg, err := parseConfig(args, io.Discard)
	if err != nil || cfg != want {
		t.Fatalf("config=%#v error=%v, want %#v", cfg, err, want)
	}
	cfg, err = parseConfig([]string{"-keyspace", "3", "-preload-count", "10", "-write-ratio", "100", "-delete-ratio", "0", "-value-size", "1048576"}, io.Discard)
	if err != nil || cfg.keyspace != 3 || cfg.preloadCount != 10 || cfg.valueSize != 1048576 || cfg.writeRatio != 100 || cfg.deleteRatio != 0 {
		t.Fatalf("valid upper boundaries were rejected or changed: %#v, %v", cfg, err)
	}
	for _, operation := range []string{"put", "get", "delete", "mixed"} {
		if cfg, err := parseConfig([]string{"-op", operation}, io.Discard); err != nil || cfg.op != operation {
			t.Errorf("operation %q: config=%#v error=%v", operation, cfg, err)
		}
	}
}

func TestParseConfigRejectsInvalidParameters(t *testing.T) {
	tests := [][]string{
		{"-workers", "0"}, {"-workers", "-1"}, {"-requests", "0"}, {"-requests", "-1"},
		{"-keyspace", "0"}, {"-keyspace", "-1"}, {"-timeout", "0"}, {"-timeout", "-1s"},
		{"-value-size", "-1"}, {"-value-size", "1048577"}, {"-op", "scan"}, {"-op", " get "},
		{"-write-ratio", "-1"}, {"-write-ratio", "101"}, {"-delete-ratio", "-1"}, {"-delete-ratio", "101"},
		{"-write-ratio", "80", "-delete-ratio", "21"}, {"-preload-count", "-1"},
		{"-format", "yaml"}, {"-format", "JSON"}, {"-requests", "abc"}, {"-timeout", "soon"},
		{"-preload=maybe"}, {"-unknown"}, {"-workers"}, {"extra"}, {"--", "extra"},
		{"-preload", "false"}, {"-workers", "2", "extra", "-requests", "3"},
		{"-rate", "-1"}, {"-rate", "1000000001"}, {"-rate", "0.5"}, {"-rate", "fast"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			cfg, err := parseConfig(args, io.Discard)
			if err == nil || errors.Is(err, flag.ErrHelp) || cfg != (benchConfig{}) {
				t.Fatalf("invalid arguments produced config=%#v error=%v", cfg, err)
			}
		})
	}
}

func TestParseConfigRejectsUnsafeOrMalformedURLs(t *testing.T) {
	for _, target := range []string{
		"", "/kv", "example.com/kv", "http:example.com/kv", "ftp://example.com/kv",
		"http://", "http://:8080/kv", "http://user:password@example.com/kv", "http://@example.com/kv",
		"http://example.com/kv?key=wrong", "http://example.com/kv?", "http://example.com/kv#section", "http://example.com/kv#",
		"http://example.com/%zz", "http://bad host/kv", "http://[::1/kv", "http://::1/kv", "http://::/kv",
		"http://[not-an-ip]/kv", "http://[:::]/kv", "http://[127.0.0.1]/kv",
		"http://example.com:abc/kv", "http://example.com:-1/kv", "http://example.com:0/kv", "http://example.com:65536/kv",
	} {
		t.Run(target, func(t *testing.T) {
			if cfg, err := parseConfig([]string{"-url", target}, io.Discard); err == nil || cfg != (benchConfig{}) {
				t.Fatalf("invalid URL produced config=%#v error=%v", cfg, err)
			}
		})
	}
	for _, target := range []string{"http://localhost", "https://example.com/kv", "http://[::1]/kv", "http://[fe80::1%25eth0]/kv", "http://example.com:65535/kv", "http://example.com/kv%3F%23"} {
		if cfg, err := parseConfig([]string{"-url", target}, io.Discard); err != nil || cfg.baseURL != target {
			t.Errorf("valid URL %q changed or rejected: config=%#v error=%v", target, cfg, err)
		}
	}
}

func TestParseConfigRejectsIntegerOverflowWithoutArbitraryLimits(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, test := range []struct {
		flag  string
		limit int
	}{
		{"-workers", maxInt / 4},
		{"-requests", maxInt / 8},
	} {
		t.Run(test.flag, func(t *testing.T) {
			for _, tooLarge := range []int{test.limit + 1, maxInt} {
				if _, err := parseConfig([]string{test.flag, strconv.Itoa(tooLarge)}, io.Discard); err == nil {
					t.Errorf("accepted overflowing value %d", tooLarge)
				}
			}
			if _, err := parseConfig([]string{test.flag, strconv.Itoa(test.limit)}, io.Discard); err != nil {
				t.Errorf("rejected largest non-overflowing value %d: %v", test.limit, err)
			}
		})
	}
}

func TestParseConfigBoundsActualWorkerResultAllocation(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	limit := maxInt / int(unsafe.Sizeof(benchResult{}))
	for _, test := range []struct {
		workers, requests int
		valid             bool
	}{
		{limit, limit, true}, {limit + 1, limit + 1, false},
		{limit + 1, limit, true}, {limit + 1, 1, true},
		{limit, limit + 1, true}, {maxInt / 4, 1, true},
	} {
		args := []string{"-workers", strconv.Itoa(test.workers), "-requests", strconv.Itoa(test.requests)}
		cfg, err := parseConfig(args, io.Discard)
		if test.valid {
			if err != nil || cfg.workers != test.workers || cfg.requests != test.requests {
				t.Errorf("valid arguments %v changed or rejected: config=%#v error=%v", args, cfg, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), "结果切片大小会溢出") || cfg != (benchConfig{}) {
			t.Errorf("overflowing result allocation accepted: arguments=%v config=%#v error=%v", args, cfg, err)
		}
	}
}

func TestParseConfigHelpDoesNotReturnAnExperiment(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"-help"}, {"-workers", "7", "-h"}} {
		var output bytes.Buffer
		cfg, err := parseConfig(args, &output)
		if !errors.Is(err, flag.ErrHelp) || cfg != (benchConfig{}) {
			t.Fatalf("help produced runnable config=%#v error=%v", cfg, err)
		}
		if !strings.Contains(output.String(), "-workers") || !strings.Contains(output.String(), "-format") ||
			strings.Contains(output.String(), "压测报告") {
			t.Fatalf("unexpected help output: %q", output.String())
		}
	}
}

func TestParseConfigCallsAreIndependent(t *testing.T) {
	first, err := parseConfig([]string{"-workers", "2", "-format", "json", "-preload=false"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	second, err := parseConfig(nil, io.Discard)
	if err != nil || first.workers != 2 || first.format != "json" || first.preload ||
		second.workers != 50 || second.format != "text" || !second.preload {
		t.Fatalf("parsing retained state: first=%#v second=%#v error=%v", first, second, err)
	}
	var calls sync.WaitGroup
	for i := 0; i < 8; i++ {
		calls.Add(1)
		go func() {
			defer calls.Done()
			if cfg, err := parseConfig([]string{"-requests", "3"}, io.Discard); err != nil || cfg.requests != 3 || cfg.workers != 50 {
				t.Errorf("concurrent parse config=%#v error=%v", cfg, err)
			}
		}()
	}
	calls.Wait()
}

func TestParseConfigDoesNotMakeRequests(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	for _, invalid := range [][]string{{"-workers", "0"}, {"-timeout", "0"}, {"-preload-count", "-1"}, {"-format", "bad"}, {"-rate", "-1"}, {"-rate", "1000000001"}, {"extra"}} {
		args := append([]string{"-url", server.URL + "/kv"}, invalid...)
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Errorf("expected invalid config: %v", args)
		}
	}
	if _, err := parseConfig([]string{"-url", server.URL + "/kv"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("configuration parsing sent %d HTTP requests", got)
	}
}

func TestParseConfigFixedArrivalRateAndSampleBoundaries(t *testing.T) {
	for _, rate := range []int{0, 1, 3, 1000000000} {
		cfg, err := parseConfig([]string{"-rate", strconv.Itoa(rate)}, io.Discard)
		if err != nil || cfg.rate != rate || cfg.requests != 200000 {
			t.Errorf("rate=%d produced config=%#v error=%v", rate, cfg, err)
		}
	}
	maxInt := int(^uint(0) >> 1)
	for _, requests := range []int{maxInt / 24, maxInt/24 + 1} {
		cfg, err := parseConfig([]string{"-rate", "1000000000", "-requests", strconv.Itoa(requests)}, io.Discard)
		if requests == maxInt/24 {
			if err != nil || cfg.requests != requests {
				t.Errorf("largest fixed-arrival sample capacity rejected: config=%#v error=%v", cfg, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), "延迟切片大小会溢出") || cfg != (benchConfig{}) {
			t.Errorf("overflowing fixed-arrival capacity accepted: config=%#v error=%v", cfg, err)
		}
	}
}

func TestParseConfigRejectsArrivalDurationOverflow(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("32-bit request counts cannot overflow a duration at supported rates")
	}
	const maxDuration = int64(1<<63 - 1)
	seconds := maxDuration / int64(time.Second)
	for _, test := range []struct {
		requests int64
		rate     int
		valid    bool
	}{
		{seconds, 1, true}, {seconds + 1, 1, false},
		// The final 6/7 second exceeds the duration's remaining fraction;
		// checking only whole seconds would silently accept this overflow.
		{seconds*7 + 5, 7, true}, {seconds*7 + 6, 7, false},
	} {
		cfg, err := parseConfig([]string{"-rate", strconv.Itoa(test.rate), "-requests", strconv.FormatInt(test.requests, 10)}, io.Discard)
		if test.valid {
			if err != nil || cfg.requests != int(test.requests) {
				t.Errorf("valid arrival horizon rejected: %#v: config=%#v error=%v", test, cfg, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), "计划时长") || cfg != (benchConfig{}) {
			t.Errorf("overflowing arrival horizon accepted: %#v: config=%#v error=%v", test, cfg, err)
		}
	}
}

func TestArrivalScheduleValidationMatchesExactArithmetic(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, requests := range []int{0, -1, 1, 2, 7, maxInt / 24, maxInt / 8, maxInt} {
		for _, rate := range []int{-1, 0, 1, 3, 7, 999999999, 1000000000, 1000000001} {
			want := requests > 0 && rate > 0 && rate <= 1000000000
			if want {
				var horizon big.Int
				horizon.Mul(big.NewInt(int64(requests)), big.NewInt(int64(time.Second)))
				horizon.Quo(&horizon, big.NewInt(int64(rate)))
				want = horizon.IsInt64()
			}
			if got := validArrivalSchedule(requests, rate); got != want {
				t.Errorf("validArrivalSchedule(%d, %d)=%v, exact arithmetic says %v", requests, rate, got, want)
			}
		}
	}
}
