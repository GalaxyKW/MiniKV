package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type fakeArrivalClock struct {
	now   time.Time
	jumps []time.Duration
	waits int
}

func (clock *fakeArrivalClock) Now() time.Time { return clock.now }
func (clock *fakeArrivalClock) WaitUntil(deadline time.Time) {
	if deadline.After(clock.now) {
		clock.now = deadline
	}
	if clock.waits < len(clock.jumps) {
		clock.now = clock.now.Add(clock.jumps[clock.waits])
	}
	clock.waits++
}

func TestArrivalDeadlineBoundaries(t *testing.T) {
	for _, rate := range []int{1, 3, 7, 1001, 999999937, 1000000000} {
		for index := 1; index < 10000; index++ {
			deadline := arrivalOffset(index, rate)
			if got := latestArrival(deadline-time.Nanosecond, rate); got != index-1 {
				t.Fatalf("rate=%d before slot %d got %d", rate, index, got)
			}
			if got := latestArrival(deadline, rate); got != index {
				t.Fatalf("rate=%d at slot %d got %d", rate, index, got)
			}
			if after := deadline + time.Nanosecond; after < arrivalOffset(index+1, rate) {
				if got := latestArrival(after, rate); got != index {
					t.Fatalf("rate=%d after slot %d got %d", rate, index, got)
				}
			}
		}
	}
}

func TestArrivalScheduleSkipsExpiredSlots(t *testing.T) {
	tests := []struct {
		name    string
		jump    time.Duration
		indices []int
		late    int
	}{
		{"on_time", 0, []int{0, 1, 2}, 0},
		{"just_before_boundary", 333333332, []int{0, 1, 2}, 0},
		{"exact_boundary", 333333333, []int{1, 2}, 1},
		{"multiple_slots", 800 * time.Millisecond, []int{2}, 2},
		{"last_nanosecond", time.Second - 1, []int{2}, 2},
		{"whole_window", time.Second, nil, 3},
		{"beyond_window", 2 * time.Second, nil, 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start := time.Now()
			clock := &fakeArrivalClock{now: start, jumps: []time.Duration{test.jump}}
			var indices []int
			counts := scheduleArrivals(3, 3, start, clock, func(index int, scheduled time.Time) bool {
				indices = append(indices, index)
				if want := start.Add(arrivalOffset(index, 3)); !scheduled.Equal(want) {
					t.Fatalf("slot %d deadline changed: %s != %s", index, scheduled, want)
				}
				return index != 1 // A live slot without capacity is busy, not late.
			})
			if !reflect.DeepEqual(indices, test.indices) || counts.late != test.late ||
				counts.started+counts.busy+counts.late != 3 {
				t.Fatalf("indices=%v counts=%+v", indices, counts)
			}
			if clock.now.Before(start.Add(time.Second)) {
				t.Fatal("schedule ended before the full offered window")
			}
		})
	}
}

func TestArrivalSchedulerDoesNotReplayAfterDispatchStall(t *testing.T) {
	start := time.Now()
	clock := &fakeArrivalClock{now: start}
	var indices []int
	counts := scheduleArrivals(10, 10, start, clock, func(index int, scheduled time.Time) bool {
		indices = append(indices, index)
		clock.now = clock.now.Add(350 * time.Millisecond)
		return true
	})
	if !reflect.DeepEqual(indices, []int{0, 3, 7}) || counts != (arrivalCounts{started: 3, late: 7}) {
		t.Fatalf("scheduler replayed stale arrivals: %v %+v", indices, counts)
	}
}

func TestFixedArrivalsBoundOutstandingWorkAndDrain(t *testing.T) {
	// Hold all workers beyond the offered window. A closed-loop implementation
	// would send more requests after release instead of reporting dropped demand.
	entered := make(chan struct{}, 10)
	release := make(chan struct{})
	released := false
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		entered <- struct{}{}
		<-release
		fmt.Fprint(w, "VALUE result\n")
	}))
	defer server.Close()
	defer func() {
		if !released {
			close(release)
		}
	}()
	cfg := benchConfig{baseURL: server.URL, workers: 2, requests: 10, rate: 20,
		op: "get", keyspace: 10, timeout: 5 * time.Second}
	done := make(chan benchResult, 1)
	go func() {
		result, _ := runBenchmark(cfg)
		done <- result
	}()
	for i := 0; i < cfg.workers; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	time.Sleep(600 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("benchmark returned without draining admitted requests")
	default:
	}
	close(release)
	released = true
	var result benchResult
	select {
	case result = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("benchmark did not finish after draining")
	}
	if received.Load() != 2 || result.total != 2 || result.successes != 2 ||
		result.droppedBusy+result.droppedLate != 8 || result.elapsed < 500*time.Millisecond {
		t.Fatalf("unexpected bounded result: received=%d result=%+v", received.Load(), result)
	}
	if len(result.latencies) != 2 || len(result.dispatchDelays) != 2 || len(result.scheduledLatencies) != 2 {
		t.Fatal("dropped slots became latency samples")
	}
	for i, duration := range result.scheduledLatencies {
		if duration != result.dispatchDelays[i]+result.latencies[i] {
			t.Fatalf("sample %d lost schedule delay", i)
		}
	}
}

func TestFixedArrivalJobRetainsOriginalWorkloadIndex(t *testing.T) {
	cfg := benchConfig{workers: 1, requests: 5, rate: 1, op: "mixed", keyspace: 99,
		writeRatio: 100, valueSize: 64, seed: 23, timeout: time.Second}
	want := requestFor(cfg, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var value kvRequest
		if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
			t.Error(err)
		}
		if value.Key != want.key || value.Value != want.value {
			t.Errorf("dropped slots renumbered the workload: %+v", value)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg.baseURL = server.URL
	client := newHTTPClient(cfg)
	defer client.CloseIdleConnections()
	result := benchResult{latencies: make([]time.Duration, 1), dispatchDelays: make([]time.Duration, 1), scheduledLatencies: make([]time.Duration, 1)}
	local := benchResult{operations: make(map[string]int64), statusCount: make(map[int]int64)}
	executeBenchmarkJob(cfg, client, benchmarkJob{requestIndex: 4, sampleIndex: 0,
		scheduledAt: time.Now().Add(-time.Second)}, &result, &local)
	if local.failures != 1 || local.httpFailures != 1 || result.dispatchDelays[0] < time.Second ||
		result.scheduledLatencies[0] != result.dispatchDelays[0]+result.latencies[0] {
		t.Fatalf("failed request lost counters or timing: local=%+v result=%+v", local, result)
	}
}

func TestFixedArrivalCLIExitsNonzeroForDroppedDemand(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(250 * time.Millisecond)
		fmt.Fprint(w, "VALUE result\n")
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := run([]string{"-url", server.URL, "-op", "get", "-requests", "10", "-rate", "100", "-workers", "1",
		"-preload=false", "-format", "json"}, &stdout, &stderr)
	report := decodeOnlyReport(t, stdout.Bytes())
	if code != 1 || !report.Complete || report.Arrivals == nil || report.Outcomes.Failures != 0 ||
		report.Outcomes.Requests > 1 || report.Arrivals.DroppedBusy+report.Arrivals.DroppedLate != 10-report.Outcomes.Requests {
		t.Fatalf("code=%d report=%+v stderr=%s", code, report, stderr.String())
	}
}
