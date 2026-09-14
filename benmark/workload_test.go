package main

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func workloadConfig() benchConfig {
	return benchConfig{workers: 1, op: "mixed", keyspace: 23, writeRatio: 30, deleteRatio: 20, seed: 1, valueSize: 16}
}

func TestWorkloadReferenceRequests(t *testing.T) {
	if workloadVersion != "indexed-pcg-v1" {
		t.Fatal("update the reference requests when changing workload version")
	}
	tests := []struct {
		seed  int64
		index int
		want  workloadRequest
	}{
		{1, 0, workloadRequest{"put", "k13", "v1-0xxxxxxxxxxxx"}},
		{1, 1, workloadRequest{"put", "k22", "v1-1xxxxxxxxxxxx"}},
		{1, 2, workloadRequest{"get", "k17", ""}},
		{1, 4, workloadRequest{"put", "k2", "v1-4xxxxxxxxxxxx"}},
		{1, 10, workloadRequest{"delete", "k12", ""}},
		{1, 17, workloadRequest{"delete", "k6", ""}},
		{-1, 0, workloadRequest{"get", "k5", ""}},
		{math.MinInt64, math.MaxInt32, workloadRequest{"get", "k14", ""}},
		{math.MaxInt64, math.MaxInt32, workloadRequest{"put", "k15", "v922337203685477"}},
	}
	for _, test := range tests {
		cfg := workloadConfig()
		cfg.seed = test.seed
		if got := requestFor(cfg, test.index); got != test.want {
			t.Errorf("seed=%d index=%d: got %#v, want %#v", test.seed, test.index, got, test.want)
		}
	}
	for _, keyspace := range []int{1, 2} {
		cfg := workloadConfig()
		cfg.keyspace = keyspace
		want := workloadRequest{"put", "k" + strconv.Itoa(keyspace-1), "v1-0xxxxxxxxxxxx"}
		if got := requestFor(cfg, 0); got != want {
			t.Errorf("keyspace=%d: got %#v, want %#v", keyspace, got, want)
		}
	}
}

func TestWorkloadRangeReductionRejections(t *testing.T) {
	// These large ranges deterministically reject the first one/two PCG draws.
	// Golden values also apply on 32-bit hosts because all arithmetic is uint64.
	for _, test := range []struct {
		index, n, want uint64
	}{
		{3, 4611686018427387905, 3197655477946921881},
		{0, 9223372036854775809, 6597998630079079801},
	} {
		var source rand.PCG
		source.Seed(1, test.index)
		if got := workloadUint64N(&source, test.n); got != test.want {
			t.Errorf("index=%d range=%d: got %d, want %d", test.index, test.n, got, test.want)
		}
	}
}

func TestWorkloadIndependentOfWorkersAndScheduling(t *testing.T) {
	const count = 1027
	cfg := workloadConfig()
	want := make([]workloadRequest, count)
	for i := range want {
		want[i] = requestFor(cfg, i)
	}
	for _, workers := range []int{1, 2, 7, 32} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			cfg := cfg
			cfg.workers = workers
			got := make([]workloadRequest, count)
			var wg sync.WaitGroup
			for worker := 0; worker < workers; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					// Reverse, interleaved traversal differs from the serial trace.
					for i := count - 1 - worker; i >= 0; i -= workers {
						got[i] = requestFor(cfg, i)
					}
				}(worker)
			}
			wg.Wait()
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("workers=%d index=%d: got %#v, want %#v", workers, i, got[i], want[i])
				}
			}
		})
	}
}

func TestWorkloadSeedAndIndexAffectTrace(t *testing.T) {
	cfg := workloadConfig()
	otherSeed := cfg
	otherSeed.seed++
	seedChanges, indexChanges := 0, 0
	for i := 0; i < 256; i++ {
		request := requestFor(cfg, i)
		if request != requestFor(otherSeed, i) {
			seedChanges++
		}
		if request != requestFor(cfg, i+1) {
			indexChanges++
		}
	}
	if seedChanges < 200 || indexChanges < 200 {
		t.Fatalf("seed/index failed to vary the trace: seed changes=%d index changes=%d", seedChanges, indexChanges)
	}
}

func TestWorkloadMixedRatioBoundaries(t *testing.T) {
	// With seed 1 and index 0, the versioned percentage draw is exactly 8.
	for _, test := range []struct {
		write, delete int
		want          string
	}{
		{0, 0, "get"}, {100, 0, "put"}, {0, 100, "delete"},
		{8, 1, "delete"}, {8, 0, "get"}, {9, 0, "put"},
		{0, 8, "get"}, {0, 9, "delete"},
	} {
		cfg := workloadConfig()
		cfg.writeRatio, cfg.deleteRatio = test.write, test.delete
		if got := requestFor(cfg, 0).operation; got != test.want {
			t.Errorf("write/delete=%d/%d: got %q, want %q", test.write, test.delete, got, test.want)
		}
	}
	for _, op := range []string{"put", "get", "delete"} {
		cfg := workloadConfig()
		cfg.op = op
		for i := 0; i < 128; i++ {
			if got := requestFor(cfg, i).operation; got != op {
				t.Fatalf("fixed %s mode emitted %s", op, got)
			}
		}
	}
}

func TestWorkloadMixAndKeyDistribution(t *testing.T) {
	cfg := workloadConfig()
	cfg.keyspace = 17
	const count = 100000
	operations := make(map[string]int)
	keys := make(map[string]int)
	for i := 0; i < count; i++ {
		request := requestFor(cfg, i)
		operations[request.operation]++
		keys[request.key]++
	}
	for op, expected := range map[string]int{"put": 30000, "delete": 20000, "get": 50000} {
		if got := operations[op]; got < expected-1000 || got > expected+1000 {
			t.Errorf("operation %s count=%d, expected about %d", op, got, expected)
		}
	}
	if len(keys) != cfg.keyspace {
		t.Fatalf("visited %d keys, want %d", len(keys), cfg.keyspace)
	}
	for key, got := range keys {
		if expected := count / cfg.keyspace; got < expected-500 || got > expected+500 {
			t.Errorf("key %s count=%d, expected about %d", key, got, expected)
		}
	}
}

func TestWorkloadRatiosAndValuesDoNotChangeKeys(t *testing.T) {
	cfg := workloadConfig()
	for i := 0; i < 128; i++ {
		want := requestFor(cfg, i).key
		for _, op := range []string{"put", "get", "delete", "mixed"} {
			changed := cfg
			changed.op, changed.writeRatio, changed.deleteRatio, changed.valueSize = op, 60, 40, 0
			if got := requestFor(changed, i).key; got != want {
				t.Fatalf("request %d changed key from %s to %s when changing operation mix", i, want, got)
			}
		}
	}
}

func TestWorkloadKeyAndValueLimits(t *testing.T) {
	for _, keyspace := range []int{1, 2, 4096, math.MaxInt} {
		cfg := workloadConfig()
		cfg.keyspace = keyspace
		for i := 0; i < 128; i++ {
			key := requestFor(cfg, i).key
			if len(key) < 2 || len(key) > 4096 || key[0] != 'k' {
				t.Fatalf("invalid key %q", key)
			}
			value, err := strconv.ParseUint(key[1:], 10, 64)
			if err != nil || value >= uint64(keyspace) {
				t.Fatalf("key %q outside keyspace %d: %v", key, keyspace, err)
			}
		}
	}
	for _, size := range []int{0, 1, 5, 6, 16, 128, 1048576} {
		cfg := workloadConfig()
		cfg.op, cfg.valueSize = "put", size
		request := requestFor(cfg, 123)
		want := "v1-123"
		if size < len(want) {
			want = want[:size]
		} else {
			want += strings.Repeat("x", size-len(want))
		}
		if request.value != want {
			t.Fatalf("value size %d: got length %d or incorrect prefix/padding", size, len(request.value))
		}
		for _, op := range []string{"get", "delete"} {
			cfg.op = op
			if got := requestFor(cfg, 123).value; got != "" {
				t.Fatalf("%s constructed a value of length %d", op, len(got))
			}
		}
	}
	cfg := workloadConfig()
	cfg.op, cfg.seed, cfg.valueSize = "put", math.MinInt64, 128
	prefix := fmt.Sprintf("v%d-%d", cfg.seed, math.MaxInt)
	if value := requestFor(cfg, math.MaxInt).value; len(value) != 128 || !strings.HasPrefix(value, prefix) {
		t.Fatalf("extreme seed/index prefix was truncated: %q", value)
	}
}

var workloadSink workloadRequest

func TestWorkloadAllocations(t *testing.T) {
	for _, test := range []struct {
		op        string
		size      int
		maxAllocs float64
	}{
		{"get", 1048576, 1}, {"delete", 1048576, 1}, {"put", 0, 1}, {"put", 128, 2},
	} {
		cfg := workloadConfig()
		cfg.op, cfg.valueSize = test.op, test.size
		allocs := testing.AllocsPerRun(1000, func() { workloadSink = requestFor(cfg, 123) })
		if allocs > test.maxAllocs {
			t.Errorf("%s size=%d: %.1f allocations, want <= %.1f (key and optional value only)",
				test.op, test.size, allocs, test.maxAllocs)
		}
	}
}

func BenchmarkRequestFor(b *testing.B) {
	for _, test := range []struct {
		op   string
		size int
	}{
		{"get", 128}, {"get", 1048576}, {"delete", 128},
		{"put", 0}, {"put", 128}, {"put", 1048576}, {"mixed", 128},
	} {
		b.Run(fmt.Sprintf("%s_%d", test.op, test.size), func(b *testing.B) {
			cfg := workloadConfig()
			cfg.keyspace, cfg.writeRatio, cfg.deleteRatio = 20000, 20, 5
			cfg.op, cfg.valueSize = test.op, test.size
			b.ReportAllocs()
			if test.op == "put" {
				b.SetBytes(int64(test.size))
			}
			for i := 0; i < b.N; i++ {
				workloadSink = requestFor(cfg, i)
			}
		})
	}
}
