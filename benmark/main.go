package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	urlpkg "net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type benchConfig struct {
	baseURL      string
	workers      int
	requests     int
	op           string
	keyspace     int
	timeout      time.Duration
	writeRatio   int
	deleteRatio  int
	preload      bool
	preloadCount int
	seed         int64
	valueSize    int
	format       string
}

func (cfg benchConfig) workerCount() int {
	// Workers beyond the number of jobs cannot add concurrency.
	return min(cfg.workers, cfg.requests)
}

type benchResult struct {
	latencies            []time.Duration
	successes            int64
	failures             int64
	logicalMisses        int64
	statusCount          map[int]int64
	errors               int64
	total                int
	elapsed              time.Duration
	startedAt            time.Time
	measurementStartedAt time.Time
	preloadElapsed       time.Duration
	preloaded            int
	operations           map[string]int64
	timeouts             int64
	httpFailures         int64
	protocolFailures     int64
}

type kvRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func newHTTPClient(cfg benchConfig) *http.Client {
	workers := cfg.workerCount()
	transport := &http.Transport{
		MaxIdleConns:        workers * 4,
		MaxIdleConnsPerHost: workers * 4,
		IdleConnTimeout:     30 * time.Second,
	}
	return &http.Client{Timeout: cfg.timeout, Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

var errResponseTooLarge = errors.New("response exceeds the KV response size limit")

func doRequest(client *http.Client, req *http.Request) (int, string, error) {
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	const maxResponseBytes = 1024*1024 + len("VALUE \n")
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxResponseBytes+1)))
	if len(body) > maxResponseBytes {
		return resp.StatusCode, "", errResponseTooLarge
	}
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

func doPut(client *http.Client, url, key, value string) (int, string, error) {
	payload, err := json.Marshal(kvRequest{Key: key, Value: value})
	if err != nil {
		return 0, "", err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	return doRequest(client, req)
}

func doGet(client *http.Client, url, key string) (int, string, error) {
	req, err := http.NewRequest(http.MethodGet, url+"?key="+urlpkg.QueryEscape(key), nil)
	if err != nil {
		return 0, "", err
	}
	return doRequest(client, req)
}

func doDelete(client *http.Client, url, key string) (int, string, error) {
	req, err := http.NewRequest(http.MethodDelete, url+"?key="+urlpkg.QueryEscape(key), nil)
	if err != nil {
		return 0, "", err
	}
	return doRequest(client, req)
}

func preloadData(cfg benchConfig, client *http.Client) error {
	_, err := preloadDataWithProgress(cfg, client, io.Discard)
	return err
}

func effectivePreloadCount(cfg benchConfig) int {
	if !cfg.preload || (cfg.op != "get" && cfg.op != "mixed") {
		return 0
	}
	count := cfg.preloadCount
	if count == 0 || count > cfg.keyspace {
		count = cfg.keyspace
	}
	return count
}

func preloadDataWithProgress(cfg benchConfig, client *http.Client, progress io.Writer) (int, error) {
	count := effectivePreloadCount(cfg)
	if count == 0 {
		return 0, nil
	}
	fmt.Fprintf(progress, "正在预热 %d 个 key...\n", count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("k%d", i)
		value := makeValue(fmt.Sprintf("v%d", i), cfg.valueSize)
		status, body, err := doPut(client, cfg.baseURL, key, value)
		if err != nil || status != http.StatusOK || body != "OK\n" {
			if len(body) > 256 {
				body = body[:256] + "..."
			}
			return i, fmt.Errorf("预热 key %s 失败: HTTP %d, body=%q, err=%v", key, status, body, err)
		}
	}
	fmt.Fprintln(progress, "预热完成。")
	return count, nil
}

func makeValue(prefix string, size int) string {
	if len(prefix) >= size {
		return prefix[:size]
	}
	return prefix + strings.Repeat("x", size-len(prefix))
}

func classifyResult(op string, status int, body string) (success, miss bool) {
	if (op == "get" || op == "delete") && status == http.StatusNotFound && body == "NOT_FOUND\n" {
		return false, true
	}
	if status != http.StatusOK {
		return false, false
	}
	if op == "get" {
		return strings.HasPrefix(body, "VALUE ") && strings.HasSuffix(body, "\n"), false
	}
	return (op == "put" || op == "delete") && body == "OK\n", false
}

func runBenchmark(cfg benchConfig) (benchResult, error) {
	return runBenchmarkWithProgress(cfg, io.Discard)
}

func runBenchmarkWithProgress(cfg benchConfig, progress io.Writer) (benchResult, error) {
	result := benchResult{
		statusCount: make(map[int]int64),
		operations:  map[string]int64{"put": 0, "get": 0, "delete": 0},
		startedAt:   time.Now().UTC(),
	}

	workers := cfg.workerCount()
	client := newHTTPClient(cfg)
	defer client.CloseIdleConnections()
	preloadStart := time.Now()
	var err error
	result.preloaded, err = preloadDataWithProgress(cfg, client, progress)
	result.preloadElapsed = time.Since(preloadStart)
	if err != nil {
		return result, err
	}
	result.latencies = make([]time.Duration, cfg.requests)

	jobs := make(chan int)
	workerResults := make([]benchResult, workers)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			local := benchResult{statusCount: make(map[int]int64), operations: make(map[string]int64)}
			for i := range jobs {
				request := requestFor(cfg, i)
				op, key, value := request.operation, request.key, request.value
				local.operations[op]++

				start := time.Now()
				status := 0
				body := ""
				var err error
				switch op {
				case "put":
					status, body, err = doPut(client, cfg.baseURL, key, value)
				case "get":
					status, body, err = doGet(client, cfg.baseURL, key)
				case "delete":
					status, body, err = doDelete(client, cfg.baseURL, key)
				default:
					err = fmt.Errorf("不支持的操作类型: %s", op)
				}
				// Each job owns a distinct slot; the slice is read only after wg.Wait.
				result.latencies[i] = time.Since(start)

				if status != 0 {
					local.statusCount[status]++
				}
				if err != nil {
					if errors.Is(err, errResponseTooLarge) {
						local.protocolFailures++
					} else {
						local.errors++
						var timeout net.Error
						if errors.As(err, &timeout) && timeout.Timeout() {
							local.timeouts++
						}
					}
					local.failures++
					continue
				}

				success, miss := classifyResult(op, status, body)
				if miss {
					local.logicalMisses++
					continue
				}

				if success {
					local.successes++
				} else {
					local.failures++
					if status == http.StatusOK || ((op == "get" || op == "delete") && status == http.StatusNotFound) {
						local.protocolFailures++
					} else {
						local.httpFailures++
					}
				}
			}
			workerResults[workerID] = local
		}(w)
	}

	start := time.Now()
	result.measurementStartedAt = start.UTC()
	for i := 0; i < cfg.requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	result.elapsed = time.Since(start)
	result.total = cfg.requests
	for _, local := range workerResults {
		result.errors += local.errors
		result.successes += local.successes
		result.failures += local.failures
		result.logicalMisses += local.logicalMisses
		result.timeouts += local.timeouts
		result.httpFailures += local.httpFailures
		result.protocolFailures += local.protocolFailures
		for status, count := range local.statusCount {
			result.statusCount[status] += count
		}
		for operation, count := range local.operations {
			result.operations[operation] += count
		}
	}
	return result, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseConfig(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	progress := stdout
	if cfg.format == "json" {
		progress = stderr
	}
	result, runErr := runBenchmarkWithProgress(cfg, progress)
	if err := writeReport(stdout, cfg, result, runErr); err != nil {
		fmt.Fprintln(stderr, "Cannot write benchmark report:", err)
		return 1
	}
	if runErr != nil {
		fmt.Fprintln(stderr, runErr)
		return 1
	}
	if result.failures > 0 {
		return 1
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
