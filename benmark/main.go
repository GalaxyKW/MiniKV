package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	urlpkg "net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
}

type benchResult struct {
	latencies     []time.Duration
	successes     int64
	failures      int64
	logicalMisses int64
	statusCount   map[int]int64
	errors        int64
	total         int
	elapsed       time.Duration
}

type kvRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func parseFlags() benchConfig {
	cfg := benchConfig{}
	flag.StringVar(&cfg.baseURL, "url", "http://127.0.0.1:8080/kv", "KV API 地址")
	flag.IntVar(&cfg.workers, "workers", 50, "并发 worker 数")
	flag.IntVar(&cfg.requests, "requests", 200000, "总请求数")
	flag.StringVar(&cfg.op, "op", "mixed", "操作类型: put|get|delete|mixed")
	flag.IntVar(&cfg.keyspace, "keyspace", 20000, "压测 key 空间大小")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Second, "HTTP 客户端超时")
	flag.IntVar(&cfg.writeRatio, "write-ratio", 20, "mixed 模式下 PUT 占比(%)")
	flag.IntVar(&cfg.deleteRatio, "delete-ratio", 5, "mixed 模式下 DELETE 占比(%)")
	flag.BoolVar(&cfg.preload, "preload", true, "GET/mixed 前是否预热数据")
	flag.IntVar(&cfg.preloadCount, "preload-count", 20000, "预热 key 数量")
	flag.Int64Var(&cfg.seed, "seed", 1, "随机种子")
	flag.IntVar(&cfg.valueSize, "value-size", 128, "value 字节数 (0 到 1048576)")
	flag.Parse()

	cfg.op = strings.ToLower(cfg.op)
	if cfg.workers <= 0 {
		cfg.workers = 1
	}
	if cfg.requests <= 0 {
		cfg.requests = 1
	}
	if cfg.keyspace <= 0 {
		cfg.keyspace = 1
	}
	if cfg.preloadCount <= 0 {
		cfg.preloadCount = cfg.keyspace
	}
	if cfg.writeRatio < 0 {
		cfg.writeRatio = 0
	}
	if cfg.writeRatio > 100 {
		cfg.writeRatio = 100
	}
	if cfg.deleteRatio < 0 {
		cfg.deleteRatio = 0
	}
	if cfg.writeRatio+cfg.deleteRatio > 100 {
		cfg.deleteRatio = 100 - cfg.writeRatio
		if cfg.deleteRatio < 0 {
			cfg.deleteRatio = 0
		}
	}
	return cfg
}

func newHTTPClient(timeout time.Duration, workers int) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        workers * 4,
		MaxIdleConnsPerHost: workers * 4,
		IdleConnTimeout:     30 * time.Second,
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

func doRequest(client *http.Client, req *http.Request) (int, string, error) {
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
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

func chooseMixedOp(r *rand.Rand, writeRatio, deleteRatio int) string {
	n := r.Intn(100)
	if n < writeRatio {
		return "put"
	}
	if n < writeRatio+deleteRatio {
		return "delete"
	}
	return "get"
}

func preloadData(cfg benchConfig, client *http.Client) error {
	if !cfg.preload || (cfg.op != "get" && cfg.op != "mixed") {
		return nil
	}
	count := cfg.preloadCount
	if count > cfg.keyspace {
		count = cfg.keyspace
	}
	fmt.Printf("正在预热 %d 个 key...\n", count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("k%d", i)
		value := makeValue(fmt.Sprintf("v%d", i), cfg.valueSize)
		status, body, err := doPut(client, cfg.baseURL, key, value)
		if err != nil || status != http.StatusOK || body != "OK\n" {
			return fmt.Errorf("预热 key %s 失败: HTTP %d, body=%q, err=%v", key, status, body, err)
		}
	}
	fmt.Println("预热完成。")
	return nil
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
	result := benchResult{
		latencies:   make([]time.Duration, 0, cfg.requests),
		statusCount: make(map[int]int64),
		total:       cfg.requests,
	}

	client := newHTTPClient(cfg.timeout, cfg.workers)
	defer client.CloseIdleConnections()
	if err := preloadData(cfg, client); err != nil {
		return result, err
	}

	jobs := make(chan int)
	latCh := make(chan time.Duration, cfg.requests)
	statusCh := make(chan int, cfg.requests)
	var errorCount int64
	var successCount int64
	var failureCount int64
	var logicalMissCount int64

	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(cfg.seed + int64(workerID)))
			for i := range jobs {
				op := cfg.op
				if op == "mixed" {
					op = chooseMixedOp(r, cfg.writeRatio, cfg.deleteRatio)
				}

				key := fmt.Sprintf("k%d", r.Intn(cfg.keyspace))
				value := makeValue(fmt.Sprintf("v%d-%d", workerID, i), cfg.valueSize)

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
				latCh <- time.Since(start)

				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					atomic.AddInt64(&failureCount, 1)
					continue
				}

				statusCh <- status
				success, miss := classifyResult(op, status, body)
				if miss {
					atomic.AddInt64(&logicalMissCount, 1)
					continue
				}

				if success {
					atomic.AddInt64(&successCount, 1)
				} else {
					atomic.AddInt64(&failureCount, 1)
				}
			}
		}(w)
	}

	start := time.Now()
	for i := 0; i < cfg.requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	result.elapsed = time.Since(start)
	close(latCh)
	close(statusCh)

	for d := range latCh {
		result.latencies = append(result.latencies, d)
	}
	for s := range statusCh {
		result.statusCount[s]++
	}
	result.errors = errorCount
	result.successes = successCount
	result.failures = failureCount
	result.logicalMisses = logicalMissCount
	return result, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil((p/100.0)*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func avgDuration(all []time.Duration) time.Duration {
	if len(all) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range all {
		total += d
	}
	return total / time.Duration(len(all))
}

func printReport(cfg benchConfig, result benchResult) {
	sort.Slice(result.latencies, func(i, j int) bool {
		return result.latencies[i] < result.latencies[j]
	})

	p50 := percentile(result.latencies, 50)
	p95 := percentile(result.latencies, 95)
	p99 := percentile(result.latencies, 99)
	p999 := percentile(result.latencies, 99.9)
	avg := avgDuration(result.latencies)

	qps := float64(0)
	goodQPS := float64(0)
	if result.elapsed > 0 {
		qps = float64(result.total) / result.elapsed.Seconds()
		goodQPS = float64(result.successes+result.logicalMisses) / result.elapsed.Seconds()
	}
	successRate := float64(0)
	if result.total > 0 {
		successRate = float64(result.successes+result.logicalMisses) * 100.0 / float64(result.total)
	}

	fmt.Println("\n===== MiniKV 压测报告 =====")
	fmt.Printf("目标地址        : %s\n", cfg.baseURL)
	fmt.Printf("操作类型        : %s\n", cfg.op)
	fmt.Printf("并发 Worker     : %d\n", cfg.workers)
	fmt.Printf("总请求数        : %d\n", result.total)
	fmt.Printf("Key 空间        : %d\n", cfg.keyspace)
	fmt.Printf("Value 字节数    : %d\n", cfg.valueSize)
	fmt.Printf("随机种子        : %d\n", cfg.seed)
	fmt.Printf("写入/删除占比   : %d%% / %d%%\n", cfg.writeRatio, cfg.deleteRatio)
	fmt.Printf("总耗时          : %v\n", result.elapsed)
	fmt.Printf("QPS             : %.2f\n", qps)
	fmt.Printf("成功吞吐量      : %.2f req/s（含正常未命中）\n", goodQPS)
	fmt.Printf("平均延迟        : %v\n", avg)
	fmt.Printf("P50 延迟        : %v\n", p50)
	fmt.Printf("P95 延迟        : %v\n", p95)
	fmt.Printf("P99 延迟        : %v\n", p99)
	fmt.Printf("P99.9 延迟      : %v\n", p999)
	fmt.Printf("成功请求        : %d\n", result.successes)
	fmt.Printf("逻辑未命中      : %d\n", result.logicalMisses)
	fmt.Printf("失败请求        : %d\n", result.failures)
	fmt.Printf("网络错误        : %d\n", result.errors)
	fmt.Printf("系统成功率      : %.2f%%\n", successRate)

	fmt.Println("HTTP 状态码分布:")
	statusCodes := make([]int, 0, len(result.statusCount))
	for code := range result.statusCount {
		statusCodes = append(statusCodes, code)
	}
	sort.Ints(statusCodes)
	for _, code := range statusCodes {
		fmt.Printf("  %d: %d\n", code, result.statusCount[code])
	}
	fmt.Println("==============================")
}

func main() {
	cfg := parseFlags()
	if cfg.valueSize < 0 || cfg.valueSize > 1024*1024 ||
		(cfg.op != "get" && cfg.op != "put" && cfg.op != "delete" && cfg.op != "mixed") {
		fmt.Fprintln(os.Stderr, "操作类型或 value-size 参数无效")
		os.Exit(2)
	}
	result, err := runBenchmark(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printReport(cfg, result)
	if result.failures > 0 {
		os.Exit(1)
	}
}
