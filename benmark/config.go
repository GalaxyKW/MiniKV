package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

func parseConfig(args []string, output io.Writer) (benchConfig, error) {
	cfg := benchConfig{}
	flags := flag.NewFlagSet("minikv-bench", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.baseURL, "url", "http://127.0.0.1:8080/kv", "KV API 地址")
	flags.IntVar(&cfg.workers, "workers", 50, "并发 worker 数")
	flags.IntVar(&cfg.requests, "requests", 200000, "总请求数")
	flags.IntVar(&cfg.rate, "rate", 0, "固定到达率 (req/s，0 为闭环负载，最大 1000000000)")
	flags.StringVar(&cfg.op, "op", "mixed", "操作类型: put|get|delete|mixed")
	flags.IntVar(&cfg.keyspace, "keyspace", 20000, "压测 key 空间大小")
	flags.DurationVar(&cfg.timeout, "timeout", 2*time.Second, "HTTP 客户端超时")
	flags.IntVar(&cfg.writeRatio, "write-ratio", 20, "mixed 模式下 PUT 占比(%)")
	flags.IntVar(&cfg.deleteRatio, "delete-ratio", 5, "mixed 模式下 DELETE 占比(%)")
	flags.BoolVar(&cfg.preload, "preload", true, "GET/mixed 前是否预热数据")
	flags.IntVar(&cfg.preloadCount, "preload-count", 20000, "预热 key 数量 (0 表示整个 key 空间)")
	flags.Int64Var(&cfg.seed, "seed", 1, "随机种子")
	flags.IntVar(&cfg.valueSize, "value-size", 128, "value 字节数 (0 到 1048576)")
	flags.StringVar(&cfg.format, "format", "text", "报告格式: text|json")
	if err := flags.Parse(args); err != nil {
		return benchConfig{}, err
	}
	if flags.NArg() != 0 {
		return benchConfig{}, fmt.Errorf("不支持位置参数: %q", flags.Args())
	}
	if cfg.workers <= 0 || cfg.requests <= 0 || cfg.keyspace <= 0 {
		return benchConfig{}, fmt.Errorf("workers、requests 和 keyspace 必须大于 0")
	}
	maxInt := int(^uint(0) >> 1)
	// Keep the requested-worker range shared by the experiment/report tools.
	if cfg.workers > maxInt/4 {
		return benchConfig{}, fmt.Errorf("workers 太大，超过支持的配置范围")
	}
	if cfg.rate < 0 || cfg.rate > int(time.Second) {
		return benchConfig{}, fmt.Errorf("rate 必须在 0 到 1000000000 之间")
	}
	// Each exact duration series needs eight bytes per planned request.
	// Fixed arrivals retain service, dispatch and scheduled-completion samples.
	sampleBytes := 8
	if cfg.rate > 0 {
		sampleBytes = 24
	}
	if cfg.requests > maxInt/sampleBytes {
		return benchConfig{}, fmt.Errorf("requests 太大，延迟切片大小会溢出")
	}
	if cfg.rate > 0 && !validArrivalSchedule(cfg.requests, cfg.rate) {
		return benchConfig{}, fmt.Errorf("固定到达计划时长超过 time.Duration 支持的范围")
	}
	if cfg.workerCount() > maxInt/int(unsafe.Sizeof(benchResult{})) {
		return benchConfig{}, fmt.Errorf("实际 workers 太大，结果切片大小会溢出")
	}
	if cfg.timeout <= 0 {
		return benchConfig{}, fmt.Errorf("timeout 必须大于 0")
	}
	if cfg.valueSize < 0 || cfg.valueSize > 1024*1024 {
		return benchConfig{}, fmt.Errorf("value-size 必须在 0 到 1048576 之间")
	}
	cfg.op = strings.ToLower(cfg.op)
	if cfg.op != "put" && cfg.op != "get" && cfg.op != "delete" && cfg.op != "mixed" {
		return benchConfig{}, fmt.Errorf("op 必须为 put、get、delete 或 mixed")
	}
	if cfg.writeRatio < 0 || cfg.writeRatio > 100 || cfg.deleteRatio < 0 || cfg.deleteRatio > 100 ||
		cfg.writeRatio+cfg.deleteRatio > 100 {
		return benchConfig{}, fmt.Errorf("write-ratio 和 delete-ratio 必须在 0 到 100 之间且总和不超过 100")
	}
	if cfg.preloadCount < 0 {
		return benchConfig{}, fmt.Errorf("preload-count 不能为负数")
	}
	if cfg.format != "text" && cfg.format != "json" {
		return benchConfig{}, fmt.Errorf("format 必须为 text 或 json")
	}
	endpoint, err := url.Parse(cfg.baseURL)
	if err != nil {
		return benchConfig{}, fmt.Errorf("无效的 url: %w", err)
	}
	if (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Hostname() == "" ||
		endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || strings.Contains(cfg.baseURL, "#") {
		return benchConfig{}, fmt.Errorf("url 必须为带主机名的 http/https 地址，且不能包含用户信息、query 或 fragment")
	}
	// Parse also accepts unbracketed IPv6 and out-of-range numeric ports, which
	// cannot be used as HTTP dial targets. Reject these before any experiment.
	if strings.HasPrefix(endpoint.Host, "[") {
		address, err := netip.ParseAddr(endpoint.Hostname())
		if err != nil || !address.Is6() {
			return benchConfig{}, fmt.Errorf("url 中的方括号必须包含合法 IPv6 地址")
		}
	} else if strings.Contains(endpoint.Host, ":") {
		if _, _, err := net.SplitHostPort(endpoint.Host); err != nil {
			return benchConfig{}, fmt.Errorf("url 主机或端口无效: %w", err)
		}
	}
	if port := endpoint.Port(); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return benchConfig{}, fmt.Errorf("url 端口必须在 1 到 65535 之间")
		}
	}
	return cfg, nil
}

func validArrivalSchedule(requests, rate int) bool {
	if requests <= 0 || rate <= 0 || rate > int(time.Second) {
		return false
	}
	const maxDuration = int64(1<<63 - 1)
	// Split before multiplying: requests*1e9 may overflow even if its quotient
	// fits. The remainder product is safe because rate is at most 1e9.
	seconds := int64(requests / rate)
	if seconds > maxDuration/int64(time.Second) {
		return false
	}
	fraction := int64(requests%rate) * int64(time.Second) / int64(rate)
	return fraction <= maxDuration-seconds*int64(time.Second)
}
