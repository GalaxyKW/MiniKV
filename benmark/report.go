package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/bits"
	"runtime"
	"runtime/debug"
	"sort"
	"time"
)

type reportConfig struct {
	URL          string `json:"url"`
	Workers      int    `json:"workers"`
	Requests     int    `json:"requests"`
	Rate         int    `json:"rate,omitempty"`
	Operation    string `json:"operation"`
	Keyspace     int    `json:"keyspace"`
	TimeoutNS    int64  `json:"timeout_ns"`
	WriteRatio   int    `json:"write_ratio"`
	DeleteRatio  int    `json:"delete_ratio"`
	Preload      bool   `json:"preload"`
	PreloadCount int    `json:"preload_count"`
	Seed         int64  `json:"seed"`
	ValueSize    int    `json:"value_size"`
}

type clientBuild struct {
	GoVersion  string `json:"go_version"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	Revision   string `json:"vcs_revision,omitempty"`
	RevisionAt string `json:"vcs_time,omitempty"`
	Modified   *bool  `json:"vcs_modified,omitempty"`
}

type preloadReport struct {
	TargetKeys    int   `json:"target_keys"`
	CompletedKeys int   `json:"completed_keys"`
	ElapsedNS     int64 `json:"elapsed_ns"`
}

type outcomeReport struct {
	Requests         int   `json:"requests"`
	Successes        int64 `json:"successes"`
	LogicalMisses    int64 `json:"logical_misses"`
	Failures         int64 `json:"failures"`
	NetworkErrors    int64 `json:"network_errors"`
	Timeouts         int64 `json:"timeouts"`
	TransportErrors  int64 `json:"transport_errors"`
	HTTPFailures     int64 `json:"http_failures"`
	ProtocolFailures int64 `json:"protocol_failures"`
}

type latencyReport struct {
	Samples int   `json:"samples"`
	Mean    int64 `json:"mean"`
	Min     int64 `json:"min"`
	P50     int64 `json:"p50"`
	P95     int64 `json:"p95"`
	P99     int64 `json:"p99"`
	P999    int64 `json:"p99_9"`
	Max     int64 `json:"max"`
}

type arrivalReport struct {
	Planned            int   `json:"planned"`
	Started            int   `json:"started"`
	DroppedBusy        int   `json:"dropped_busy"`
	DroppedLate        int   `json:"dropped_late"`
	ScheduleDurationNS int64 `json:"schedule_duration_ns"`
}

// Schema 1 records client-side configuration and measurements. Engine and
// gateway build/configuration must be recorded separately by the experiment.
type benchmarkReport struct {
	SchemaVersion         int              `json:"schema_version"`
	StartedAt             string           `json:"started_at"`
	MeasurementStartedAt  string           `json:"measurement_started_at,omitempty"`
	Complete              bool             `json:"complete"`
	Error                 string           `json:"error,omitempty"`
	LoadModel             string           `json:"load_model"`
	WorkloadGenerator     string           `json:"workload_generator"`
	Config                reportConfig     `json:"config"`
	ClientBuild           clientBuild      `json:"client_build"`
	Preload               preloadReport    `json:"preload"`
	ElapsedNS             int64            `json:"elapsed_ns"`
	Outcomes              outcomeReport    `json:"outcomes"`
	Operations            map[string]int64 `json:"operations"`
	HTTPStatus            map[int]int64    `json:"http_statuses"`
	LatencyNS             latencyReport    `json:"latency_ns"`
	QPSTotal              float64          `json:"qps_total"`
	QPSSuccessful         float64          `json:"qps_successful"`
	SystemSuccessRatePct  float64          `json:"system_success_rate_pct"`
	Arrivals              *arrivalReport   `json:"arrivals,omitempty"`
	DispatchDelayNS       *latencyReport   `json:"dispatch_delay_ns,omitempty"`
	ScheduledLatencyNS    *latencyReport   `json:"scheduled_latency_ns,omitempty"`
	OfferedSuccessRatePct *float64         `json:"offered_success_rate_pct,omitempty"`
}

func currentBuild() clientBuild {
	result := clientBuild{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				result.Revision = setting.Value
			case "vcs.time":
				result.RevisionAt = setting.Value
			case "vcs.modified":
				modified := setting.Value == "true"
				result.Modified = &modified
			}
		}
	}
	return result
}

func percentile(sorted []time.Duration, numerator, denominator int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if denominator <= 0 {
		panic("percentile denominator must be positive")
	}
	if numerator <= 0 {
		return sorted[0]
	}
	if numerator >= denominator {
		return sorted[len(sorted)-1]
	}
	// Exact nearest-rank arithmetic: floating point can turn P99.9 of 1000
	// samples into ceil(999.0000000000001), selecting the wrong sample. The
	// 128-bit intermediate also avoids overflowing for a large sample count.
	hi, lo := bits.Mul64(uint64(len(sorted)), uint64(numerator))
	rank, remainder := bits.Div64(hi, lo, uint64(denominator))
	if remainder != 0 {
		rank++
	}
	return sorted[rank-1]
}

func avgDuration(all []time.Duration) time.Duration {
	if len(all) == 0 {
		return 0
	}
	// Accumulate the quotient and remainder separately: summing all durations
	// can overflow int64 for a long or highly concurrent experiment.
	n := int64(len(all))
	var mean, remainder int64
	for _, duration := range all {
		value := int64(duration)
		mean += value / n
		part := value % n
		if remainder >= n-part {
			mean++
			remainder -= n - part
		} else {
			remainder += part
		}
	}
	return time.Duration(mean)
}

func summarizeLatency(latencies []time.Duration) latencyReport {
	// Workers have finished; reuse each sample allocation for exact sorting.
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	report := latencyReport{
		Samples: len(latencies), Mean: int64(avgDuration(latencies)),
		P50: int64(percentile(latencies, 50, 100)), P95: int64(percentile(latencies, 95, 100)),
		P99: int64(percentile(latencies, 99, 100)), P999: int64(percentile(latencies, 999, 1000)),
	}
	if len(latencies) > 0 {
		report.Min = int64(latencies[0])
		report.Max = int64(latencies[len(latencies)-1])
	}
	return report
}

func makeReport(cfg benchConfig, result benchResult, runErr error) benchmarkReport {
	report := benchmarkReport{
		SchemaVersion: 1, StartedAt: result.startedAt.Format(time.RFC3339Nano),
		Complete: runErr == nil, LoadModel: "closed_loop", WorkloadGenerator: workloadVersion,
		Config: reportConfig{
			URL: cfg.baseURL, Workers: cfg.workers, Requests: cfg.requests, Rate: cfg.rate, Operation: cfg.op,
			Keyspace: cfg.keyspace, TimeoutNS: int64(cfg.timeout), WriteRatio: cfg.writeRatio,
			DeleteRatio: cfg.deleteRatio, Preload: cfg.preload, PreloadCount: cfg.preloadCount,
			Seed: cfg.seed, ValueSize: cfg.valueSize,
		},
		ClientBuild: currentBuild(),
		Preload:     preloadReport{TargetKeys: effectivePreloadCount(cfg), CompletedKeys: result.preloaded, ElapsedNS: int64(result.preloadElapsed)},
		ElapsedNS:   int64(result.elapsed),
		Outcomes: outcomeReport{
			Requests: result.total, Successes: result.successes, LogicalMisses: result.logicalMisses,
			Failures: result.failures, NetworkErrors: result.errors, Timeouts: result.timeouts,
			TransportErrors: result.errors - result.timeouts, HTTPFailures: result.httpFailures, ProtocolFailures: result.protocolFailures,
		},
		Operations: result.operations, HTTPStatus: result.statusCount,
		LatencyNS: summarizeLatency(result.latencies),
	}
	if !result.measurementStartedAt.IsZero() {
		report.MeasurementStartedAt = result.measurementStartedAt.Format(time.RFC3339Nano)
	}
	if runErr != nil {
		report.Error = "preload_failed"
	}
	if result.elapsed > 0 {
		report.QPSTotal = float64(result.total) / result.elapsed.Seconds()
		report.QPSSuccessful = float64(result.successes+result.logicalMisses) / result.elapsed.Seconds()
	}
	if result.total > 0 {
		report.SystemSuccessRatePct = float64(result.successes+result.logicalMisses) * 100 / float64(result.total)
	}
	if cfg.rate > 0 {
		report.LoadModel = "fixed_arrival"
		report.Arrivals = &arrivalReport{
			Planned: cfg.requests, Started: result.total, DroppedBusy: result.droppedBusy,
			DroppedLate: result.droppedLate, ScheduleDurationNS: int64(arrivalOffset(cfg.requests, cfg.rate)),
		}
		dispatch, scheduled := summarizeLatency(result.dispatchDelays), summarizeLatency(result.scheduledLatencies)
		report.DispatchDelayNS, report.ScheduledLatencyNS = &dispatch, &scheduled
		offeredSuccessRate := float64(0)
		if cfg.requests > 0 {
			offeredSuccessRate = float64(result.successes+result.logicalMisses) * 100 / float64(cfg.requests)
		}
		report.OfferedSuccessRatePct = &offeredSuccessRate
	}
	return report
}

func writeReport(output io.Writer, cfg benchConfig, result benchResult, runErr error) error {
	report := makeReport(cfg, result, runErr)
	if cfg.format == "json" {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	w := bufio.NewWriter(output)
	fmt.Fprintln(w, "\n===== MiniKV 压测报告 =====")
	fmt.Fprintf(w, "目标地址        : %s\n", cfg.baseURL)
	fmt.Fprintf(w, "操作类型        : %s\n", cfg.op)
	fmt.Fprintf(w, "并发 Worker     : %d\n", cfg.workers)
	fmt.Fprintf(w, "总请求数        : %d\n", result.total)
	if report.Arrivals != nil {
		fmt.Fprintf(w, "负载模型        : %s\n", report.LoadModel)
		fmt.Fprintf(w, "目标到达率      : %d req/s\n", cfg.rate)
		fmt.Fprintf(w, "计划到达数      : %d\n", report.Arrivals.Planned)
		fmt.Fprintf(w, "实际发起数      : %d\n", report.Arrivals.Started)
		fmt.Fprintf(w, "Worker 忙丢弃   : %d\n", report.Arrivals.DroppedBusy)
		fmt.Fprintf(w, "调度过期丢弃    : %d\n", report.Arrivals.DroppedLate)
		fmt.Fprintf(w, "计划到达时长    : %v\n", time.Duration(report.Arrivals.ScheduleDurationNS))
	}
	fmt.Fprintf(w, "Key 空间        : %d\n", cfg.keyspace)
	fmt.Fprintf(w, "Value 字节数    : %d\n", cfg.valueSize)
	fmt.Fprintf(w, "随机种子        : %d\n", cfg.seed)
	fmt.Fprintf(w, "写入/删除占比   : %d%% / %d%%\n", cfg.writeRatio, cfg.deleteRatio)
	fmt.Fprintf(w, "负载生成器      : %s\n", report.WorkloadGenerator)
	fmt.Fprintf(w, "实际 PUT/GET/DEL: %d / %d / %d\n", result.operations["put"], result.operations["get"], result.operations["delete"])
	fmt.Fprintf(w, "总耗时          : %v\n", result.elapsed)
	fmt.Fprintf(w, "QPS             : %.2f\n", report.QPSTotal)
	fmt.Fprintf(w, "成功吞吐量      : %.2f req/s（含正常未命中）\n", report.QPSSuccessful)
	if report.Arrivals != nil {
		writeArrivalLatency(w, "服务延迟", report.LatencyNS)
		writeArrivalLatency(w, "调度延迟", *report.DispatchDelayNS)
		writeArrivalLatency(w, "计划到完成延迟", *report.ScheduledLatencyNS)
	} else {
		fmt.Fprintf(w, "平均延迟        : %v\n", time.Duration(report.LatencyNS.Mean))
		fmt.Fprintf(w, "P50 延迟        : %v\n", time.Duration(report.LatencyNS.P50))
		fmt.Fprintf(w, "P95 延迟        : %v\n", time.Duration(report.LatencyNS.P95))
		fmt.Fprintf(w, "P99 延迟        : %v\n", time.Duration(report.LatencyNS.P99))
		fmt.Fprintf(w, "P99.9 延迟      : %v\n", time.Duration(report.LatencyNS.P999))
	}
	fmt.Fprintf(w, "成功请求        : %d\n", result.successes)
	fmt.Fprintf(w, "逻辑未命中      : %d\n", result.logicalMisses)
	fmt.Fprintf(w, "失败请求        : %d\n", result.failures)
	fmt.Fprintf(w, "网络错误        : %d\n", result.errors)
	fmt.Fprintf(w, "客户端超时      : %d\n", result.timeouts)
	fmt.Fprintf(w, "HTTP 状态失败   : %d\n", result.httpFailures)
	fmt.Fprintf(w, "响应格式失败    : %d\n", result.protocolFailures)
	if report.Arrivals != nil {
		fmt.Fprintf(w, "已发请求成功率  : %.2f%%（分母：%d 个已发请求）\n", report.SystemSuccessRatePct, report.Arrivals.Started)
		fmt.Fprintf(w, "计划到达成功率  : %.2f%%（分母：%d 个计划到达）\n", *report.OfferedSuccessRatePct, report.Arrivals.Planned)
	} else {
		fmt.Fprintf(w, "系统成功率      : %.2f%%\n", report.SystemSuccessRatePct)
	}
	if runErr != nil {
		fmt.Fprintln(w, "测量未开始：数据预置失败。")
	}
	fmt.Fprintln(w, "HTTP 状态码分布:")
	statusCodes := make([]int, 0, len(result.statusCount))
	for code := range result.statusCount {
		statusCodes = append(statusCodes, code)
	}
	sort.Ints(statusCodes)
	for _, code := range statusCodes {
		fmt.Fprintf(w, "  %d: %d\n", code, result.statusCount[code])
	}
	fmt.Fprintln(w, "==============================")
	return w.Flush()
}

func writeArrivalLatency(w io.Writer, name string, latency latencyReport) {
	fmt.Fprintf(w, "%s: 样本=%d 平均=%v 最小=%v P50=%v P95=%v P99=%v P99.9=%v 最大=%v\n", name,
		latency.Samples, time.Duration(latency.Mean), time.Duration(latency.Min), time.Duration(latency.P50),
		time.Duration(latency.P95), time.Duration(latency.P99), time.Duration(latency.P999), time.Duration(latency.Max))
}
