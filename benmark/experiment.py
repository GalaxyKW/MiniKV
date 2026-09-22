#!/usr/bin/env python3
"""Run isolated, repeatable MiniKV experiments using only the standard library."""

import argparse
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import signal
import shutil
import socket
import subprocess
import sys
import time

from experiment_support import collect_metadata, sample_process


ROOT = Path(__file__).resolve().parents[1]
MAX_DURATION_NS = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
INTERRUPT_STATE = {"launching": False, "pending": None}


class ExperimentError(Exception):
    pass


class Interrupted(ExperimentError):
    pass


class DeadlineSocket(socket.socket):
    """Bound every raw I/O by one deadline, including buffered HTTP reads."""

    def __init__(self, deadline):
        super().__init__(socket.AF_INET, socket.SOCK_STREAM)
        self.deadline = deadline

    def arm_timeout(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("stats absolute deadline exceeded")
        self.settimeout(remaining)

    def connect(self, address):
        self.arm_timeout()
        return super().connect(address)

    def sendall(self, data, flags=0):
        self.arm_timeout()
        return super().sendall(data, flags)

    def recv_into(self, buffer, nbytes=0, flags=0):
        # socket.makefile() uses SocketIO.readinto -> recv_into for each raw
        # receive. A peer trickling headers/body cannot renew the total budget.
        self.arm_timeout()
        return super().recv_into(buffer, nbytes, flags)


class StatsConnection(http.client.HTTPConnection):
    def __init__(self, port, timeout):
        super().__init__("127.0.0.1", port, timeout=timeout)
        self.deadline = time.monotonic() + timeout

    def connect(self):
        self.sock = DeadlineSocket(self.deadline)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.host, self.port))


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    # Each artifact directory is exclusively ours. Atomic replacement keeps an
    # earlier index readable if this process stops halfway through an update.
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    temporary.replace(path)


def append_json(stream, value):
    stream.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
    stream.flush()


def integer(low, high):
    def parse(raw):
        value = int(raw)
        if not low <= value <= high:
            raise argparse.ArgumentTypeError(f"must be in [{low}, {high}]")
        return value
    return parse


def positive_seconds(raw):
    value = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return value


def validate_arrival_config(rate, requests, maximum=sys.maxsize):
    if type(rate) is not int or not 0 <= rate <= 1000000000:
        raise ValueError("rate must be an integer in [0, 1000000000]")
    if rate and (requests > maximum // 24 or requests * 1000000000 // rate > MAX_DURATION_NS):
        raise ValueError("fixed-arrival request count or schedule duration is too large")


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="new artifact directory; existing paths are refused")
    for name, default in (("engine", ROOT / "build/engine"), ("gateway", ROOT / "bin/minikv-go"),
                          ("bench", ROOT / "bin/minikv-bench")):
        parser.add_argument("--" + name, type=Path, default=default)
    parser.add_argument("--modes", default="throughput,reliable")
    parser.add_argument("--repeats", type=integer(1, 1000), default=3)
    parser.add_argument("--requests", type=integer(1, sys.maxsize // 8), default=20000)
    parser.add_argument("--rate", type=integer(0, 1000000000), default=0,
                        help="planned arrivals per second; 0 uses the closed-loop workload")
    parser.add_argument("--workers", type=integer(1, sys.maxsize // 4), default=20)
    parser.add_argument("--keyspace", type=integer(1, sys.maxsize), default=1000)
    parser.add_argument("--op", choices=("put", "get", "delete", "mixed"), default="mixed")
    parser.add_argument("--write-ratio", type=integer(0, 100), default=20)
    parser.add_argument("--delete-ratio", type=integer(0, 100), default=5)
    parser.add_argument("--value-size", type=integer(0, 1048576), default=128)
    parser.add_argument("--seed", type=integer(-(1 << 63), (1 << 63) - 1), default=1)
    parser.add_argument("--engine-workers", type=integer(1, 1024), default=4)
    parser.add_argument("--rpc-pool", type=integer(1, 65534), default=32)
    parser.add_argument("--gomaxprocs", type=integer(1, 1024), default=4)
    parser.add_argument("--wal-batch", type=integer(1, 65536), default=64)
    parser.add_argument("--wal-flush-ms", type=integer(1, 60000), default=2)
    parser.add_argument("--max-data-bytes", type=integer(0, MAX_UINT64), default=0,
                        help="engine live key/value byte capacity; 0 keeps the unlimited default")
    parser.add_argument("--snapshot-ms", type=integer(1, 86400000), default=1000,
                        help="enabled snapshot interval; every mode also runs with snapshots disabled")
    parser.add_argument("--sample-ms", type=integer(0, 60000), default=100,
                        help="/proc sampling interval; 0 disables periodic resource sampling")
    parser.add_argument("--stats-ms", type=integer(0, 60000), default=250,
                        help="/stats sampling interval; 0 disables periodic status sampling")
    for name, default in (("run-timeout", 120), ("startup-timeout", 10),
                          ("shutdown-timeout", 15), ("settle-timeout", 10)):
        parser.add_argument("--" + name, type=positive_seconds, default=default)
    args = parser.parse_args(argv)
    if sys.version_info < (3, 8):
        parser.error("Python 3.8 or newer is required")
    args.modes = args.modes.split(",")
    if (not args.modes or len(set(args.modes)) != len(args.modes)
            or any(mode not in ("throughput", "reliable") for mode in args.modes)):
        parser.error("--modes must be a unique comma-separated subset of throughput,reliable")
    if args.write_ratio + args.delete_ratio > 100:
        parser.error("write-ratio + delete-ratio must not exceed 100")
    try:
        validate_arrival_config(args.rate, args.requests)
    except ValueError as error:
        parser.error(str(error))
    if sys.platform != "linux":
        parser.error("the engine and process sampler require Linux")
    for name in ("engine", "gateway", "bench"):
        binary = getattr(args, name).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"{name} is not an executable file: {binary}; build with make first")
        setattr(args, name, binary)
    if args.output is None:
        args.output = ROOT / "benmark/results" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    args.output = args.output.absolute()
    try:
        args.output.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        parser.error(f"cannot create a new output directory: {error}")
    return args


def runtime_environment(args):
    # No ambient MINIKV_*, Go tuning, proxy, credentials or loader settings.
    # These already-built programs need only this explicit runtime environment.
    return {"PATH": os.defpath, "LANG": "C", "LC_ALL": "C", "TZ": "UTC",
            "GOMAXPROCS": str(args.gomaxprocs), "GOGC": "100", "GOMEMLIMIT": "off", "GODEBUG": ""}


def ports():
    # Hold both reservations simultaneously so our own choices cannot collide.
    # Servers do not accept inherited listening sockets; an external bind race
    # can still cause startup failure, which is retained as a failed run.
    with socket.socket() as engine, socket.socket() as gateway:
        engine.bind(("127.0.0.1", 0))
        gateway.bind(("127.0.0.1", 0))
        return engine.getsockname()[1], gateway.getsockname()[1]


def configuration(args, directory, mode, interval):
    engine_port, http_port = ports()
    engine = {
        "MINIKV_DATA_DIR": str(directory / "data"), "MINIKV_ENGINE_HOST": "127.0.0.1",
        "MINIKV_ENGINE_PORT": str(engine_port), "MINIKV_WAL_MODE": mode,
        "MINIKV_WAL_BATCH_SIZE": str(args.wal_batch), "MINIKV_WAL_FLUSH_MS": str(args.wal_flush_ms),
        "MINIKV_WAL_QUEUE_BYTES": "16777216", "MINIKV_SNAPSHOT_INTERVAL_MS": str(interval),
        "MINIKV_WORKERS": str(args.engine_workers), "MINIKV_REQUEST_QUEUE_SIZE": "128",
        "MINIKV_MAX_CONNECTIONS": str(max(256, args.rpc_pool + 2)), "MINIKV_CLIENT_IDLE_MS": "30000",
    }
    if args.max_data_bytes:
        engine["MINIKV_MAX_DATA_BYTES"] = str(args.max_data_bytes)
    gateway = {"MINIKV_ENGINE_ADDR": f"127.0.0.1:{engine_port}",
               "MINIKV_HTTP_ADDR": f"127.0.0.1:{http_port}", "MINIKV_RPC_POOL_SIZE": str(args.rpc_pool),
               "MINIKV_RPC_TIMEOUT_MS": "2000"}
    expected = {"url": f"http://127.0.0.1:{http_port}/kv", "workers": args.workers,
                "requests": args.requests, "operation": args.op, "keyspace": args.keyspace,
                "timeout_ns": 2000000000, "write_ratio": args.write_ratio, "delete_ratio": args.delete_ratio,
                "preload": True, "preload_count": 0, "seed": args.seed, "value_size": args.value_size}
    command = [str(args.bench), "-url", expected["url"], "-workers", str(args.workers),
               "-requests", str(args.requests), "-op", args.op, "-keyspace", str(args.keyspace),
               "-timeout", "2s", "-write-ratio", str(args.write_ratio), "-delete-ratio", str(args.delete_ratio),
               "-preload=true", "-preload-count", "0", "-seed", str(args.seed),
               "-value-size", str(args.value_size), "-format", "json"]
    if args.rate:
        expected["rate"] = args.rate
        command.extend(("-rate", str(args.rate)))
    return engine, gateway, expected, command, engine_port, http_port


def check_alive(processes):
    for name in ("engine", "gateway"):
        if name in processes and processes[name].poll() is not None:
            raise ExperimentError(f"{name} exited unexpectedly with {processes[name].returncode}")


def fetch_stats(port, timeout=1):
    sample = {"started_at": utc_now(), "monotonic_start_ns": time.monotonic_ns(),
              "http_status": None, "body": None, "error": None}
    connection = StatsConnection(port, timeout)
    try:
        connection.request("GET", "/stats")
        response = connection.getresponse()
        sample["http_status"] = response.status
        body = response.read(1024 * 1024 + 1)
        if len(body) > 1024 * 1024:
            raise ExperimentError("stats response exceeds 1 MiB")
        # read(amt) can return a short EOF without raising IncompleteRead.
        if response.length not in (None, 0):
            raise ExperimentError("incomplete stats response body")
        sample["body"] = json.loads(body)
        if response.status != 200:
            raise ExperimentError(f"stats HTTP {response.status}")
        validate_stats(sample["body"])
    except Interrupted:
        raise
    except (OSError, ValueError, http.client.HTTPException, ExperimentError) as error:
        sample["error"] = str(error)
    finally:
        connection.close()
    sample.update(finished_at=utc_now(), monotonic_end_ns=time.monotonic_ns())
    return sample


def validate_stats(body):
    if not isinstance(body, dict) or body.get("schema_version") != 1:
        raise ExperimentError("unsupported stats schema")
    if any(not isinstance(body.get(name), dict) for name in ("engine", "server", "gateway")):
        raise ExperimentError("stats missing a component")
    engine = body["engine"]
    if engine.get("io_failed") is not False or engine.get("stopping") is not False:
        raise ExperimentError("engine failed or stopping")
    for field in ("keys", "applied_sequence", "durable_sequence", "wal_pending_bytes", "snapshot_successes_total", "snapshot_failures_total"):
        if type(engine.get(field)) is not int or engine[field] < 0:
            raise ExperimentError(f"invalid stats field: {field}")
    if engine["snapshot_failures_total"]:
        raise ExperimentError("a snapshot failed during this experiment")


def validate_data_capacity(body, maximum, initial=False):
    # Older engines and archives need not provide these additive stats when no
    # capacity was requested. Positive limits must be proved by the running engine.
    if not maximum:
        return
    engine = body.get("engine") if isinstance(body, dict) else None
    if not isinstance(engine, dict):
        raise ExperimentError("missing data capacity stats")
    for name in ("data_capacity_bytes", "data_bytes", "data_rejections_total"):
        if type(engine.get(name)) is not int or not 0 <= engine[name] <= MAX_UINT64:
            raise ExperimentError("invalid data capacity stats field: " + name)
    if engine["data_capacity_bytes"] != maximum or engine["data_bytes"] > maximum:
        raise ExperimentError("data capacity stats do not match the configured limit")
    if initial and (engine["data_bytes"] != 0 or engine["data_rejections_total"] != 0):
        raise ExperimentError("startup data capacity stats do not describe an unused empty database")


def validate_data_rejections(after, settled, report, maximum):
    if maximum and not (after["engine"]["data_rejections_total"] <= settled["engine"]["data_rejections_total"]
                        <= report["outcomes"]["failures"]):
        raise ExperimentError("data capacity rejections contradict benchmark failures")


def wait_ready(processes, engine_port, http_port, timeout):
    deadline = time.monotonic() + timeout
    last_error = "not listening"
    while time.monotonic() < deadline:
        check_alive(processes)
        try:
            if http_port is None:
                with socket.create_connection(("127.0.0.1", engine_port), timeout=min(.1, max(.001, deadline - time.monotonic()))):
                    check_alive(processes)
                    return
            else:
                sample = fetch_stats(http_port, min(.5, max(.001, deadline - time.monotonic())))
                last_error = sample["error"]
                if last_error is None:
                    check_alive(processes)
                    return sample
        except OSError as error:
            last_error = str(error)
        time.sleep(.02)
    raise ExperimentError(f"startup timeout: {last_error}")


def stop_processes(processes, timeout):
    result = {}
    for name in ("bench", "gateway", "engine"):
        if name not in processes:
            continue
        process = processes[name]
        state = {"pid": process.pid, "forced": False, "returncode": process.poll()}
        if state["returncode"] is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                state["forced"] = True
                process.kill()
                process.wait()
        else:
            process.wait()
        state["returncode"] = process.returncode
        result[name] = state
    return result


LATENCY_FIELDS = ("samples", "mean", "min", "p50", "p95", "p99", "p99_9", "max")


def validate_latency(latency, samples):
    if (any(type(latency[name]) is not int or not 0 <= latency[name] <= MAX_DURATION_NS for name in LATENCY_FIELDS)
            or latency["samples"] != samples):
        raise ValueError("invalid latency samples")
    ranks = [latency[name] for name in ("min", "p50", "p95", "p99", "p99_9", "max")]
    if ranks != sorted(ranks) or not latency["min"] <= latency["mean"] <= latency["max"]:
        raise ValueError("inconsistent latency summary")
    if samples == 0 and any(latency[name] for name in LATENCY_FIELDS):
        raise ValueError("empty latency population has nonzero measurements")


def report_degraded(report):
    return report["load_model"] == "fixed_arrival" and bool(
        report["outcomes"]["failures"] + report["arrivals"]["dropped_busy"] + report["arrivals"]["dropped_late"])


def benchmark_exit_code(report):
    return int(report_degraded(report))


def arrival_metrics(report):
    arrivals = report["arrivals"]
    return {"load_model": "fixed_arrival", "rate": report["config"]["rate"],
            "arrival_planned": arrivals["planned"], "arrival_started": arrivals["started"],
            "dropped_busy": arrivals["dropped_busy"], "dropped_late": arrivals["dropped_late"],
            "failures": report["outcomes"]["failures"], "offered_success_rate_pct": report["offered_success_rate_pct"],
            "dispatch_p99_ms": report["dispatch_delay_ns"]["p99"] / 1000000 if arrivals["started"] else None,
            "scheduled_p99_ms": report["scheduled_latency_ns"]["p99"] / 1000000 if arrivals["started"] else None}


def fixed_arrival_settled(body):
    values = (body["server"].get("requests_inflight"), body["gateway"]["rpc"].get("pool_in_use"),
              body["engine"].get("async_requests_inflight", 0))
    if any(type(value) is not int or value < 0 for value in values):
        raise ValueError("fixed-arrival settling requires valid in-flight request counters")
    engine = body["engine"]
    return not any(values) and engine["durable_sequence"] == engine["applied_sequence"] and engine["wal_pending_bytes"] == 0


def validate_fixed_completion(after, settled, report, mode):
    initial = report["preload"]["completed_keys"]
    writes = report["operations"]["put"] + report["operations"]["delete"]
    minimum = initial + max(0, writes - report["outcomes"]["failures"])
    maximum = initial + writes
    first, last = after["engine"], settled["engine"]
    if (not minimum <= first["applied_sequence"] <= last["applied_sequence"] <= maximum
            or not 0 <= first["durable_sequence"] <= first["applied_sequence"]
            or first["durable_sequence"] > last["durable_sequence"]
            or (mode == "reliable" and first["durable_sequence"] < minimum)
            or (mode == "reliable" and minimum == maximum and first["wal_pending_bytes"] != 0)
            or not fixed_arrival_settled(settled)):
        raise ValueError("fixed-arrival WAL evidence contradicts attempted writes or quiescent drain")


def read_report(path, expected):
    try:
        report = json.loads(path.read_text(), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
        rate = expected.get("rate", 0)
        validate_arrival_config(rate, expected["requests"], (1 << 63) - 1)
        if (type(report["schema_version"]) is not int or report["schema_version"] != 1 or report["complete"] is not True
                or report["load_model"] != ("fixed_arrival" if rate else "closed_loop")
                or not isinstance(report["workload_generator"], str)
                or not report["workload_generator"]
                or report["config"] != expected or report.get("error")
                or any(type(report["config"][key]) is not type(value) for key, value in expected.items())):
            raise ValueError("report schema, completion or configuration mismatch")
        if not rate and any(name in report for name in ("arrivals", "dispatch_delay_ns", "scheduled_latency_ns", "offered_success_rate_pct")):
            raise ValueError("closed-loop report contains fixed-arrival measurements")
        outcomes = report["outcomes"]
        fields = ("requests", "successes", "logical_misses", "failures", "network_errors", "timeouts",
                  "transport_errors", "http_failures", "protocol_failures")
        if any(type(outcomes[name]) is not int or outcomes[name] < 0 for name in fields):
            raise ValueError("invalid outcome count")
        if ((not rate and outcomes["requests"] != expected["requests"])
                or sum(outcomes[name] for name in ("successes", "logical_misses", "failures")) != outcomes["requests"]
                or sum(outcomes[name] for name in ("network_errors", "http_failures", "protocol_failures")) != outcomes["failures"]
                or outcomes["timeouts"] + outcomes["transport_errors"] != outcomes["network_errors"]):
            raise ValueError("outcome counts do not reconcile")
        if rate:
            arrivals = report["arrivals"]
            if (any(type(arrivals[name]) is not int or not 0 <= arrivals[name] <= expected["requests"]
                    for name in ("planned", "started", "dropped_busy", "dropped_late"))
                    or arrivals["planned"] != expected["requests"]
                    or arrivals["started"] != outcomes["requests"]
                    or arrivals["started"] + arrivals["dropped_busy"] + arrivals["dropped_late"] != arrivals["planned"]
                    or type(arrivals["schedule_duration_ns"]) is not int
                    or arrivals["schedule_duration_ns"] != expected["requests"] * 1000000000 // rate):
                raise ValueError("arrival counts or schedule duration do not reconcile")
        operations = report["operations"]
        if (set(operations) != {"put", "get", "delete"}
                or any(type(operations[name]) is not int or operations[name] < 0 for name in ("put", "get", "delete"))
                or sum(operations.values()) != outcomes["requests"]):
            raise ValueError("operation counts do not reconcile")
        if expected["operation"] != "mixed" and operations[expected["operation"]] != outcomes["requests"]:
            raise ValueError("operation counts contradict the configured workload")
        if expected["operation"] == "mixed":
            ratios = {"put": expected["write_ratio"], "delete": expected["delete_ratio"],
                      "get": 100 - expected["write_ratio"] - expected["delete_ratio"]}
            # Finite samples need not match the requested proportions, but a
            # disabled operation can never be generated by this workload.
            if any(ratio == 0 and operations[name] for name, ratio in ratios.items()):
                raise ValueError("operation counts contradict the configured workload")
        latency = report["latency_ns"]
        validate_latency(latency, outcomes["requests"])
        if type(report["elapsed_ns"]) is not int or not 0 < report["elapsed_ns"] <= MAX_DURATION_NS:
            raise ValueError("invalid elapsed time")
        if rate:
            if report["elapsed_ns"] < arrivals["schedule_duration_ns"]:
                raise ValueError("measurement ends before the arrival schedule")
            for name in ("dispatch_delay_ns", "scheduled_latency_ns"):
                validate_latency(report[name], outcomes["requests"])
            if (report["scheduled_latency_ns"]["max"] > report["elapsed_ns"]
                    or report["scheduled_latency_ns"]["mean"] - latency["mean"] - report["dispatch_delay_ns"]["mean"] not in (0, 1)
                    or report["scheduled_latency_ns"]["min"] < latency["min"] + report["dispatch_delay_ns"]["min"]
                    or report["scheduled_latency_ns"]["max"] > latency["max"] + report["dispatch_delay_ns"]["max"]
                    or any(report["scheduled_latency_ns"][name] < max(latency[name], report["dispatch_delay_ns"][name])
                           for name in LATENCY_FIELDS if name != "samples")):
                raise ValueError("scheduled latency contradicts dispatch or service latency")
        rate_fields = ("qps_total", "qps_successful", "system_success_rate_pct")
        for field in rate_fields + (("offered_success_rate_pct",) if rate else ()):
            if type(report[field]) not in (float, int) or not math.isfinite(report[field]) or report[field] < 0:
                raise ValueError("invalid rate")
        successful = outcomes["successes"] + outcomes["logical_misses"]
        expected_rates = {"qps_total": outcomes["requests"] * 1000000000 / report["elapsed_ns"],
                          "qps_successful": successful * 1000000000 / report["elapsed_ns"],
                          "system_success_rate_pct": successful * 100 / outcomes["requests"] if outcomes["requests"] else 0}
        if rate:
            expected_rates["offered_success_rate_pct"] = successful * 100 / expected["requests"]
        if any(not math.isclose(report[field], value, rel_tol=1e-9, abs_tol=1e-9)
               for field, value in expected_rates.items()):
            raise ValueError("reported rates do not match counts and elapsed time")
        preload = report["preload"]
        target = expected["keyspace"] if expected["operation"] in ("get", "mixed") else 0
        if (type(preload["target_keys"]) is not int or type(preload["completed_keys"]) is not int
                or preload["target_keys"] != target or preload["completed_keys"] != target
                or type(preload["elapsed_ns"]) is not int or not 0 <= preload["elapsed_ns"] <= MAX_DURATION_NS):
            raise ValueError("preload did not complete the requested initial dataset")
        if outcomes["failures"] and not rate:
            raise ValueError(f"{outcomes['failures']} benchmark requests failed")
        statuses = report["http_statuses"]
        if not isinstance(statuses, dict) or any(type(value) is not int or value < 0 for value in statuses.values()):
            raise ValueError("invalid HTTP status counts")
        expected_statuses = {key: value for key, value in (("200", outcomes["successes"]),
                                                          ("404", outcomes["logical_misses"])) if value}
        if rate:
            received = sum(statuses.values())
            if (any(re.fullmatch(r"[1-9][0-9]{2}", key) is None for key in statuses)
                    or not outcomes["requests"] - outcomes["network_errors"] <= received <= outcomes["requests"]
                    or statuses.get("200", 0) < outcomes["successes"]
                    or statuses.get("404", 0) < outcomes["logical_misses"]
                    or received - statuses.get("200", 0) < outcomes["logical_misses"] + outcomes["http_failures"]):
                raise ValueError("HTTP status counts contradict attempted outcomes")
        elif {key: value for key, value in statuses.items() if value} != expected_statuses:
            raise ValueError("HTTP status counts contradict successful outcomes")
        if outcomes["logical_misses"] > operations["get"] + operations["delete"]:
            raise ValueError("PUT cannot produce a logical miss")
        timestamp_ns(report["measurement_started_at"])
        return report
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        raise ExperimentError(f"invalid benchmark report: {error}") from error


def timestamp_ns(value):
    # Parse Go's variable 1..9 fractional digits explicitly. Older Python
    # fromisoformat versions only accept 3/6 digits; float nanoseconds also
    # lose boundary precision for present-day timestamps.
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})", value)
    if match is None:
        raise ValueError("timestamp must be RFC3339 with at most nine fractional digits")
    date = datetime.fromisoformat(match[1] + match[3].replace("Z", "+00:00"))
    return int(date.timestamp()) * 1000000000 + int((match[2] or "0").ljust(9, "0"))


def observations(directory, report, interval):
    start = timestamp_ns(report["measurement_started_at"])
    end = start + report["elapsed_ns"]
    samples = [json.loads(line) for line in (directory / "stats.jsonl").read_text().splitlines()]
    inside = [sample for sample in samples if sample["error"] is None
              and timestamp_ns(sample["started_at"]) >= start and timestamp_ns(sample["finished_at"]) <= end]
    completed = None
    if len(inside) >= 2:
        completed = inside[-1]["body"]["engine"]["snapshot_successes_total"] - inside[0]["body"]["engine"]["snapshot_successes_total"]
    in_progress = any(sample["body"]["engine"].get("snapshot_in_progress") is True for sample in inside)
    resources = [json.loads(line) for line in (directory / "resources.jsonl").read_text().splitlines()]
    memory = {}
    for name in ("engine", "gateway", "bench"):
        all_hwm = [sample["processes"][name]["hwm_bytes"] for sample in resources
                   if name in sample["processes"] and sample["processes"][name]["hwm_bytes"] is not None]
        rss = [sample["processes"][name]["rss_bytes"] for sample in resources
               if name in sample["processes"] and sample["processes"][name]["rss_bytes"] is not None
               and start <= timestamp_ns(sample["processes"][name]["sampled_at_utc"]) <= end]
        memory[name] = {"observed_lifetime_hwm_bytes": max(all_hwm, default=None),
                        "measurement_sampled_rss_max_bytes": max(rss, default=None), "measurement_rss_samples": len(rss)}
    return {"alignment": "wall_clock_approximate", "stats_samples": len(samples),
            "stats_errors": sum(sample["error"] is not None for sample in samples),
            "measurement_stats_samples": len(inside), "measurement_observed_snapshot_completions": completed,
            "measurement_snapshot_in_progress_observed": in_progress,
            "snapshot_activity_observed": bool((completed or 0) > 0 or in_progress) if interval else None,
            "memory": memory}


def run_case(args, name, mode, interval):
    directory = args.output / name
    directory.mkdir()
    (directory / "data").mkdir()
    result = {"schema_version": 1, "name": name, "wal_mode": mode, "snapshot_interval_ms": interval,
              "started_at": utc_now(), "status": "running", "errors": []}
    write_json(directory / "result.json", result)
    processes, logs = {}, []
    report = None
    interrupted = False
    previous_data_rejections = 0

    def check_data_capacity(body, initial=False):
        nonlocal previous_data_rejections
        validate_data_capacity(body, args.max_data_bytes, initial)
        if args.max_data_bytes:
            current = body["engine"]["data_rejections_total"]
            if current < previous_data_rejections:
                raise ExperimentError("data capacity rejection counter decreased")
            previous_data_rejections = current

    try:
        engine_env, gateway_env, expected, command, engine_port, http_port = configuration(args, directory, mode, interval)
        base_env = runtime_environment(args)
        commands = {"cwd": str(ROOT), "runtime_environment": base_env,
                    "engine": {"argv": [str(args.engine)], "environment": engine_env},
                    "gateway": {"argv": [str(args.gateway)], "environment": gateway_env},
                    "bench": {"argv": command, "environment": {}}}
        write_json(directory / "commands.json", commands)

        def launch(role, argv, environment, stdout_name, stderr_name=None):
            stdout = (directory / stdout_name).open("wb")
            logs.append(stdout)
            stderr = stdout
            if stderr_name:
                stderr = (directory / stderr_name).open("wb")
                logs.append(stderr)
            # Keep terminal signals in the parent so it can stop services in
            # dependency order. Never find or kill processes by executable name.
            # Defer terminal signal delivery until the Popen handle is registered
            # so even a signal during process creation cannot orphan this child.
            # Defer in our Python handler, not in the OS mask: children must
            # inherit unblocked SIGTERM so their graceful shutdown still works.
            INTERRUPT_STATE["launching"] = True
            try:
                process = subprocess.Popen(argv, cwd=ROOT, env=dict(base_env, **environment),
                                           stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, start_new_session=True)
                processes[role] = process
            finally:
                INTERRUPT_STATE["launching"] = False
            if INTERRUPT_STATE["pending"] is not None:
                signum = INTERRUPT_STATE["pending"]
                INTERRUPT_STATE["pending"] = None
                raise Interrupted(f"received {signal.Signals(signum).name}")
            result.setdefault("pids", {})[role] = process.pid
            write_json(directory / "result.json", result)

        launch("engine", [str(args.engine)], engine_env, "engine.log")
        wait_ready(processes, engine_port, None, args.startup_timeout)
        launch("gateway", [str(args.gateway)], gateway_env, "gateway.log")
        before = wait_ready(processes, engine_port, http_port, args.startup_timeout)
        write_json(directory / "stats-before.json", before["body"])
        check_data_capacity(before["body"], initial=True)
        engine = before["body"]["engine"]
        if engine["keys"] != 0 or engine["applied_sequence"] != 0 or engine.get("wal_mode") != mode:
            raise ExperimentError("startup stats do not describe the requested empty database and WAL mode")
        if (before["body"]["server"].get("workers_capacity") != args.engine_workers
                or before["body"]["gateway"].get("rpc", {}).get("pool_capacity") != args.rpc_pool):
            raise ExperimentError("startup stats do not match engine workers or RPC pool")

        with (directory / "resources.jsonl").open("w") as resources, (directory / "stats.jsonl").open("w") as stats:
            append_json(stats, before)
            launch("bench", command, {}, "report.json", "benchmark.stderr.log")
            deadline = time.monotonic() + args.run_timeout
            next_resource = next_stats = time.monotonic()
            while processes["bench"].poll() is None:
                check_alive(processes)
                now = time.monotonic()
                if now >= deadline:
                    raise ExperimentError("benchmark run timeout (includes preload and report generation)")
                if args.sample_ms and now >= next_resource:
                    append_json(resources, {"sampled_at": utc_now(), "monotonic_ns": time.monotonic_ns(),
                                            "processes": {role: sample_process(process.pid) for role, process in processes.items()}})
                    next_resource = time.monotonic() + args.sample_ms / 1000
                if args.stats_ms and now >= next_stats:
                    sample = fetch_stats(http_port, min(1, max(.001, deadline - time.monotonic())))
                    append_json(stats, sample)
                    if sample["http_status"] == 200:
                        check_data_capacity(sample["body"])
                    next_stats = time.monotonic() + args.stats_ms / 1000
                time.sleep(.01)
            processes["bench"].wait()
            after = fetch_stats(http_port)
            append_json(stats, after)
            write_json(directory / "stats-after.json", after["body"])
            if after["error"]:
                raise ExperimentError("post-benchmark stats: " + after["error"])
            check_data_capacity(after["body"])
            check_alive(processes)
            report = read_report(directory / "report.json", expected)
            if processes["bench"].returncode != benchmark_exit_code(report):
                raise ExperimentError(f"benchmark exit {processes['bench'].returncode} contradicts its report")
            # Throughput acknowledgements may leave a pending tail. Preserve
            # immediate and drained states without adding drain time to QPS.
            target = after["body"]["engine"]["applied_sequence"]
            settle_start = time.monotonic()
            settled = after
            quiet_observations = 0
            if args.rate:
                result["wal_drain_started_monotonic_ns"] = after["monotonic_start_ns"]
            def drained():
                if args.rate:
                    return fixed_arrival_settled(settled["body"])
                return (settled["body"]["engine"]["durable_sequence"] >= target
                        and settled["body"]["engine"]["wal_pending_bytes"] == 0)

            while True:
                quiet_observations = quiet_observations + 1 if drained() else 0
                # Engine and in-flight counters are sampled separately. A
                # second quiet query observes engine state after the first
                # query saw all admitted work finish.
                if quiet_observations >= (2 if args.rate else 1):
                    break
                check_alive(processes)
                remaining = args.settle_timeout - (time.monotonic() - settle_start)
                if remaining <= 0:
                    raise ExperimentError("WAL drain timeout")
                time.sleep(min(.02, remaining))
                remaining = args.settle_timeout - (time.monotonic() - settle_start)
                if remaining <= 0:
                    raise ExperimentError("WAL drain timeout")
                settled = fetch_stats(http_port, min(1, remaining))
                append_json(stats, settled)
                if settled["error"]:
                    raise ExperimentError("WAL drain stats: " + settled["error"])
                check_data_capacity(settled["body"])
            result["wal_drain_elapsed_ns"] = int((time.monotonic() - settle_start) * 1000000000)
            if args.rate:
                validate_fixed_completion(after["body"], settled["body"], report, mode)
            write_json(directory / "stats-settled.json", settled["body"])
            validate_data_rejections(after["body"], settled["body"], report, args.max_data_bytes)
        result["observations"] = observations(directory, report, interval)
        result["metrics"] = {"qps_successful": report["qps_successful"], "p99_ns": report["latency_ns"]["p99"],
                             "failures": report["outcomes"]["failures"], "logical_misses": report["outcomes"]["logical_misses"]}
        if args.rate:
            result["metrics"].update(arrival_metrics(report))
    except Interrupted as error:
        interrupted = True
        result["errors"].append(str(error))
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        # Record even a first signal during normal shutdown. Defer raising so
        # it cannot interrupt reaping, then stop the matrix after this run.
        cleanup_signals = []

        def defer_cleanup_signal(signum, frame):
            cleanup_signals.append(signum)

        previous = {sig: signal.signal(sig, defer_cleanup_signal) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            result["processes"] = stop_processes(processes, args.shutdown_timeout)
            for role, state in result["processes"].items():
                expected_exit = benchmark_exit_code(report) if role == "bench" and report is not None else 0
                if state["forced"] or state["returncode"] != expected_exit:
                    result["errors"].append(f"{role} shutdown: exit={state['returncode']}, forced={state['forced']}")
            for log in logs:
                log.close()
            completed_status = "degraded" if report is not None and report_degraded(report) else "ok"
            result.update(status="interrupted" if interrupted else ("failed" if result["errors"] else completed_status), finished_at=utc_now())
            write_json(directory / "result.json", result)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
        if cleanup_signals:
            result["status"] = "interrupted"
            result["errors"].append(f"received {signal.Signals(cleanup_signals[0]).name} during cleanup")
            write_json(directory / "result.json", result)
    return result


def main(argv=None):
    args = parse_args(argv)
    results = []
    index = {"schema_version": 1, "status": "running", "runs": results}
    exit_code = 0

    def interrupted(signum, frame):
        if INTERRUPT_STATE["launching"]:
            INTERRUPT_STATE["pending"] = signum
            return
        raise Interrupted(f"received {signal.Signals(signum).name}")

    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        cases = [(mode, interval) for mode in args.modes for interval in (0, args.snapshot_ms)]
        plan = []
        for repeat in range(args.repeats):
            # Rotate the order between repeats to expose rather than lock in
            # first/last-run bias. This does not eliminate host/cache effects.
            offset = repeat % len(cases)
            for mode, interval in cases[offset:] + cases[:offset]:
                name = f"r{repeat + 1:02d}-{mode}-snapshot-{'on' if interval else 'off'}"
                plan.append({"name": name, "wal_mode": mode, "snapshot_interval_ms": interval})
        manifest = {"schema_version": 1, "started_at": utc_now(), "arguments": {key: str(value) if isinstance(value, Path) else value
                     for key, value in vars(args).items() if key not in ("rate", "max_data_bytes") or value}, "plan": plan, "runtime_environment": runtime_environment(args),
                    "metadata": collect_metadata(ROOT, {name: getattr(args, name) for name in ("engine", "gateway", "bench")})}
        write_json(args.output / "manifest.json", manifest)
        write_json(args.output / "index.json", index)
        # Keep the selected executables with the artifacts. A concurrent `make`
        # can replace the original paths without changing later repetitions.
        executable_directory = args.output / "binaries"
        executable_directory.mkdir()
        manifest["executables"] = {}
        for name in ("engine", "gateway", "bench"):
            identity = manifest["metadata"]["binaries"][name]
            if not identity["available"]:
                raise ExperimentError(f"cannot fingerprint {name}: {identity['errors']}")
            destination = executable_directory / name
            shutil.copyfile(getattr(args, name), destination)
            digest = hashlib.sha256()
            with destination.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != identity["sha256"]:
                raise ExperimentError(f"{name} changed while copying; start a new experiment")
            destination.chmod(0o555)
            setattr(args, name, destination)
            manifest["executables"][name] = {"path": str(destination), "sha256": digest.hexdigest()}
        write_json(args.output / "manifest.json", manifest)
        print(f"Artifacts: {args.output}", flush=True)
        for case in plan:
            print(f"Running {case['name']} ...", flush=True)
            result = run_case(args, case["name"], case["wal_mode"], case["snapshot_interval_ms"])
            results.append(result)
            write_json(args.output / "index.json", index)
            if result["status"] in ("ok", "degraded"):
                metrics = result["metrics"]
                p99 = f"{metrics['p99_ns'] / 1000000:.3f} ms" if not args.rate or metrics["arrival_started"] else "unavailable"
                print(f"  QPS successful={metrics['qps_successful']:.1f}, P99={p99}", flush=True)
                if args.rate:
                    print(f"  Offered={args.rate}/s, attempted={metrics['arrival_started']}/{metrics['arrival_planned']}, "
                          f"dropped busy/late={metrics['dropped_busy']}/{metrics['dropped_late']}, failures={metrics['failures']}, "
                          f"offered success={metrics['offered_success_rate_pct']:.3f}%", flush=True)
                if result["status"] == "degraded":
                    exit_code = 1
                if case["snapshot_interval_ms"] and not result["observations"]["snapshot_activity_observed"]:
                    if args.stats_ms == 0:
                        print("  Periodic status sampling is disabled; measurement-window snapshot evidence is unavailable.", flush=True)
                    else:
                        print("  No snapshot activity observed inside measurement; extend the run before comparing snapshot cost.", flush=True)
            else:
                exit_code = 1
                print("  " + "; ".join(result["errors"]), file=sys.stderr, flush=True)
                if result["status"] == "interrupted":
                    break
        index["status"] = "complete" if len(results) == len(plan) else "interrupted"
    except (Interrupted, OSError, ValueError, ExperimentError) as error:
        index.update(status="interrupted" if isinstance(error, Interrupted) else "failed", error=str(error))
        exit_code = 1
        print(str(error), file=sys.stderr)
    finally:
        index.update(finished_at=utc_now(), successful_runs=sum(result["status"] == "ok" for result in results),
                     failed_runs=sum(result["status"] not in (("ok", "degraded") if args.rate else ("ok",)) for result in results))
        if args.rate:
            index["degraded_runs"] = sum(result["status"] == "degraded" for result in results)
        try:
            write_json(args.output / "index.json", index)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
