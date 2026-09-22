#!/usr/bin/env python3
"""Summarize recorded MiniKV experiments without modifying or rerunning them."""

import argparse
from collections import deque
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import sys

from experiment import (ExperimentError, arrival_metrics, benchmark_exit_code, fixed_arrival_settled, read_report, report_degraded,
                        timestamp_ns, validate_arrival_config, validate_data_capacity, validate_data_rejections,
                        validate_fixed_completion, validate_stats)


ROLES = ("engine", "gateway", "bench")
MEMORY_FIELDS = ("measurement_sampled_rss_max_bytes", "observed_lifetime_hwm_bytes")
STATUSES = ("valid", "degraded", "failed", "interrupted", "missing", "incomplete", "invalid")
MEASURED = ("valid", "degraded")
FIXED_METRICS = ("rate", "arrival_planned", "arrival_started", "dropped_busy", "dropped_late", "failures",
                 "offered_success_rate_pct", "dispatch_p99_ms", "scheduled_p99_ms")
UNITS = {"qps_successful": "requests/s", "p99_ms": "ms", "memory": "bytes"}


class InvalidArtifact(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidArtifact(message)


def reject_constant(value):
    raise InvalidArtifact("nonfinite JSON number: " + value)


def finite_float(value):
    number = float(value)
    require(math.isfinite(number), "nonfinite JSON number: " + value)
    return number


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_constant, parse_float=finite_float)


def safe_path(root, name):
    require(isinstance(name, str) and name and "\\" not in name, "invalid relative artifact path")
    relative = Path(name)
    require(not relative.is_absolute() and all(part not in ("", ".", "..") for part in name.split("/")),
            "artifact path must be relative and cannot traverse parent directories")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        raise InvalidArtifact("artifact path escapes the experiment directory")
    return path


def integer(value, minimum=0, maximum=None):
    return type(value) is int and value >= minimum and (maximum is None or value <= maximum)


def manifest_for(directory):
    root = Path(directory).resolve()
    require(root.is_dir(), "not an experiment directory: " + str(root))
    manifest = load_json(safe_path(root, "manifest.json"))
    require(isinstance(manifest, dict) and type(manifest.get("schema_version")) is int
            and manifest["schema_version"] == 1, "unsupported manifest schema")
    args, plan = manifest.get("arguments"), manifest.get("plan")
    require(isinstance(args, dict) and isinstance(plan, list) and plan, "manifest requires arguments and a nonempty plan")
    limits = {"workers": (1, (2**63 - 1) // 4), "requests": (1, (2**63 - 1) // 8),
              "keyspace": (1, 2**63 - 1), "write_ratio": (0, 100), "delete_ratio": (0, 100),
              "value_size": (0, 1048576), "seed": (-(2**63), 2**63 - 1), "engine_workers": (1, 1024),
              "rpc_pool": (1, 65534), "gomaxprocs": (1, 1024), "wal_batch": (1, 65536),
              "wal_flush_ms": (1, 60000), "snapshot_ms": (1, 86400000),
              "repeats": (1, 1000), "sample_ms": (0, 60000), "stats_ms": (0, 60000)}
    for name, bounds in limits.items():
        require(integer(args.get(name), *bounds), "invalid manifest argument: " + name)
    validate_arrival_config(args.get("rate", 0), args["requests"], (1 << 63) - 1)
    require(integer(args.get("max_data_bytes", 0), maximum=(1 << 64) - 1),
            "invalid manifest argument: max_data_bytes")
    for name in ("run_timeout", "startup_timeout", "shutdown_timeout", "settle_timeout"):
        value = args.get(name)
        require(type(value) in (int, float) and 0 < value <= sys.float_info.max, "invalid manifest timeout: " + name)
    require(args.get("op") in ("put", "get", "delete", "mixed")
            and args["write_ratio"] + args["delete_ratio"] <= 100, "invalid manifest workload")
    require(isinstance(args.get("output"), str) and Path(args["output"]).is_absolute(), "manifest output must be an absolute path")
    modes = args.get("modes")
    require(isinstance(modes, list) and modes and all(mode in ("throughput", "reliable") for mode in modes)
            and len(set(modes)) == len(modes), "invalid manifest modes")
    runtime = manifest.get("runtime_environment")
    require(isinstance(runtime, dict) and all(isinstance(key, str) and isinstance(value, str)
                                            for key, value in runtime.items()), "invalid runtime environment")
    require(runtime.get("GOMAXPROCS") == str(args["gomaxprocs"]), "runtime GOMAXPROCS differs from arguments")
    executables = manifest.get("executables")
    metadata = manifest.get("metadata")
    require(isinstance(executables, dict) and isinstance(metadata, dict)
            and isinstance(metadata.get("binaries"), dict), "missing executable provenance")
    for role in ROLES:
        binary, source = executables.get(role), metadata["binaries"].get(role)
        require(isinstance(binary, dict) and isinstance(binary.get("path"), str)
                and Path(binary["path"]).is_absolute() and isinstance(binary.get("sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", binary["sha256"]), "invalid executable identity: " + role)
        require(isinstance(source, dict) and source.get("available") is True
                and source.get("sha256") == binary["sha256"], "inconsistent executable provenance: " + role)
    names = set()
    for case in plan:
        require(isinstance(case, dict), "invalid plan entry")
        name = case.get("name")
        safe_path(root, name)
        require(name not in names, "duplicate planned run: " + name)
        names.add(name)
        require(case.get("wal_mode") in modes and integer(case.get("snapshot_interval_ms"))
                and case["snapshot_interval_ms"] in (0, args["snapshot_ms"]), "invalid planned mode or snapshot interval")
    # Schema 1 records the runner's complete matrix before launching any case.
    # A truncated plan must not hide an absent run by redefining "planned".
    expected_plan = {
        ("r{:02d}-{}-snapshot-{}".format(repeat, mode, "on" if interval else "off"), mode, interval)
        for repeat in range(1, args["repeats"] + 1)
        for mode in modes
        for interval in (0, args["snapshot_ms"])
    }
    actual_plan = {(case["name"], case["wal_mode"], case["snapshot_interval_ms"]) for case in plan}
    require(actual_plan == expected_plan,
            "manifest plan differs from the complete experiment matrix (missing {}, unexpected {})".format(
                len(expected_plan - actual_plan), len(actual_plan - expected_plan)))
    return root, manifest


def verify_binaries(root, manifest):
    errors, missing = [], []
    for role in ROLES:
        try:
            path = safe_path(root, "binaries/" + role)
            if not path.exists():
                missing.append(role)
                continue
            require(path.is_file(), "copied executable is not a regular file: " + role)
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            require(digest.hexdigest() == manifest["executables"][role]["sha256"], "copied executable SHA256 mismatch: " + role)
        except (OSError, ValueError) as error:
            errors.append(str(error))
    if len(missing) == len(ROLES) and not errors:
        return [], ["Executable copies are absent; provenance uses recorded hashes only (recorded_hashes_only)."], "recorded_hashes_only"
    if missing:
        errors.append("only some executable copies are present; missing: " + ", ".join(missing))
    return errors, [], "invalid" if errors else "copies_verified"


def config_for(root, manifest, case):
    commands = load_json(safe_path(root, case["name"] + "/commands.json"))
    args = manifest["arguments"]
    require(isinstance(commands, dict) and commands.get("runtime_environment") == manifest["runtime_environment"],
            "command runtime environment differs from manifest")
    for role in ROLES:
        record = commands.get(role)
        require(isinstance(record, dict) and isinstance(record.get("argv"), list)
                and record["argv"] and record["argv"][0] == manifest["executables"][role]["path"],
                "command executable differs from manifest: " + role)
        require("sha256" not in record or record["sha256"] == manifest["executables"][role]["sha256"],
                "command SHA256 differs from manifest: " + role)
    engine, gateway = commands["engine"].get("environment"), commands["gateway"].get("environment")
    require(isinstance(engine, dict) and isinstance(gateway, dict), "missing server environment")
    engine_port = engine.get("MINIKV_ENGINE_PORT")
    address = gateway.get("MINIKV_HTTP_ADDR")
    require(isinstance(engine_port, str) and engine_port.isdigit() and 1 <= int(engine_port) <= 65535,
            "invalid engine port")
    match = re.fullmatch(r"127\.0\.0\.1:([0-9]+)", address) if isinstance(address, str) else None
    require(match is not None and 1 <= int(match.group(1)) <= 65535 and int(match.group(1)) != int(engine_port),
            "invalid loopback HTTP endpoint")
    expected_engine = {
        "MINIKV_DATA_DIR": str(Path(args["output"]) / case["name"] / "data"),
        "MINIKV_ENGINE_HOST": "127.0.0.1", "MINIKV_ENGINE_PORT": engine_port,
        "MINIKV_WAL_MODE": case["wal_mode"], "MINIKV_WAL_BATCH_SIZE": str(args["wal_batch"]),
        "MINIKV_WAL_FLUSH_MS": str(args["wal_flush_ms"]), "MINIKV_WAL_QUEUE_BYTES": "16777216",
        "MINIKV_SNAPSHOT_INTERVAL_MS": str(case["snapshot_interval_ms"]), "MINIKV_WORKERS": str(args["engine_workers"]),
        "MINIKV_REQUEST_QUEUE_SIZE": "128", "MINIKV_MAX_CONNECTIONS": str(max(256, args["rpc_pool"] + 2)),
        "MINIKV_CLIENT_IDLE_MS": "30000",
    }
    if args.get("max_data_bytes", 0):
        expected_engine["MINIKV_MAX_DATA_BYTES"] = str(args["max_data_bytes"])
    expected_gateway = {"MINIKV_ENGINE_ADDR": "127.0.0.1:" + engine_port, "MINIKV_HTTP_ADDR": address,
                        "MINIKV_RPC_POOL_SIZE": str(args["rpc_pool"]), "MINIKV_RPC_TIMEOUT_MS": "2000"}
    require(engine == expected_engine and gateway == expected_gateway, "server configuration differs from manifest plan")
    for role in ("engine", "gateway"):
        require(commands[role]["argv"] == [manifest["executables"][role]["path"]], "unexpected server arguments")
    expected = {"url": "http://" + address + "/kv", "workers": args["workers"], "requests": args["requests"],
                "operation": args["op"], "keyspace": args["keyspace"], "timeout_ns": 2000000000,
                "write_ratio": args["write_ratio"], "delete_ratio": args["delete_ratio"], "preload": True,
                "preload_count": 0, "seed": args["seed"], "value_size": args["value_size"]}
    argv = [manifest["executables"]["bench"]["path"], "-url", expected["url"], "-workers", str(args["workers"]),
            "-requests", str(args["requests"]), "-op", args["op"], "-keyspace", str(args["keyspace"]),
            "-timeout", "2s", "-write-ratio", str(args["write_ratio"]), "-delete-ratio", str(args["delete_ratio"]),
            "-preload=true", "-preload-count", "0", "-seed", str(args["seed"]),
            "-value-size", str(args["value_size"]), "-format", "json"]
    if args.get("rate", 0):
        expected["rate"] = args["rate"]
        argv.extend(("-rate", str(args["rate"])))
    require(commands["bench"]["argv"] == argv and commands["bench"].get("environment") == {},
            "benchmark command differs from manifest workload")
    return expected


def empty_memory():
    return {role: {field: None for field in MEMORY_FIELDS} for role in ROLES}


def invalidate_row(row):
    row.update(status="invalid", qps_successful=None, p99_ms=None, memory=empty_memory(), snapshot_comparison_eligible=False)
    if row.get("load_model") == "fixed_arrival":
        row.update({name: None for name in FIXED_METRICS if name != "rate"})


def validate_completion(root, manifest, case, result, report):
    max_data_bytes = manifest["arguments"].get("max_data_bytes", 0)
    processes = result.get("processes")
    require(isinstance(processes, dict), "successful result lacks process exit evidence")
    for role in ROLES:
        state = processes.get(role)
        require(isinstance(state, dict) and type(state.get("returncode")) is int
                and state["returncode"] == (benchmark_exit_code(report) if role == "bench" else 0)
                and state.get("forced") is False
                and integer(state.get("pid"), minimum=1),
                "successful result contradicts process exit: " + role)
    snapshots, bodies = {}, {}
    for phase in ("before", "after", "settled"):
        body = load_json(safe_path(root, case["name"] + "/stats-" + phase + ".json"))
        validate_stats(body)
        validate_data_capacity(body, max_data_bytes, initial=phase == "before")
        require(type(body.get("schema_version")) is int, "invalid stats schema type")
        workers, pool = body["server"].get("workers_capacity"), body["gateway"].get("rpc", {}).get("pool_capacity")
        require(body["engine"].get("wal_mode") == case["wal_mode"]
                and type(workers) is int and workers == manifest["arguments"]["engine_workers"]
                and type(pool) is int and pool == manifest["arguments"]["rpc_pool"],
                "stats configuration differs from manifest: " + phase)
        snapshots[phase] = body["engine"]
        bodies[phase] = body
    require(all(snapshots["before"][field] == 0 for field in ("keys", "applied_sequence", "durable_sequence", "wal_pending_bytes")),
            "startup stats do not describe an empty database")
    validate_data_rejections(bodies["after"], bodies["settled"], report, max_data_bytes)
    if max_data_bytes:
        validate_capacity_samples(root, case, max_data_bytes, snapshots["settled"]["data_rejections_total"])
    if report["load_model"] == "fixed_arrival":
        validate_fixed_completion(bodies["after"], bodies["settled"], report, case["wal_mode"])
        validate_quiet_confirmation(root, case, result, bodies["settled"])
        return
    # Every successful PUT/DELETE appends one record, including DELETE misses;
    # preload contributes one PUT per completed key. A drained but unrelated
    # stats file must not make a report with missing writes appear valid.
    expected_sequence = (report["preload"]["completed_keys"]
                         + report["operations"]["put"] + report["operations"]["delete"])
    require(snapshots["after"]["applied_sequence"] == expected_sequence,
            "post-benchmark sequence differs from preload and reported writes")
    require(snapshots["after"]["durable_sequence"] <= snapshots["after"]["applied_sequence"],
            "post-benchmark durable sequence exceeds applied sequence")
    if case["wal_mode"] == "reliable":
        require(snapshots["after"]["durable_sequence"] == expected_sequence
                and snapshots["after"]["wal_pending_bytes"] == 0,
                "reliable acknowledgements precede WAL durability")
    require(snapshots["settled"]["applied_sequence"] == expected_sequence
            and snapshots["settled"]["durable_sequence"] == expected_sequence
            and snapshots["settled"]["wal_pending_bytes"] == 0,
            "successful result lacks matching WAL drain evidence")


def read_samples(root, case, filename):
    path = safe_path(root, case["name"] + "/" + filename)
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            sample = json.loads(line, parse_constant=reject_constant, parse_float=finite_float)
            require(isinstance(sample, dict), "invalid sampling record")
            yield sample


def validate_capacity_samples(root, case, maximum, settled_rejections):
    # A configured limit is part of run validity, so optional observation
    # warnings must not swallow contradictory capacity evidence.
    previous = 0
    for sample in read_samples(root, case, "stats.jsonl"):
        if sample.get("http_status") != 200:
            continue
        body = sample.get("body")
        validate_data_capacity(body, maximum)
        validate_stats(body)
        rejections = body["engine"]["data_rejections_total"]
        require(previous <= rejections <= settled_rejections,
                "sampled data capacity rejections regressed or exceed settled stats")
        previous = rejections


def validate_quiet_confirmation(root, case, result, settled):
    boundary = result.get("wal_drain_started_monotonic_ns")
    require(integer(boundary, minimum=1), "missing fixed-arrival drain evidence boundary")
    samples = list(deque(read_samples(root, case, "stats.jsonl"), maxlen=2))
    require(len(samples) == 2, "fixed-arrival drain needs two quiet observations")
    previous_start, previous_end = None, boundary
    for sample in samples:
        start, end = sample.get("monotonic_start_ns"), sample.get("monotonic_end_ns")
        require(integer(start, minimum=1) and integer(end, minimum=1)
                and previous_end <= start <= end and (previous_start is None or previous_start < start),
                "fixed-arrival drain observations are not distinct and ordered after the drain boundary")
        require("error" in sample and sample["error"] is None
                and type(sample.get("http_status")) is int and sample["http_status"] == 200,
                "fixed-arrival drain observation did not succeed")
        body = sample["body"]
        validate_stats(body)
        require(fixed_arrival_settled(body), "fixed-arrival drain lacks consecutive quiet observations")
        require(body["engine"].get("wal_mode") == settled["engine"]["wal_mode"],
                "fixed-arrival drain observation has a different WAL mode")
        previous_start, previous_end = start, end
    require(samples[-1]["body"] == settled, "confirmed drain observation differs from settled stats")
    require(samples[0]["body"]["engine"]["applied_sequence"] <= settled["engine"]["applied_sequence"],
            "applied sequence regressed between quiet observations")


def observe(root, case, report, row, processes):
    start = timestamp_ns(report["measurement_started_at"])
    end = start + report["elapsed_ns"]
    # Stats and resource sampling are optional, independent evidence streams.
    # Keep one stream's observations even when the other is unavailable.
    try:
        inside, failures = [], 0
        for sample in read_samples(root, case, "stats.jsonl"):
            if sample["error"] is not None:
                failures += 1
                continue
            validate_stats(sample["body"])
            require(type(sample["http_status"]) is int and sample["http_status"] == 200, "invalid successful status sample")
            first, last = timestamp_ns(sample["started_at"]), timestamp_ns(sample["finished_at"])
            require(first <= last, "status sample ends before it starts")
            engine = sample["body"]["engine"]
            require(engine.get("wal_mode") == case["wal_mode"] and type(engine.get("snapshot_in_progress")) is bool,
                    "invalid sampled snapshot state")
            if start <= first and last <= end:
                inside.append(engine)
        counters = [engine["snapshot_successes_total"] for engine in inside]
        require(counters == sorted(counters), "sampled snapshot counter decreased")
        if case["snapshot_interval_ms"]:
            row["snapshot_activity_observed"] = bool((len(counters) >= 2 and counters[-1] > counters[0])
                                                      or any(engine["snapshot_in_progress"] for engine in inside))
        if failures:
            row["warnings"].append("{} status samples failed".format(failures))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, ExperimentError) as error:
        row["warnings"].append("snapshot sampling evidence unavailable: " + str(error))
    try:
        identities, unavailable_roles, empty_sample_warnings = {}, set(), set()
        for sample in read_samples(root, case, "resources.jsonl"):
            require(isinstance(sample["processes"], dict), "invalid sampled processes")
            for role in ROLES:
                process = sample["processes"].get(role)
                if process is None or role in unavailable_roles:
                    continue
                try:
                    # A process may exit between /proc reads. The sampler then
                    # clears every counter, including identity, and marks this
                    # sample unavailable. It contributes no new evidence and
                    # must not erase earlier, identified memory observations.
                    if (isinstance(process, dict) and process.get("available") is False
                            and process.get("rss_bytes") is None and process.get("hwm_bytes") is None):
                        if role not in empty_sample_warnings:
                            row["warnings"].append(role + " resource sample unavailable; no memory values in this sample")
                            empty_sample_warnings.add(role)
                        continue
                    expected_pid = processes[role].get("pid")
                    require(isinstance(process, dict) and integer(expected_pid, minimum=1)
                            and integer(process.get("pid"), minimum=1) and process["pid"] == expected_pid,
                            "sampled PID differs from recorded process")
                    started = process.get("starttime_ticks")
                    require(integer(started), "sampled process starttime must be a nonnegative integer")
                    require(role not in identities or identities[role] == started,
                            "sampled process starttime changed")
                    identities[role] = started
                    # available=False may only mean another counter (such as
                    # /proc/io) was unreadable; known memory values remain usable.
                    for source, field in zip(("rss_bytes", "hwm_bytes"), MEMORY_FIELDS):
                        value = process[source]
                        require(value is None or integer(value, maximum=2**64 - 1), "invalid sampled memory value")
                        if value is None or (source == "rss_bytes" and not start <= timestamp_ns(process["sampled_at_utc"]) <= end):
                            continue
                        previous = row["memory"][role][field]
                        row["memory"][role][field] = value if previous is None else max(previous, value)
                except (ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
                    # Reject this role's whole stream, including earlier values
                    # which could belong to a mixed run, but retain other roles.
                    row["memory"][role] = {field: None for field in MEMORY_FIELDS}
                    unavailable_roles.add(role)
                    row["warnings"].append(role + " resource sampling evidence unavailable: " + str(error))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        row["memory"] = empty_memory()
        row["warnings"].append("resource sampling evidence unavailable: " + str(error))


def read_run(root, manifest, case, provenance_errors):
    row = {"name": case["name"], "status": "missing", "errors": [], "warnings": [],
           "qps_successful": None, "p99_ms": None, "snapshot_activity_observed": None,
           "snapshot_comparison_eligible": False, "memory": empty_memory()}
    if manifest["arguments"].get("rate", 0):
        row.update({name: None for name in FIXED_METRICS})
        row.update(load_model="fixed_arrival", rate=manifest["arguments"]["rate"])
    try:
        result = load_json(safe_path(root, case["name"] + "/result.json"))
        require(isinstance(result, dict) and type(result.get("schema_version")) is int
                and result["schema_version"] == 1 and result.get("name") == case["name"]
                and result.get("wal_mode") == case["wal_mode"]
                and type(result.get("snapshot_interval_ms")) is int
                and result["snapshot_interval_ms"] == case["snapshot_interval_ms"], "result identity differs from planned run")
        errors = result.get("errors")
        require(isinstance(errors, list) and all(isinstance(error, str) for error in errors), "invalid result errors")
        status = result.get("status")
        require(status in ("ok", "degraded", "failed", "interrupted", "running"), "unknown run status")
        require(status != "degraded" or manifest["arguments"].get("rate", 0), "closed-loop result cannot be degraded")
        if status not in ("ok", "degraded"):
            row["status"] = "incomplete" if status == "running" else status
            row["errors"] = errors or ["recorded run status: " + status]
            return row
        require(not errors, "run claims success while recording errors")
        expected = config_for(root, manifest, case)
        require(not provenance_errors, "; ".join(provenance_errors))
        report_path = safe_path(root, case["name"] + "/report.json")
        if not report_path.exists():
            raise FileNotFoundError(str(report_path))
        report = read_report(report_path, expected)
        require((status == "degraded") == report_degraded(report), "run status contradicts measured workload losses")
        require(all(type(report["config"][key]) is type(value) for key, value in expected.items()), "report configuration type mismatch")
        validate_completion(root, manifest, case, result, report)
        row.update(status="degraded" if report_degraded(report) else "valid", qps_successful=report["qps_successful"],
                   p99_ms=report["latency_ns"]["p99"] / 1000000 if report["outcomes"]["requests"] else None,
                   workload_generator=report["workload_generator"])
        if report["load_model"] == "fixed_arrival":
            row.update(arrival_metrics(report))
        if row["status"] == "degraded":
            row["warnings"].append("Complete fixed-arrival measurement contains dropped arrivals or failed requests.")
        observe(root, case, report, row, result["processes"])
        row["snapshot_comparison_eligible"] = row["status"] == "valid" and (
            case["snapshot_interval_ms"] == 0 or row["snapshot_activity_observed"] is True)
        if row["status"] == "degraded":
            row["warnings"].append("Degraded workload is excluded from clean snapshot comparisons.")
        elif not row["snapshot_comparison_eligible"]:
            row["warnings"].append("snapshot activity was not established inside measurement; do not infer snapshot cost")
    except FileNotFoundError as error:
        if row["status"] in MEASURED:
            invalidate_row(row)
        row["errors"].append("missing artifact: " + str(error))
    except Exception as error:
        invalidate_row(row)
        row["errors"].append(str(error))
    return row


def counts(rows):
    result = {"planned": len(rows), **{status: sum(row["status"] == status for row in rows) for status in STATUSES}}
    result["unfinished"] = result["missing"] + result["incomplete"] + result["interrupted"]
    result["snapshot_evidence_insufficient"] = sum(row["status"] == "valid" and not row["snapshot_comparison_eligible"] for row in rows)
    return result


def distribution(values):
    values = [value for value in values if value is not None]
    return {"n": len(values), "min": min(values) if values else None,
            "median": statistics.median(values) if values else None, "max": max(values) if values else None}


def summarize_experiment(root, manifest):
    provenance_errors, provenance_warnings, verification = verify_binaries(root, manifest)
    groups = {}
    for case in manifest["plan"]:
        key = (case["wal_mode"], case["snapshot_interval_ms"])
        group = groups.setdefault(key, {"wal_mode": key[0], "snapshot_interval_ms": key[1], "runs": []})
        group["runs"].append(read_run(root, manifest, case, provenance_errors))
    rows = [row for group in groups.values() for row in group["runs"]]
    generators = {row["workload_generator"] for row in rows if row["status"] in MEASURED}
    if len(generators) > 1:
        for row in rows:
            if row["status"] in MEASURED:
                invalidate_row(row)
                row["errors"].append("workload generator differs across runs using the same recorded binary")
    for group in groups.values():
        valid = [row for row in group["runs"] if row["status"] in MEASURED]
        group["counts"] = counts(group["runs"])
        metrics = ("qps_successful", "p99_ms")
        if manifest["arguments"].get("rate", 0):
            group.update(load_model="fixed_arrival", rate=manifest["arguments"]["rate"])
            metrics += FIXED_METRICS
        group["distributions"] = {name: distribution([row[name] for row in valid]) for name in metrics}
        group["distributions"]["memory"] = {
            role: {field: distribution([row["memory"][role][field] for row in valid]) for field in MEMORY_FIELDS}
            for role in ROLES}
    return {"path": str(root), "arguments": manifest["arguments"], "errors": provenance_errors,
            "warnings": provenance_warnings, "binary_verification": verification,
            "binary_sha256": {role: manifest["executables"][role]["sha256"] for role in ROLES},
            "counts": counts(rows), "groups": list(groups.values())}


def metric_values(row):
    for name in ("qps_successful", "p99_ms") + (FIXED_METRICS if row.get("load_model") == "fixed_arrival" else ()):
        yield name, row[name]
    for role in ROLES:
        for field in MEMORY_FIELDS:
            yield role + "." + field, row["memory"][role][field]


def metric_distributions(group):
    for name in ("qps_successful", "p99_ms") + (FIXED_METRICS if group.get("load_model") == "fixed_arrival" else ()):
        yield name, group["distributions"][name]
    for role in ROLES:
        for field in MEMORY_FIELDS:
            yield role + "." + field, group["distributions"]["memory"][role][field]


def write_csv(summary, output):
    fields = ("schema_version", "record_type", "experiment", "wal_mode", "snapshot_interval_ms", "run", "status",
              "metric", "n", "value", "min", "median", "max", "snapshot_activity_observed", "errors", "warnings")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for experiment in summary["experiments"]:
        for group in experiment["groups"]:
            base = {"schema_version": 1, "experiment": experiment["path"], "wal_mode": group["wal_mode"],
                    "snapshot_interval_ms": group["snapshot_interval_ms"]}
            for row in group["runs"]:
                metrics = list(metric_values(row)) if row["status"] in MEASURED else [("", None)]
                for metric, value in metrics:
                    writer.writerow(dict(base, record_type="run", run=row["name"], status=row["status"],
                                         metric=metric, value=value, snapshot_activity_observed=row["snapshot_activity_observed"],
                                         errors="; ".join(row["errors"]), warnings="; ".join(experiment["warnings"] + row["warnings"])))
            for metric, value in metric_distributions(group):
                writer.writerow(dict(base, record_type="aggregate", metric=metric, **value))


def write_text(summary, output):
    output.write("MiniKV experiment summary (schema 1)\n")
    output.write("Distributions summarize per-run values; the median of run P99s is not a pooled P99.\n")
    for experiment in summary["experiments"]:
        output.write("\n" + experiment["path"] + "\n")
        for message in experiment["errors"] + experiment["warnings"]:
            output.write("  " + message + "\n")
        for group in experiment["groups"]:
            output.write("  {} snapshot={} ms: {}\n".format(group["wal_mode"], group["snapshot_interval_ms"],
                                                          ", ".join("{}={}".format(key, value) for key, value in group["counts"].items())))
            for row in group["runs"]:
                output.write("    {} [{}]".format(row["name"], row["status"]))
                if row["status"] in MEASURED:
                    p99 = "{:.6f} ms".format(row["p99_ms"]) if row["p99_ms"] is not None else "unavailable"
                    output.write(" QPS={:.3f}, P99={}".format(row["qps_successful"], p99))
                    if row.get("load_model") == "fixed_arrival":
                        output.write("; offered={}/s attempted={}/{} dropped busy/late={}/{} failures={} offered success={:.3f}%".format(
                            row["rate"], row["arrival_started"], row["arrival_planned"], row["dropped_busy"], row["dropped_late"],
                            row["failures"], row["offered_success_rate_pct"]))
                    for role in ROLES:
                        memory = row["memory"][role]
                        output.write("; {} RSS/HWM={}/{} bytes".format(role, memory[MEMORY_FIELDS[0]], memory[MEMORY_FIELDS[1]]))
                output.write("\n")
                for message in row["errors"] + row["warnings"]:
                    output.write("      " + message + "\n")
            for metric, values in metric_distributions(group):
                output.write("    {}: n={} min={} median={} max={}\n".format(metric, values["n"], values["min"], values["median"], values["max"]))
    output.write("\nTotal: " + ", ".join("{}={}".format(key, value) for key, value in summary["counts"].items()) + "\n")


def main(argv=None):
    if sys.version_info < (3, 8):
        print("MiniKV experiment summaries require Python 3.8 or newer.", file=sys.stderr)
        return 2
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument("--format", choices=("text", "json", "csv"), default="text")
    args = parser.parse_args(argv)
    try:
        sources = [manifest_for(directory) for directory in args.directories]
        require(len({root for root, _ in sources}) == len(sources), "duplicate experiment directory")
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        parser.error(str(error))
    experiments = [summarize_experiment(root, manifest) for root, manifest in sources]
    totals = {key: sum(experiment["counts"][key] for experiment in experiments) for key in experiments[0]["counts"]}
    units = dict(UNITS)
    if any(experiment["arguments"].get("rate", 0) for experiment in experiments):
        units.update(rate="requests/s", offered_success_rate_pct="percent", dispatch_p99_ms="ms", scheduled_p99_ms="ms")
    summary = {"schema_version": 1, "units": units, "counts": totals, "experiments": experiments}
    try:
        if args.format == "json":
            json.dump(summary, sys.stdout, ensure_ascii=False, indent=2, allow_nan=False)
            sys.stdout.write("\n")
        elif args.format == "csv":
            write_csv(summary, sys.stdout)
        else:
            write_text(summary, sys.stdout)
        sys.stdout.flush()
    except (OSError, ValueError) as error:
        print("Cannot write summary: " + str(error), file=sys.stderr)
        return 1
    if totals["valid"] != totals["planned"]:
        print("Some planned runs are degraded, failed, missing, interrupted, incomplete, or invalid; see the summary.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
