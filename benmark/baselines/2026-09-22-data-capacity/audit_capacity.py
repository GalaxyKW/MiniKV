#!/usr/bin/env python3
"""Recompute the frozen capacity comparison from portable, read-only raw artifacts.

Only analysis.json and analysis.csv are replaced. Recorded absolute paths are
compared as provenance strings, never opened. No loads, subprocesses or Git calls.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import sys


ROLES = ("engine", "gateway", "bench")
COLLECTOR_FILES = ("benmark/experiment.py", "benmark/summarize.py",
                   "benmark/experiment_support.py", "benmark/snapshot_report.py")
DEFAULTS = {"requests": 20000, "workers": 20, "keyspace": 1000, "op": "mixed",
            "write_ratio": 20, "delete_ratio": 5, "value_size": 128, "seed": 1,
            "engine_workers": 4, "rpc_pool": 32, "gomaxprocs": 4, "wal_batch": 64,
            "wal_flush_ms": 2, "snapshot_ms": 1000, "sample_ms": 100, "stats_ms": 250,
            "run_timeout": 120.0, "startup_timeout": 10.0, "shutdown_timeout": 15.0,
            "settle_timeout": 10.0, "rate": 0}
FORMAL = {"requests": 500000, "workers": 40, "keyspace": 5000, "op": "put",
          "value_size": 16, "seed": 1, "engine_workers": 20, "rpc_pool": 64,
          "gomaxprocs": 4, "wal_batch": 64, "wal_flush_ms": 2, "snapshot_ms": 1000,
          "sample_ms": 100, "stats_ms": 250, "rate": 0}
DATA_FIELDS = ("keys", "data_capacity_bytes", "data_bytes", "data_rejections_total",
               "applied_sequence", "durable_sequence", "wal_pending_bytes")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def uint(value):
    return type(value) is int and 0 <= value < 2**64


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def load(path):
    def constant(value):
        raise ValueError("nonfinite JSON number: " + value)
    def floating(value):
        result = float(value)
        require(math.isfinite(result), "nonfinite JSON float")
        return result
    value = json.loads(path.read_text(), parse_constant=constant, parse_float=floating)
    return value


def relative(root, name):
    require(isinstance(name, str) and name and "\\" not in name, "invalid artifact path")
    path = Path(name)
    require(not path.is_absolute() and ".." not in path.parts, "artifact path must be relative")
    target = (root / path).resolve()
    require(target != root and root in target.parents, "artifact path escapes archive")
    return target


def common_arguments(plan):
    result = dict(DEFAULTS)
    arguments = plan["common_arguments"]
    require(isinstance(arguments, list) and len(arguments) % 2 == 0, "invalid common arguments")
    seen = set()
    for flag, raw in zip(arguments[::2], arguments[1::2]):
        require(isinstance(flag, str) and flag.startswith("--") and isinstance(raw, str), "invalid argument pair")
        key = flag[2:].replace("-", "_")
        require(key in result and key not in seen, "unknown/duplicate common argument: " + flag)
        seen.add(key)
        result[key] = raw if key == "op" else float(raw) if key.endswith("timeout") else int(raw)
    require(all(result[key] == value for key, value in FORMAL.items()), "plan differs from formal workload")
    return result


def validate_plan(root, plan):
    require(plan.get("schema_version") == 1, "unsupported plan schema")
    require(re.fullmatch("[0-9a-f]{40}", plan.get("collector_revision", "")), "collector revision is not frozen")
    require(set(plan["arms"]) == {"A", "B", "C"}, "plan requires exactly A/B/C")
    for arm, engine, capacity in (("A", "engine-A", 0), ("B", "engine-B", 0), ("C", "engine-B", 131072)):
        require(plan["arms"][arm]["engine"] == engine and type(plan["arms"][arm]["max_data_bytes"]) is int
                and plan["arms"][arm]["max_data_bytes"] == capacity, "unexpected arm: " + arm)
    stages = plan["stages"]
    require(len(stages) == 12, "formal plan must retain all twelve stages")
    names = set()
    for number, stage in enumerate(stages, 1):
        relative(root, stage["directory"])
        require(stage["directory"] not in names, "duplicate stage directory")
        names.add(stage["directory"])
        block = (number - 1) // 3 + 1
        modes = ["throughput", "reliable"] if block % 2 else ["reliable", "throughput"]
        require(stage["block"] == block and stage["modes"] == modes, "stage block/mode order differs from protocol")
    require([stage["arm"] for stage in stages] == list("ABC" + "CBA" + "BCA" + "ACB"), "arm order differs from protocol")
    for key in ("engine-A", "engine-B", "gateway", "bench"):
        require(re.fullmatch("[0-9a-f]{64}", plan["binaries"][key].get("sha256", "")), "invalid binary digest: " + key)
    coverage = plan["coverage"]
    for key, value in {"minimum_elapsed_seconds": 10, "minimum_internal_idle_stats": 2,
                       "minimum_on_snapshot_completions": 5, "off_snapshot_completions": 0,
                       "requests": 500000, "final_keys": 5000, "final_data_bytes": 103890,
                       "maximum_possible_data_bytes": 105000, "data_rejections": 0,
                       "failures": 0, "settled_pending_bytes": 0}.items():
        require(coverage.get(key) == value, "unexpected frozen coverage threshold: " + key)
    comparisons = plan["comparisons"]
    require(comparisons["contrasts"] == ["B/A", "C/B"] and comparisons["required_eligible_pairs"] == 4,
            "unexpected comparison matrix")
    require(comparisons["qps_investigation"] == {"ratio_below": .95, "minimum_pairs": 3}, "unexpected QPS threshold")
    for key in ("p99_investigation", "p999_investigation"):
        require(comparisons[key] == {"ratio_above": 1.05, "minimum_pairs": 3}, "unexpected latency threshold")
    require(comparisons["engine_rss_diagnostic"] == {"ratio_above": 1.05, "increase_bytes_above": 1048576,
                                                    "minimum_pairs": 3}, "unexpected RSS threshold")
    return common_arguments(plan)


def collector_identity(root, repository, plan):
    errors, hashes = [], {}
    try:
        expected = plan["collector_files_sha256"]
        require(set(expected) == set(COLLECTOR_FILES), "collector module hashes are incomplete")
        for name in COLLECTOR_FILES:
            hashes[name] = digest(relative(repository, name))
            require(hashes[name] == expected[name], "analysis collector bytes differ: " + name)
        require(digest(root / "run_plan.py") == plan["driver_sha256"], "execution driver hash differs")
    except Exception as error:
        errors.append(str(error))
    return {"errors": errors, "actual_files_sha256": hashes}


def execution_evidence(root, plan, timestamp):
    result = {"errors": [], "warnings": [], "complete": False, "recorded_status": None,
              "stages": {stage["directory"]: {"errors": [], "warnings": [], "record": None,
                                                "state": "missing"} for stage in plan["stages"]}}
    try:
        execution = load(root / "execution.json")
        result["recorded_status"] = execution.get("status")
        require(execution["plan_sha256"] == digest(root / "plan.json"), "execution used a different plan hash")
        records = execution["stages"]
        require(isinstance(records, list) and len(records) <= len(plan["stages"]), "unexpected execution stage count")
        previous_end = timestamp(execution["started_at"])
        for position, record in enumerate(records):
            stage = plan["stages"][position]
            require(all(record.get(key) == stage[key] for key in ("directory", "block", "arm")),
                    "execution stage order/identity differs at position " + str(position + 1))
            item = result["stages"][stage["directory"]]
            item["record"] = record
            try:
                first = timestamp(record["started_at"])
                require(previous_end is not None and previous_end <= first,
                        "execution stage overlaps an earlier or unfinished stage")
                if record.get("finished_at") is None:
                    require(record.get("returncode") is None, "stage has exit status without a finish timestamp")
                    item["state"] = "incomplete"
                    item["warnings"].append("stage has no final exit/timestamp evidence")
                    previous_end = None
                else:
                    last = timestamp(record["finished_at"])
                    require(first <= last and type(record.get("returncode")) is int,
                            "invalid completed stage timestamp/exit status")
                    previous_end = last
                    item["state"] = "complete" if record["returncode"] == 0 else "failed"
                    if record["returncode"] != 0:
                        item["warnings"].append("stage recorded nonzero exit; inspect individual round outcomes")
            except Exception as error:
                item["errors"].append(str(error))
                result["errors"].append(stage["directory"] + ": " + str(error))
        for stage in plan["stages"][len(records):]:
            result["stages"][stage["directory"]]["warnings"].append("stage was not executed")
        if execution["status"] == "complete":
            require(len(records) == 12 and previous_end is not None, "complete execution lacks finished stages")
            require(previous_end <= timestamp(execution["finished_at"]), "execution finish precedes final stage")
            result["complete"] = True
        else:
            require(execution["status"] in ("running", "failed", "interrupted"), "unknown execution status")
            result["warnings"].append("execution matrix is incomplete; recorded per-round outcomes remain distinct")
    except Exception as error:
        result["errors"].append(str(error))
    return result


def stage_order_evidence(directory, manifest, cases, record, timestamp):
    """Validate the observed prefix without requiring nonexistent final records.

    A process interrupted after a completed round cannot retroactively invalidate
    that round. Missing endings are reported separately from contradictory order.
    """
    warnings = []
    require(record is not None, "missing execution record")
    stage_first = timestamp(record["started_at"])
    stage_last = timestamp(record["finished_at"]) if record.get("finished_at") else None
    manifest_start = timestamp(manifest["started_at"])
    require(stage_first <= manifest_start and (stage_last is None or manifest_start <= stage_last),
            "manifest start is outside execution stage")
    index = load(directory / "index.json")
    recorded = index["runs"]
    require(isinstance(recorded, list) and len(recorded) <= len(cases), "unexpected stage index length")
    require([item["name"] for item in recorded] == [case["name"] for case in cases[:len(recorded)]],
            "stage index order differs from planned prefix")
    if index["status"] == "complete":
        require(len(recorded) == len(cases), "complete stage index omits planned rounds")
    else:
        require(index["status"] in ("running", "failed", "interrupted"), "unknown stage index status")
        warnings.append("stage index is incomplete; completed round records are retained")
    index_finish = timestamp(index["finished_at"]) if index.get("finished_at") else None
    if index_finish is not None:
        require(manifest_start <= index_finish and (stage_last is None or index_finish <= stage_last),
                "stage index finish is outside execution stage")
    previous = manifest_start
    missing_or_running = False
    for number, case in enumerate(cases):
        path = relative(directory, case["name"] + "/result.json")
        if not path.exists():
            missing_or_running = True
            continue
        require(not missing_or_running, "a later round started after a missing or unfinished round")
        result = load(path)
        require(result["name"] == case["name"], "result identity differs from plan")
        first = timestamp(result["started_at"])
        require(previous <= first, "round timestamps overlap or regress")
        if number < len(recorded):
            require(recorded[number] == result, "stage index differs from its recorded round result")
        if result.get("finished_at") is None:
            require(result.get("status") == "running", "terminal round lacks a finish timestamp")
            missing_or_running = True
            warnings.append(case["name"] + " lacks final round evidence")
            continue
        last = timestamp(result["finished_at"])
        require(first <= last and (stage_last is None or last <= stage_last)
                and (index_finish is None or last <= index_finish), "round ends outside stage/index")
        previous = last
    return warnings


def check_git(metadata, revision):
    git = metadata["git"]
    require(git.get("available") is True and git.get("head") == revision and git.get("dirty") is False
            and git.get("tracked_dirty") is False and not git.get("untracked_files") and not git.get("errors"),
            "collector metadata is dirty, unavailable, or a different revision")


def check_stage(root, plan, stage, manifest, common, execution):
    arguments = manifest["arguments"]
    arm = plan["arms"][stage["arm"]]
    recorded_root = Path(plan["binaries"]["engine-A"]["path"]).parent.parent
    require(arguments["output"] == str(recorded_root / stage["directory"]),
            "recorded output path does not identify this planned stage")
    for key, value in common.items():
        require(arguments.get(key, 0 if key == "rate" else None) == value, "manifest argument differs: " + key)
    require(arguments["modes"] == stage["modes"] and arguments["repeats"] == 1
            and arguments.get("max_data_bytes", 0) == arm["max_data_bytes"], "manifest arm or mode differs")
    expected_cases = [{"name": "r01-{}-snapshot-{}".format(mode, "on" if interval else "off"),
                       "wal_mode": mode, "snapshot_interval_ms": interval}
                      for mode in stage["modes"] for interval in (0, common["snapshot_ms"])]
    require(manifest["plan"] == expected_cases, "manifest run order differs")
    runtime = {"PATH": "/bin:/usr/bin", "LANG": "C", "LC_ALL": "C", "TZ": "UTC", "GOMAXPROCS": "4",
               "GOGC": "100", "GOMEMLIMIT": "off", "GODEBUG": ""}
    require(manifest["runtime_environment"] == runtime, "runtime environment differs")
    check_git(manifest["metadata"], plan["collector_revision"])
    for role in ROLES:
        binary = plan["binaries"][arm["engine"] if role == "engine" else role]
        require(arguments[role] == binary["path"], "selected source binary path differs: " + role)
        require(manifest["executables"][role]["path"] == str(Path(arguments["output"]) / "binaries" / role),
                "copied executable path differs from stage: " + role)
        require(manifest["executables"][role]["sha256"] == binary["sha256"], "wrong executable hash: " + role)
        require(manifest["metadata"]["binaries"][role]["sha256"] == binary["sha256"], "wrong source binary hash: " + role)
    host = load(relative(root, stage["directory"] + "-host-before.json"))
    check_git(host, plan["collector_revision"])
    for role in ROLES:
        require(host["binaries"][role]["sha256"] == manifest["executables"][role]["sha256"], "host binary hash differs")
    require(execution is not None, "missing execution record")
    command = execution["argv"]
    require(isinstance(command, list) and len(command) >= 2 and command[1].endswith("/benmark/experiment.py"), "wrong collector command")
    expected = ["--output", arguments["output"], "--modes", ",".join(stage["modes"]), "--repeats", "1"]
    for role in ROLES:
        expected.extend(["--" + role, plan["binaries"][arm["engine"] if role == "engine" else role]["path"]])
    expected += plan["common_arguments"]
    if arm["max_data_bytes"]:
        expected += ["--max-data-bytes", str(arm["max_data_bytes"])]
    require(command[2:] == expected, "execution arguments differ from frozen plan")


def check_data(engine, arm, phase, coverage):
    for field in DATA_FIELDS:
        if field.startswith("data_") and arm == "A" and field not in engine:
            continue
        require(uint(engine.get(field)), "invalid {} {}".format(phase, field))
    if phase in ("after", "settled"):
        require(engine["keys"] == coverage["final_keys"], phase + " final keys differ")
    if arm != "A":
        require(engine["data_capacity_bytes"] == (131072 if arm == "C" else 0), "observed capacity differs from arm")
        require(engine["data_rejections_total"] == 0, "unexpected data rejection")
        require(engine["data_bytes"] <= coverage["maximum_possible_data_bytes"], "logical bytes exceed workload bound")
        if phase in ("after", "settled"):
            require(engine["data_bytes"] == coverage["final_data_bytes"], phase + " logical bytes differ")
        if phase == "before":
            require(engine["data_bytes"] == 0, "startup is not empty")
    if phase == "settled":
        require(engine["applied_sequence"] == engine["durable_sequence"] == coverage["requests"]
                and engine["wal_pending_bytes"] == 0, "settled WAL does not match every successful PUT")
    for field in ("wal_commit_failures_total", "snapshot_failures_total", "async_callback_failures_total"):
        require(type(engine.get(field)) is int and engine[field] == 0, "engine failure counter: " + field)


def resource_metrics(directory, report, manifest, result, official, timestamp):
    metrics = {role: {"cpu_cores": None, "cpu_window_seconds": None, "cpu_samples": 0,
                      "rss_bytes": None, "rss_samples": 0, "hwm_bytes": None, "errors": []} for role in ROLES}
    start = timestamp(report["measurement_started_at"])
    end = start + report["elapsed_ns"]
    try:
        samples = list(official.read_samples(directory.parent, {"name": directory.name}, "resources.jsonl"))
    except Exception as error:
        for row in metrics.values():
            row["errors"].append(str(error))
        return metrics
    hz = manifest["metadata"]["cpu"].get("clock_ticks_per_second")
    for role, row in metrics.items():
        try:
            pid = result["processes"][role]["pid"]
            identity = None
            previous = None
            first_cpu = last_cpu = None
            for sample in samples:
                process = sample["processes"][role]
                when = timestamp(process["sampled_at_utc"])
                require(process["pid"] == pid, "sampled PID differs")
                started = process.get("starttime_ticks")
                if started is None and all(process.get(field) is None for field in
                                           ("cpu_user_ticks", "cpu_system_ticks", "rss_bytes", "hwm_bytes")):
                    continue
                require(uint(started) and (identity is None or started == identity), "process identity changed")
                identity = started
                hwm = process.get("hwm_bytes")
                require(hwm is None or uint(hwm), "invalid HWM")
                if hwm is not None:
                    row["hwm_bytes"] = max(hwm, row["hwm_bytes"] or 0)
                if not start <= when <= end:
                    continue
                rss = process.get("rss_bytes")
                require(rss is None or uint(rss), "invalid RSS")
                if rss is not None:
                    row["rss_bytes"] = max(rss, row["rss_bytes"] or 0)
                    row["rss_samples"] += 1
                user, system, mono = (process.get(key) for key in ("cpu_user_ticks", "cpu_system_ticks", "monotonic_ns"))
                if user is None or system is None or mono is None:
                    continue
                require(all(uint(value) for value in (user, system, mono)), "invalid CPU sample")
                current = (mono, user, system, when)
                if previous is not None:
                    require(mono > previous[0] and user >= previous[1] and system >= previous[2]
                            and when >= previous[3], "CPU time/counters decreased")
                first_cpu = current if first_cpu is None else first_cpu
                last_cpu = previous = current
                row["cpu_samples"] += 1
            if row["cpu_samples"] >= 2:
                require(type(hz) is int and hz > 0, "missing process clock tick frequency")
                span = (last_cpu[0] - first_cpu[0]) / 1e9
                row.update(cpu_window_seconds=span,
                           cpu_cores=((last_cpu[1] + last_cpu[2]) - (first_cpu[1] + first_cpu[2])) / hz / span,
                           cpu_first_utc_ns=first_cpu[3], cpu_last_utc_ns=last_cpu[3])
        except Exception as error:
            row.update(cpu_cores=None, rss_bytes=None, hwm_bytes=None)
            row["errors"].append(str(error))
    return metrics


def analyze_round(root, plan, stage, case, manifest, base, stage_errors, modules):
    official, experiment, snapshot = modules
    row = {"stage": stage["directory"], "block": stage["block"], "arm": stage["arm"],
           "run": case["name"], "wal_mode": case["wal_mode"], "snapshot": "on" if case["snapshot_interval_ms"] else "off",
           "official_status": base.get("status", "missing"), "eligible": False,
           "errors": list(stage_errors) + list(base.get("errors", [])), "coverage_errors": [],
           "warnings": list(base.get("warnings", [])), "qps": None, "p99_ms": None, "p999_ms": None,
           "max_ms": None, "elapsed_seconds": None, "before": None, "after": None, "settled": None, "resources": None,
           "snapshot_internal_samples": 0, "snapshot_idle_samples": 0, "snapshot_completed_delta": None,
           "snapshot_window": None, "measurement_started_at": None, "measurement_end_utc_ns": None}
    directory = relative(root, stage["directory"])
    run = relative(directory, case["name"])
    if manifest is None:
        return row
    try:
        result = load(run / "result.json")
        report = experiment.read_report(run / "report.json", official.config_for(directory, manifest, case))
        row.update(qps=report["qps_successful"], p99_ms=report["latency_ns"]["p99"] / 1e6,
                   p999_ms=report["latency_ns"]["p99_9"] / 1e6, max_ms=report["latency_ns"]["max"] / 1e6,
                   elapsed_seconds=report["elapsed_ns"] / 1e9,
                   measurement_started_at=report["measurement_started_at"],
                   measurement_end_utc_ns=experiment.timestamp_ns(report["measurement_started_at"]) + report["elapsed_ns"])
        require(result.get("finished_at") is not None, "round lacks final timestamp and shutdown evidence")
        require(experiment.timestamp_ns(result["started_at"]) <= experiment.timestamp_ns(report["started_at"])
                <= experiment.timestamp_ns(report["measurement_started_at"])
                <= row["measurement_end_utc_ns"] <= experiment.timestamp_ns(result["finished_at"]),
                "client measurement is outside the recorded round")
        require(report["load_model"] == "closed_loop" and report["workload_generator"] == "indexed-pcg-v1", "unexpected load generator")
        require(report["client_build"].get("vcs_revision") == plan["common_go_revision"]
                and report["client_build"].get("vcs_modified") is False, "benchmark build revision differs")
        require(report["outcomes"]["requests"] == report["outcomes"]["successes"] == 500000
                and report["outcomes"]["failures"] == 0 and report["operations"] == {"put": 500000, "get": 0, "delete": 0}
                and report["http_statuses"] == {"200": 500000} and report["preload"]["completed_keys"] == 0,
                "workload did not complete every planned PUT successfully")
        for phase in ("before", "after", "settled"):
            body = load(run / ("stats-" + phase + ".json"))
            row[phase] = {field: body["engine"].get(field) for field in DATA_FIELDS}
            check_data(body["engine"], stage["arm"], phase, plan["coverage"])
            for section, field in ((body["server"], "requests_rejected_total"),
                                   (body["server"], "connections_rejected_total"),
                                   (body["gateway"]["rpc"], "errors_total"),
                                   (body["gateway"]["rpc"], "retries_total")):
                require(type(section.get(field)) is int and section[field] == 0, "unexpected rejection/RPC counter: " + field)
        coverage_state = snapshot.empty_snapshot()
        internal = snapshot.internal_samples(directory, manifest, case, report, coverage_state)
        for sample in internal:
            check_data(sample["engine"], stage["arm"], "internal", plan["coverage"])
        idle = [sample for sample in internal if sample["engine"]["snapshot_in_progress"] is False]
        row.update(snapshot_internal_samples=len(internal), snapshot_idle_samples=len(idle),
                   failed_stats_queries=coverage_state["coverage"]["failed_queries"])
        if len(idle) >= 2:
            first, last = idle[0], idle[-1]
            row["snapshot_completed_delta"] = last["engine"]["snapshot_successes_total"] - first["engine"]["snapshot_successes_total"]
            row["snapshot_window"] = {label: {field: sample[field] for field in ("line", "started_at", "finished_at")}
                                      for label, sample in (("first", first), ("last", last))}
            for label, sample in (("first", first), ("last", last)):
                row["snapshot_window"][label]["snapshot_successes_total"] = sample["engine"]["snapshot_successes_total"]
        else:
            row["coverage_errors"].append("fewer than two wholly internal idle stats queries")
        completed = row["snapshot_completed_delta"]
        if row["snapshot"] == "on" and (completed is None or completed < 5):
            row["coverage_errors"].append("fewer than five internal completed snapshots")
        if row["snapshot"] == "off":
            if completed != 0:
                row["coverage_errors"].append("OFF completion delta is not zero")
            if any(sample["engine"]["snapshot_in_progress"] for sample in internal):
                row["errors"].append("snapshot in progress while snapshots are disabled")
        if row["elapsed_seconds"] < 10:
            row["coverage_errors"].append("measurement shorter than ten seconds")
        row["resources"] = resource_metrics(run, report, manifest, result, official, experiment.timestamp_ns)
    except Exception as error:
        row["errors"].append(str(error))
    if row["official_status"] != "valid":
        row["errors"].append("official summary status is " + row["official_status"])
    row["eligible"] = not row["errors"] and not row["coverage_errors"]
    return row


def ratios(rows, plan):
    index = {(row["block"], row["arm"], row["wal_mode"], row["snapshot"]): row for row in rows}
    pairs, groups = [], []
    for contrast in plan["comparisons"]["contrasts"]:
        numerator, denominator = contrast.split("/")
        for mode in ("throughput", "reliable"):
            for state in ("off", "on"):
                selected = []
                for block in range(1, 5):
                    new = index[block, numerator, mode, state]
                    old = index[block, denominator, mode, state]
                    pair = {"contrast": contrast, "block": block, "wal_mode": mode, "snapshot": state,
                            "numerator_stage": new["stage"], "denominator_stage": old["stage"],
                            "eligible": new["eligible"] and old["eligible"], "qps_ratio": None,
                            "p99_ratio": None, "p999_ratio": None, "engine_rss_ratio": None,
                            "engine_rss_increase_bytes": None}
                    for field in ("qps", "p99", "p999"):
                        key = field if field == "qps" else field + "_ms"
                        if new[key] is not None and old[key] is not None and old[key] > 0:
                            pair[field + "_ratio"] = new[key] / old[key]
                    new_rss = (new.get("resources") or {}).get("engine", {}).get("rss_bytes")
                    old_rss = (old.get("resources") or {}).get("engine", {}).get("rss_bytes")
                    if new_rss is not None and old_rss is not None and old_rss > 0:
                        pair.update(engine_rss_ratio=new_rss / old_rss, engine_rss_increase_bytes=new_rss - old_rss)
                    pairs.append(pair)
                    selected.append(pair)
                complete = all(pair["eligible"] and all(pair[name] is not None for name in
                               ("qps_ratio", "p99_ratio", "p999_ratio")) for pair in selected)
                group = {"contrast": contrast, "wal_mode": mode, "snapshot": state,
                         "eligible_pairs": sum(pair["eligible"] for pair in selected),
                         "classification": "inconclusive", "triggered_metrics": [], "metrics": {},
                         "engine_rss_diagnostic": "inconclusive"}
                for metric, worse in (("qps", lambda value: value < .95), ("p99", lambda value: value > 1.05),
                                      ("p999", lambda value: value > 1.05)):
                    recorded_values = [pair[metric + "_ratio"] for pair in selected]
                    values = [pair[metric + "_ratio"] if pair["eligible"] else None for pair in selected]
                    present = [value for value in values if value is not None]
                    count = sum(worse(value) for value in present)
                    group["metrics"][metric] = {"ratios": values, "recorded_ratios": recorded_values,
                                                "count_past_threshold": count,
                                                "median": statistics.median(present) if present else None,
                                                "minimum": min(present) if present else None,
                                                "maximum": max(present) if present else None}
                    if complete and count >= 3:
                        group["triggered_metrics"].append(metric)
                if complete:
                    group["classification"] = "investigate" if group["triggered_metrics"] else "not_triggered"
                rss_complete = complete and all(pair["engine_rss_ratio"] is not None for pair in selected)
                rss_count = sum(pair["eligible"] and pair["engine_rss_ratio"] is not None and pair["engine_rss_ratio"] > 1.05
                                and pair["engine_rss_increase_bytes"] > 1048576 for pair in selected)
                group["engine_rss_pairs_past_threshold"] = rss_count
                if rss_complete:
                    group["engine_rss_diagnostic"] = "investigate" if rss_count >= 3 else "not_triggered"
                groups.append(group)
    return pairs, groups


def flatten(value, prefix=""):
    result = {}
    for key, item in value.items():
        name = prefix + key
        if isinstance(item, dict):
            result.update(flatten(item, name + "."))
        else:
            result[name] = json.dumps(item, ensure_ascii=False) if isinstance(item, list) else item
    return result


def write_outputs(root, analysis):
    temporary = root / "analysis.json.tmp"
    temporary.write_text(json.dumps(analysis, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    temporary.replace(root / "analysis.json")
    rows = [{"record_type": kind, **flatten(row)} for kind, collection in
            (("round", analysis["rounds"]), ("pair", analysis["pairs"]), ("comparison", analysis["comparisons"]))
            for row in collection]
    fields = list(dict.fromkeys(key for row in rows for key in row))
    temporary = root / "analysis.csv.tmp"
    with temporary.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(root / "analysis.csv")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--repository", type=Path, required=True)
    args = parser.parse_args()
    root, repository = args.root.resolve(), args.repository.resolve()
    plan = load(root / "plan.json")
    common = validate_plan(root, plan)
    identity = collector_identity(root, repository, plan)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(repository / "benmark"))
    import summarize as official
    import experiment
    import snapshot_report as snapshot
    execution = execution_evidence(root, plan, experiment.timestamp_ns)
    analysis = {"schema_version": 1, "plan_sha256": digest(root / "plan.json"),
                "auditor_sha256": digest(Path(__file__)), "collector": identity,
                "errors": list(identity["errors"]) + list(execution["errors"]), "warnings": list(execution["warnings"]),
                "execution": {"complete": execution["complete"], "recorded_status": execution["recorded_status"],
                              "stages": {state: sum(item["state"] == state for item in execution["stages"].values())
                                         for state in ("complete", "failed", "incomplete", "missing")}},
                "stages": [], "rounds": [], "pairs": [], "comparisons": [],
                "interpretation": {"not_triggered": plan["policies"]["no_trigger_interpretation"],
                                   "investigate": plan["policies"]["trigger_interpretation"],
                                   "cpu": "Average CPU cores over the identified interior process sampling interval; no CPU/op estimate.",
                                   "rss": "Maximum sampled RSS inside measurement; HWM is observed process-lifetime high-water mark.",
                                   "alignment": "UTC alignment of process samples and HTTP query intervals is approximate; CPU duration uses process monotonic time.",
                                   "ratios": "Pair rows and recorded_ratios retain all available raw ratios; group ratios/distributions use eligible pairs only. Four eligible pairs are required for classification."}}
    for binary, info in plan["binaries"].items():
        path = relative(root, "frozen/" + binary)
        if path.exists():
            if digest(path) != info["sha256"]:
                analysis["errors"].append("frozen binary hash differs: " + binary)
        else:
            analysis["warnings"].append("frozen/" + binary + " absent; recorded hashes only")
    for stage in plan["stages"]:
        directory = relative(root, stage["directory"])
        evidence = execution["stages"][stage["directory"]]
        errors = list(analysis["errors"]) + list(evidence["errors"])
        warnings = list(evidence["warnings"])
        manifest, summary, by_name = None, None, {}
        cases = [{"name": "r01-{}-snapshot-{}".format(mode, "on" if interval else "off"),
                  "wal_mode": mode, "snapshot_interval_ms": interval}
                 for mode in stage["modes"] for interval in (0, 1000)]
        try:
            stage_root, manifest = official.manifest_for(directory)
            summary = official.summarize_experiment(stage_root, manifest)
            errors.extend(summary["errors"])
            by_name = {row["name"]: row for group in summary["groups"] for row in group["runs"]}
            check_stage(root, plan, stage, manifest, common, evidence["record"])
            warnings.extend(stage_order_evidence(directory, manifest, cases, evidence["record"], experiment.timestamp_ns))
        except Exception as error:
            errors.append(str(error))
        analysis["stages"].append({**stage, "errors": errors, "binary_verification": summary["binary_verification"] if summary else None,
                                   "official_counts": summary["counts"] if summary else None,
                                   "execution_state": evidence["state"],
                                   "warnings": warnings + (summary["warnings"] if summary else [])})
        for case in cases:
            row = analyze_round(root, plan, stage, case, manifest, by_name.get(case["name"], {}),
                                errors, (official, experiment, snapshot))
            row["stage_execution_state"] = evidence["state"]
            row["warnings"].extend(warnings)
            analysis["rounds"].append(row)
    analysis["pairs"], analysis["comparisons"] = ratios(analysis["rounds"], plan)
    analysis["counts"] = {"planned_rounds": len(analysis["rounds"]),
                           "eligible_rounds": sum(row["eligible"] for row in analysis["rounds"]),
                           "official_status": {status: sum(row["official_status"] == status for row in analysis["rounds"])
                                               for status in official.STATUSES},
                           "inconclusive_comparisons": sum(group["classification"] == "inconclusive" for group in analysis["comparisons"])}
    write_outputs(root, analysis)
    print(json.dumps(analysis["counts"], sort_keys=True))
    return int(analysis["counts"]["inconclusive_comparisons"] > 0)


if __name__ == "__main__":
    sys.exit(main())
