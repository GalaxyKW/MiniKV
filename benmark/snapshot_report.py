#!/usr/bin/env python3
"""Inspect snapshot counters in recorded experiment stages without rerunning them."""

import argparse
import json
import math
from pathlib import Path
import sys

import summarize
from experiment import read_report, timestamp_ns, validate_stats


UINT64_MAX = 2**64 - 1
PHASE_FIELDS = {
    "capture": "snapshot_capture_duration_ns_total",
    "write": "snapshot_write_duration_ns_total",
    "compact": "snapshot_compact_duration_ns_total",
}
ACQUISITIONS = "snapshot_capture_state_lock_acquisitions_total"
HOLD_DURATION = "snapshot_capture_state_lock_duration_ns_total"
HOLD_MAX = "snapshot_capture_state_lock_duration_ns_max"
ACCOUNTING_FIELDS = (
    ACQUISITIONS, HOLD_DURATION, HOLD_MAX,
    "snapshot_file_write_calls_total",
    "snapshot_file_written_bytes_total",
    "snapshot_file_installed_bytes_total",
    "snapshot_compact_written_bytes_total",
)
CORE_COUNTERS = (
    "keys", "applied_sequence", "durable_sequence", "wal_pending_bytes",
    "snapshot_successes_total", "snapshot_failures_total",
)
MONOTONIC_COUNTERS = (
    "applied_sequence", "durable_sequence", "snapshot_successes_total", "snapshot_failures_total",
)


def metric(kind="counter", phase=False):
    value = {"availability": "no_window", "reason": "No common idle window.",
             "kind": kind, "start": None, "end": None, "delta": None}
    if phase:
        value["mean_ns"] = None
    return value


def empty_snapshot(status="not_evaluated"):
    return {
        "status": status,
        "coverage": {"status": "unavailable", "reasons": [], "measurement_elapsed_ns": None,
                     "internal_samples": 0, "idle_samples": 0, "failed_queries": 0,
                     "completed": None, "window": None},
        "phases": {name: metric(phase=True) for name in PHASE_FIELDS},
        "accounting": {name: metric("lifetime_max" if name == HOLD_MAX else "counter")
                       for name in ACCOUNTING_FIELDS},
        "capture_hold_mean_ns": None, "warnings": [], "errors": [],
    }


def uint64(value):
    return summarize.integer(value, maximum=UINT64_MAX)


def internal_samples(root, manifest, case, report, result):
    start = timestamp_ns(report["measurement_started_at"])
    end = start + report["elapsed_ns"]
    expected_sequence = (report["preload"]["completed_keys"]
                         + report["operations"]["put"] + report["operations"]["delete"])
    inside = []
    previous_finish = None
    previous_monotonic_end = None
    for line, sample in enumerate(summarize.read_samples(root, case, "stats.jsonl"), 1):
        if sample["error"] is not None:
            summarize.require(isinstance(sample["error"], str) and bool(sample["error"]),
                              "invalid recorded query error at line {}".format(line))
            result["coverage"]["failed_queries"] += 1
            continue
        summarize.require(type(sample["http_status"]) is int and sample["http_status"] == 200,
                          "invalid successful query status at line {}".format(line))
        first, last = timestamp_ns(sample["started_at"]), timestamp_ns(sample["finished_at"])
        summarize.require(first <= last and (previous_finish is None or first >= previous_finish),
                          "successful query times overlap or decrease at line {}".format(line))
        previous_finish = last
        if "monotonic_start_ns" in sample or "monotonic_end_ns" in sample:
            mono_first, mono_last = sample.get("monotonic_start_ns"), sample.get("monotonic_end_ns")
            summarize.require(uint64(mono_first) and uint64(mono_last) and mono_first <= mono_last
                              and (previous_monotonic_end is None or mono_first >= previous_monotonic_end),
                              "invalid monotonic query times at line {}".format(line))
            previous_monotonic_end = mono_last
        if first < start or last > end:
            continue
        body = sample["body"]
        validate_stats(body)
        summarize.require(type(body["schema_version"]) is int, "invalid sampled stats schema type")
        engine = body["engine"]
        summarize.require(engine.get("wal_mode") == case["wal_mode"]
                          and type(engine.get("snapshot_in_progress")) is bool,
                          "invalid sampled snapshot state at line {}".format(line))
        for field in CORE_COUNTERS:
            summarize.require(uint64(engine.get(field)), "invalid sampled uint64: " + field)
        summarize.require(engine["durable_sequence"] <= engine["applied_sequence"] <= expected_sequence,
                          "sampled sequences contradict the validated report")
        workers = body["server"].get("workers_capacity")
        pool = body["gateway"].get("rpc", {}).get("pool_capacity")
        summarize.require(type(workers) is int and workers == manifest["arguments"]["engine_workers"]
                          and type(pool) is int and pool == manifest["arguments"]["rpc_pool"],
                          "sampled server configuration differs from manifest")
        if inside:
            for field in MONOTONIC_COUNTERS:
                summarize.require(engine[field] >= inside[-1]["engine"][field],
                                  "sampled counter decreased: " + field)
        inside.append({"line": line, "started_at": sample["started_at"],
                       "finished_at": sample["finished_at"], "engine": engine})
    return inside


def field_evidence(samples, field):
    """Validate the whole internal stream, including busy and trimmed samples."""
    if not samples:
        return "no_window", "No successful query lies wholly inside measurement."
    missing = sum(field not in sample["engine"] for sample in samples)
    nulls = sum(field in sample["engine"] and sample["engine"][field] is None for sample in samples)
    previous = None
    for sample in samples:
        value = sample["engine"].get(field)
        if value is None:
            continue
        if not uint64(value):
            return "invalid", "Not a uint64 at stats.jsonl line {}.".format(sample["line"])
        if previous is not None and value < previous:
            return "invalid", "Counter decreased at stats.jsonl line {}.".format(sample["line"])
        previous = value
    if missing == len(samples):
        return "missing", "Field is absent from every internal sample."
    if nulls == len(samples):
        return "null", "Field is null in every internal sample."
    if missing or nulls:
        return "partial", "Field is absent or null in some internal samples."
    return "available", None


def apply_metric(target, evidence, field, first, last):
    availability, reason = evidence
    target.update(availability=availability, reason=reason)
    if first is not None:
        for label, sample in (("start", first), ("end", last)):
            value = sample["engine"].get(field)
            target[label] = value if uint64(value) else None
    if availability == "available":
        if first is None:
            target.update(availability="no_window", reason="Fewer than two internal idle samples.")
        elif target["kind"] == "counter":
            target["delta"] = target["end"] - target["start"]


def snapshot_for(root, manifest, case, report, thresholds):
    result = empty_snapshot()
    coverage = result["coverage"]
    coverage["measurement_elapsed_ns"] = report["elapsed_ns"]
    try:
        samples = internal_samples(root, manifest, case, report, result)
    except FileNotFoundError:
        result["status"] = "missing"
        coverage["reasons"].append("stats.jsonl is missing; boundary queries cannot replace periodic evidence.")
        result["warnings"].append("Snapshot sampling is unavailable.")
        return result
    except Exception as error:
        result["status"] = "invalid"
        result["errors"].append(str(error))
        coverage["reasons"].append("Invalid sampling stream; no window was selected.")
        return result

    result["status"] = "valid"
    coverage["internal_samples"] = len(samples)
    if coverage["failed_queries"]:
        result["warnings"].append("{} recorded queries failed in stats.jsonl; those queries provide no state evidence."
                                  .format(coverage["failed_queries"]))
    # Inspect all internal counters before choosing endpoints. No field can
    # discard a busy/interior sample or choose a cheaper window for itself.
    evidence = {field: field_evidence(samples, field)
                for field in tuple(PHASE_FIELDS.values()) + ACCOUNTING_FIELDS}
    idle = [index for index, sample in enumerate(samples) if not sample["engine"]["snapshot_in_progress"]]
    coverage["idle_samples"] = len(idle)
    first = last = None
    if len(idle) >= 2:
        first, last = samples[idle[0]], samples[idle[-1]]
        coverage["window"] = {
            "first": {key: first[key] for key in ("line", "started_at", "finished_at")},
            "last": {key: last[key] for key in ("line", "started_at", "finished_at")},
            "trimmed_head_samples": idle[0], "trimmed_tail_samples": len(samples) - 1 - idle[-1],
        }
        coverage["completed"] = (last["engine"]["snapshot_successes_total"]
                                 - first["engine"]["snapshot_successes_total"])
        if coverage["completed"] < thresholds["min_completed"]:
            coverage["reasons"].append("Completed snapshots are below min_completed.")
    else:
        coverage["reasons"].append("Fewer than two internal idle samples.")
    if report["elapsed_ns"] / 1000000000 < thresholds["min_measurement_seconds"]:
        coverage["reasons"].append("Full measurement duration is below min_measurement_seconds.")
    coverage["status"] = "insufficient" if coverage["reasons"] else "sufficient"

    for phase, field in PHASE_FIELDS.items():
        target = result["phases"][phase]
        apply_metric(target, evidence[field], field, first, last)
        if target["availability"] == "available" and coverage["status"] == "sufficient":
            target["mean_ns"] = target["delta"] / coverage["completed"]
    for field, target in result["accounting"].items():
        apply_metric(target, evidence[field], field, first, last)
    for field, (availability, reason) in evidence.items():
        if availability == "invalid":
            result["errors"].append(field + ": " + reason)

    acquisitions = result["accounting"][ACQUISITIONS]
    duration = result["accounting"][HOLD_DURATION]
    if acquisitions["availability"] == "available":
        if acquisitions["delta"] != coverage["completed"]:
            result["warnings"].append("Capture acquisitions differ from completed snapshots; "
                                      "capture_hold_mean_ns is per acquisition, not per completed snapshot.")
        if (acquisitions["delta"] > 0 and duration["availability"] == "available"
                and coverage["status"] == "sufficient"):
            result["capture_hold_mean_ns"] = duration["delta"] / acquisitions["delta"]
    return result


def analyze_stage(directory, thresholds):
    result = {"path": str(directory), "arguments": None, "binary_verification": None,
              "binary_sha256": None, "counts": None, "errors": [], "warnings": [], "runs": []}
    try:
        root, manifest = summarize.manifest_for(directory)
        base = summarize.summarize_experiment(root, manifest)
    except Exception as error:
        result["errors"].append(str(error))
        return result
    for field in ("path", "arguments", "binary_verification", "binary_sha256", "counts", "errors", "warnings"):
        result[field] = base[field]
    rows = {row["name"]: row for group in base["groups"] for row in group["runs"]}
    for case in manifest["plan"]:
        row = rows[case["name"]]
        client = {field: row[field] for field in ("status", "qps_successful", "p99_ms", "errors", "warnings")}
        snapshot = empty_snapshot()
        if client["status"] == "valid":
            try:
                report = read_report(summarize.safe_path(root, case["name"] + "/report.json"),
                                     summarize.config_for(root, manifest, case))
                snapshot = snapshot_for(root, manifest, case, report, thresholds)
            except Exception as error:
                snapshot["status"] = "invalid"
                snapshot["errors"].append(str(error))
        else:
            snapshot["coverage"]["reasons"].append("Client artifacts did not pass experiment validation.")
        result["runs"].append({**case, "client": client, "snapshot": snapshot})
    return result


def write_text(report, output):
    thresholds = report["thresholds"]
    output.write("MiniKV snapshot report (schema 1)\n")
    output.write("Coverage thresholds: min_completed={}, min_measurement_seconds={}.\n".format(
        thresholds["min_completed"], thresholds["min_measurement_seconds"]))
    output.write("These are evidence filters, not performance acceptance criteria. Client metrics cover the full measurement.\n")
    output.write("All phases share each run's earliest/latest internal idle samples; wall-clock alignment is approximate.\n")
    output.write("Phase means are not pauses. File bytes are not device writes. Lifetime maxima are not window maxima.\n")
    for stage in report["experiments"]:
        output.write("\n{} (binary verification: {})\n".format(stage["path"], stage["binary_verification"]))
        for message in stage["errors"] + stage["warnings"]:
            output.write("  " + message + "\n")
        if stage["counts"] is not None:
            output.write("  Client counts: " + ", ".join("{}={}".format(key, value)
                                                        for key, value in stage["counts"].items()) + "\n")
        for row in stage["runs"]:
            client, snapshot = row["client"], row["snapshot"]
            coverage = snapshot["coverage"]
            output.write("  {}: client={} QPS={} P99={} ms; snapshot={} coverage={} completed={}\n".format(
                row["name"], client["status"], client["qps_successful"], client["p99_ms"], snapshot["status"],
                coverage["status"], coverage["completed"]))
            output.write("    samples: internal={} idle={} failed_queries={}\n".format(
                coverage["internal_samples"], coverage["idle_samples"], coverage["failed_queries"]))
            window = coverage["window"]
            if window:
                output.write("    shared window: lines {}..{}, query finishes {}..{}; trimmed head/tail={}/{}\n".format(
                    window["first"]["line"], window["last"]["line"], window["first"]["finished_at"],
                    window["last"]["finished_at"], window["trimmed_head_samples"], window["trimmed_tail_samples"]))
            for phase, value in snapshot["phases"].items():
                mean = None if value["mean_ns"] is None else value["mean_ns"] / 1000000
                output.write("    {}: {} start/end/delta={}/{}/{} ns; mean={} ms; {}\n".format(
                    phase, value["availability"], value["start"], value["end"], value["delta"], mean,
                    value["reason"] or ""))
            for field, value in snapshot["accounting"].items():
                output.write("    {}: {} ({}) start/end/delta={}/{}/{}; {}\n".format(
                    field, value["availability"], value["kind"], value["start"], value["end"], value["delta"],
                    value["reason"] or ""))
            output.write("    capture hold mean per acquisition={} ns\n".format(snapshot["capture_hold_mean_ns"]))
            for message in client["errors"] + client["warnings"] + coverage["reasons"] + snapshot["errors"] + snapshot["warnings"]:
                output.write("    " + message + "\n")


def positive_count(raw):
    try:
        value = int(raw)
        if value >= 1:
            return value
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("must be an integer of at least 1")


def nonnegative_seconds(raw):
    try:
        value = float(raw)
        if math.isfinite(value) and value >= 0:
            return value
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("must be finite and nonnegative")


def main(argv=None):
    if sys.version_info < (3, 8):
        print("MiniKV snapshot reports require Python 3.8 or newer.", file=sys.stderr)
        return 2
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path, help="experiment stage directories containing manifest.json")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--min-completed", type=positive_count, default=1,
                        help="minimum completed snapshots in the common idle window (default: 1)")
    parser.add_argument("--min-measurement-seconds", type=nonnegative_seconds, default=0.0,
                        help="minimum full client measurement duration (default: 0)")
    args = parser.parse_args(argv)
    try:
        directories = [directory.resolve() for directory in args.directories]
        summarize.require(len(set(directories)) == len(directories), "duplicate experiment directory")
    except (OSError, ValueError, RuntimeError) as error:
        parser.error(str(error))
    thresholds = {"min_completed": args.min_completed, "min_measurement_seconds": args.min_measurement_seconds}
    report = {"schema_version": 1, "thresholds": thresholds,
              "experiments": [analyze_stage(directory, thresholds) for directory in directories]}
    try:
        if args.format == "json":
            json.dump(report, sys.stdout, indent=2, ensure_ascii=False, allow_nan=False)
            sys.stdout.write("\n")
        else:
            write_text(report, sys.stdout)
        sys.stdout.flush()
    except (OSError, ValueError) as error:
        print("Cannot write snapshot report: " + str(error), file=sys.stderr)
        return 1
    failed = any(stage["errors"] or any(row["client"]["status"] != "valid"
                                      or row["snapshot"]["status"] == "invalid" or row["snapshot"]["errors"]
                                      for row in stage["runs"]) for stage in report["experiments"])
    if failed:
        print("Some artifacts are failed, missing, unfinished, or invalid; see the report.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
