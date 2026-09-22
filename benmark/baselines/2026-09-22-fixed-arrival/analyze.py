#!/usr/bin/env python3
"""Read-only analysis of the frozen A/B/C fixed-arrival experiment (Python 3.8+)."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys

sys.dont_write_bytecode = True
STAGES = {"formal": ["A1", "B1", "C1", "B2", "C2", "A2", "C3", "A3", "B3"], "pilot": ["A", "B", "C"]}
BOUNDS = {"server.requests_inflight": 132, "engine.async_requests_inflight": 132,
          "server.request_queue_depth": 128, "server.workers_active": 4,
          "gateway.rpc.pool_in_use": 256, "gateway.rpc.connections": 256,
          "gateway.rpc.idle_connections": 256, "server.connections": 258,
          "engine.wal_pending_bytes": 16777216}
CAPACITIES = {"server.requests_capacity": 132, "engine.async_requests_capacity": 132,
              "server.request_queue_capacity": 128, "server.workers_capacity": 4,
              "gateway.rpc.pool_capacity": 256, "server.connection_capacity": 258,
              "engine.wal_queue_capacity_bytes": 16777216}
ZERO = ("engine.wal_commit_failures_total", "engine.snapshot_failures_total",
        "engine.async_callback_failures_total", "server.connections_rejected_total",
        "gateway.rpc.errors_total", "gateway.rpc.exchange_errors_total", "gateway.rpc.retries_total")
COUNTERS = ("server.requests_rejected_total", "server.requests_started_total", "gateway.rpc.calls_total",
            "engine.applied_sequence", "engine.durable_sequence")
GAUGES = tuple(BOUNDS) + ("engine.wal_inflight_bytes", "engine.wal_queued_records",
                          "engine.wal_durable_waiters", "engine.wal_capacity_waiters")
METRICS = ("planned", "started", "busy", "late", "successes", "http503", "failures", "loss_fraction",
           "arrival_loss_fraction", "late_fraction", "offered_success_rate_pct", "qps_successful",
           "service_p99_ms", "dispatch_p99_ms", "scheduled_p99_ms", "elapsed_seconds", "settled_keys")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def field(body, name):
    for part in name.split("."):
        body = body[part]
    return body


def check(errors, condition, message):
    if not condition and message not in errors:
        errors.append(message)


def stats_evidence(directory, case, report, row, s):
    bodies = {phase: s.load_json(directory / case["name"] / ("stats-" + phase + ".json"))
              for phase in ("before", "after", "settled")}
    samples = list(s.read_samples(directory, case, "stats.jsonl"))
    start = s.timestamp_ns(report["measurement_started_at"])
    end = start + report["elapsed_ns"]
    inside, successful, failed = [], [], 0
    for sample in samples:
        if sample["error"] is not None:
            failed += 1
            continue
        s.require(type(sample["http_status"]) is int and sample["http_status"] == 200, "bad stats HTTP status")
        first, last = s.timestamp_ns(sample["started_at"]), s.timestamp_ns(sample["finished_at"])
        s.require(first <= last, "stats query ends before it starts")
        successful.append(sample["body"])
        if start <= first and last <= end:
            inside.append(sample["body"])
    errors = row["invariant_errors"]
    observed = list(bodies.values()) + successful
    for body in observed:
        s.require(type(body.get("schema_version")) is int, "invalid stats schema type")
        try:
            s.validate_stats(body)
        except Exception as error:
            check(errors, False, str(error))
        check(errors, body["engine"]["wal_mode"] == "reliable", "sampled WAL mode changed")
        check(errors, type(body["engine"]["snapshot_in_progress"]) is bool, "invalid sampled snapshot state")
        for name in set(GAUGES + ZERO + COUNTERS + tuple(CAPACITIES)):
            s.require(s.integer(field(body, name)), "missing or invalid stats integer: " + name)
        for name, limit in BOUNDS.items():
            check(errors, field(body, name) <= limit, name + " exceeds " + str(limit))
        for name, limit in CAPACITIES.items():
            check(errors, field(body, name) == limit, name + " differs from configured capacity")
        for name in ZERO:
            check(errors, field(body, name) == 0, name + " is nonzero")
        check(errors, body["gateway"]["rpc"]["closed"] is False, "RPC pool closed during measurement")
        check(errors, body["engine"]["durable_sequence"] <= body["engine"]["applied_sequence"],
              "durable sequence exceeds applied sequence")
    check(errors, bool(inside), "no fully contained measurement stats queries")
    for name in COUNTERS:
        values = [field(body, name) for body in successful]
        check(errors, values == sorted(values), "sampled counter regressed: " + name)
    row["stats"] = {"successful_queries": len(successful), "failed_queries": failed,
                    "measurement_queries": len(inside), "alignment": "fully contained wall-clock query intervals",
                    "all_observed_max": {name: max(field(body, name) for body in observed) for name in GAUGES},
                    "measurement_gauges": {name: {"n": len(inside),
                        "mean": statistics.mean(field(body, name) for body in inside) if inside else None,
                        "max": max(field(body, name) for body in inside) if inside else None} for name in GAUGES}}
    if failed:
        row["warnings"].append(str(failed) + " stats queries failed; no values imputed")
    row["counter_deltas"] = {phase: {name: field(bodies[phase], name) - field(bodies["before"], name)
                                    for name in COUNTERS} for phase in ("after", "settled")}
    for phase, values in row["counter_deltas"].items():
        check(errors, all(value >= 0 for value in values.values()), phase + " counter delta is negative")
    return bodies


def analyze_run(directory, manifest, case, base, stage, plan, s):
    row = dict(base, stage=stage, arm=stage[0], snapshot="on" if case["snapshot_interval_ms"] else "off",
               artifact_errors=list(base["errors"]), invariant_errors=[], attribution_checks={},
               complete_validated_measurement=base["status"] in s.MEASURED, generator_quality_pass=False,
               hypothesis_pass=False, attribution_supported=False, supported_isolation=False,
               metrics={name: None for name in METRICS})
    try:
        report = s.read_report(directory / case["name"] / "report.json", s.config_for(directory, manifest, case))
        a, o, h = report["arrivals"], report["outcomes"], report["http_statuses"]
        m = row["metrics"]
        m.update(planned=a["planned"], started=a["started"], busy=a["dropped_busy"], late=a["dropped_late"],
                 successes=o["successes"], http503=h.get("503", 0), failures=o["failures"],
                 loss_fraction=(a["planned"] - o["successes"]) / a["planned"],
                 arrival_loss_fraction=(a["dropped_busy"] + a["dropped_late"]) / a["planned"],
                 late_fraction=a["dropped_late"] / a["planned"], elapsed_seconds=report["elapsed_ns"] / 1e9,
                 offered_success_rate_pct=report["offered_success_rate_pct"], qps_successful=report["qps_successful"],
                 service_p99_ms=report["latency_ns"]["p99"] / 1e6 if a["started"] else None,
                 dispatch_p99_ms=report["dispatch_delay_ns"]["p99"] / 1e6 if a["started"] else None,
                 scheduled_p99_ms=report["scheduled_latency_ns"]["p99"] / 1e6 if a["started"] else None)
        row["client_build"] = report["client_build"]
        check(row["artifact_errors"], report["client_build"]["vcs_revision"] == plan["runtime_revision"]
              and report["client_build"]["vcs_modified"] is False, "benchmark build source differs from frozen plan")
        check(row["artifact_errors"], report["workload_generator"] == "indexed-pcg-v1", "unexpected workload generator")
        row["generator_quality_pass"] = m["late_fraction"] <= plan["generator_quality"]["max_late_fraction"]
        if not row["generator_quality_pass"]:
            row["warnings"].append("GENERATOR QUALITY FAILED: late/planned exceeds 1%; mechanism isolation incomplete")
        check(row["invariant_errors"], m["planned"] == m["started"] + m["busy"] + m["late"], "arrival accounting mismatch")
        supported_failures = (all(o[name] == 0 for name in ("network_errors", "timeouts", "transport_errors",
                              "protocol_failures", "logical_misses")) and set(h) <= {"200", "503"}
                              and o["failures"] == o["http_failures"] == m["http503"] and h.get("200", 0) == m["successes"])
        check(row["invariant_errors"], supported_failures, "unsupported service failure; cannot attribute HTTP503")
        bodies = stats_evidence(directory, case, report, row, s)
        checks = row["attribution_checks"]
        checks["only_supported_http_outcomes"] = supported_failures
        for phase, d in row["counter_deltas"].items():
            checks[phase + "_http503_equals_server_rejected_delta"] = m["http503"] == d["server.requests_rejected_total"]
            checks[phase + "_success_equals_server_started_delta_equals_applied"] = (
                m["successes"] == d["server.requests_started_total"] == d["engine.applied_sequence"])
            checks[phase + "_gateway_calls_equals_arrivals_started"] = d["gateway.rpc.calls_total"] == m["started"]
        settled = bodies["settled"]
        m["settled_keys"] = settled["engine"]["keys"]
        check(row["invariant_errors"], s.fixed_arrival_settled(settled), "settled inflight/WAL state is not quiet")
        row["attribution_supported"] = all(checks.values()) and not row["invariant_errors"] and not row["artifact_errors"]
        rejected = row["counter_deltas"]["settled"]["server.requests_rejected_total"]
        row["hypothesis_pass"] = {"A": m["busy"] == rejected == m["failures"] == 0,
                                  "B": m["busy"] > 0 and rejected == m["failures"] == 0,
                                  "C": rejected > 0 and m["http503"] > 0 and m["busy"] == 0}[stage[0]]
    except Exception as error:
        row["artifact_errors"].append(str(error))
    row["complete_validated_measurement"] &= not row["artifact_errors"]
    row["attribution_failed_checks"] = [name for name, passed in row["attribution_checks"].items() if not passed]
    row["supported_isolation"] = (row["complete_validated_measurement"] and row["attribution_supported"]
                                  and row["generator_quality_pass"] and row["hypothesis_pass"])
    return row


def analyze(root, phase, repo, s):
    plan = s.load_json(root / "PLAN.json")
    plan_hash = digest(root / "PLAN.json")
    errors, rows, stages = [], [], []
    try:
        recorded_hash = (root / "PLAN.sha256").read_text().split()
        check(errors, bool(recorded_hash) and recorded_hash[0] == plan_hash, "PLAN SHA256 mismatch")
    except OSError as error:
        errors.append(str(error))
    try:
        host_context = s.load_json(root / "host-context.json")
        s.require(isinstance(host_context, dict), "host context is not an object")
    except Exception as error:
        host_context = None
        errors.append(str(error))
    check(errors, all(plan[name + "_order"] == order for name, order in STAGES.items()),
          "frozen outer plan is incomplete or reordered")
    check(errors, plan["arms"] == {"A": {"workers": 256, "wal_flush_ms": 2},
          "B": {"workers": 20, "wal_flush_ms": 1000}, "C": {"workers": 256, "wal_flush_ms": 1000}}, "arm plan changed")
    check(errors, plan["common"]["requests"] == 5000 and plan["pilot_requests"] == 1000
          and plan["generator_quality"]["max_late_fraction"] == .01, "run size or generator gate changed")
    expected_stages = STAGES[phase]
    try:
        execution = s.load_json(root / phase / "execution.json")
        s.require(isinstance(execution, list) and all(isinstance(record, dict) for record in execution),
                  "execution log is not an array of records")
    except Exception as error:
        execution = []
        errors.append(str(error))
    check(errors, [record.get("stage") for record in execution] == expected_stages, "execution does not cover complete ordered plan")
    previous_end, runtime = None, None
    for stage in expected_stages:
        stage_errors, stage_rows = [], []
        evidence = {"stage": stage, "errors": stage_errors}
        stages.append(evidence)
        try:
            directory, manifest = s.manifest_for(root / phase / stage)
            expected = dict(plan["common"], **plan["arms"][stage[0]])
            expected["modes"] = [expected["modes"]]
            if phase == "pilot":
                expected["requests"] = plan["pilot_requests"]
            check(stage_errors, all(manifest["arguments"].get(k) == v for k, v in expected.items()), "stage configuration differs from PLAN")
            check(stage_errors, [case["name"] for case in manifest["plan"]] ==
                  ["r01-reliable-snapshot-off", "r01-reliable-snapshot-on"], "stage snapshot order changed")
            source = manifest["metadata"]["git"]
            check(stage_errors, source["available"] is True and source["head"] == plan["runtime_revision"]
                  and source["dirty"] is False and source["tracked_dirty"] is False
                  and source["tracked_diff_sha256"] == hashlib.sha256(b"").hexdigest(), "stage source is not clean frozen revision")
            hashes = {role: manifest["executables"][role]["sha256"] for role in s.ROLES}
            check(stage_errors, hashes == {role: plan["binaries"][role]["sha256"] for role in s.ROLES}, "stage binary hashes differ from PLAN")
            check(stage_errors, all(manifest["metadata"]["binaries"][role]["path"] == plan["binaries"][role]["path"]
                  == manifest["arguments"][role] for role in s.ROLES), "binary source paths differ from PLAN")
            if runtime is None:
                runtime = manifest["runtime_environment"]
            check(stage_errors, manifest["runtime_environment"] == runtime, "runtime environment differs across stages")
            summary = s.summarize_experiment(directory, manifest)
            stage_errors.extend(summary["errors"])
            evidence.update(source=source, binary_sha256=hashes, binary_verification=summary["binary_verification"],
                            runtime_environment=manifest["runtime_environment"], warnings=summary["warnings"],
                            manifest_sha256=digest(directory / "manifest.json"),
                            host={name: manifest["metadata"][name] for name in ("platform", "cpu", "memory", "cmake_caches")})
            by_name = {r["name"]: r for g in summary["groups"] for r in g["runs"]}
            stage_rows = [analyze_run(directory, manifest, case, by_name[case["name"]], stage, plan, s)
                          for case in manifest["plan"]]
            record = next(item for item in execution if item.get("stage") == stage)
            command = ["--output", manifest["arguments"]["output"]]
            for role, identity in plan["binaries"].items():
                command += ["--" + role, identity["path"]]
            for key, value in dict(plan["common"], **plan["arms"][stage[0]]).items():
                command += ["--" + key.replace("_", "-"), str(plan["pilot_requests"] if phase == "pilot" and key == "requests" else value)]
            check(stage_errors, record["command"][2:] == command, "stage invocation differs from PLAN")
            first, last = s.timestamp_ns(record["started_at"]), s.timestamp_ns(record["finished_at"])
            check(stage_errors, first <= last and (previous_end is None or previous_end <= first), "stage execution overlaps or is unordered")
            previous_end = last
            run_index = s.load_json(directory / "index.json")
            check(stage_errors, run_index["status"] == "complete" and len(run_index["runs"]) == 2, "stage index is incomplete")
            check(stage_errors, record["returncode"] == int(any(r["status"] == "degraded" for r in run_index["runs"])), "stage exit contradicts measurements")
            previous_run_end = first
            for offset, case in enumerate(manifest["plan"]):
                result = s.load_json(directory / case["name"] / "result.json")
                check(stage_errors, result == run_index["runs"][offset], "index and per-run result differ or are reordered")
                run_start, run_end = s.timestamp_ns(result["started_at"]), s.timestamp_ns(result["finished_at"])
                check(stage_errors, previous_run_end <= run_start <= run_end <= last, "run execution overlaps or escapes stage")
                previous_run_end = run_end
        except Exception as error:
            stage_errors.append(str(error))
        for snapshot in ("off", "on"):
            matches = [r for r in stage_rows if r["snapshot"] == snapshot]
            row = matches[0] if matches else {"stage": stage, "arm": stage[0], "snapshot": snapshot, "status": "missing",
                "artifact_errors": ["planned run unavailable"], "invariant_errors": [], "attribution_checks": {},
                "metrics": {name: None for name in METRICS}, "memory": s.empty_memory(),
                "complete_validated_measurement": False, "generator_quality_pass": False,
                "hypothesis_pass": False, "attribution_supported": False, "supported_isolation": False}
            if stage_errors:
                row["artifact_errors"].extend(stage_errors)
                row.update(complete_validated_measurement=False, attribution_supported=False, supported_isolation=False)
            rows.append(row)
    groups = []
    for arm in "ABC":
        for snapshot in ("off", "on"):
            cohort = [r for r in rows if r["arm"] == arm and r["snapshot"] == snapshot]
            measured = [r for r in cohort if not errors and r["complete_validated_measurement"]
                        and not r["artifact_errors"] and not r["invariant_errors"]]
            groups.append({"arm": arm, "snapshot": snapshot, "planned": len(cohort),
                "complete_validated": sum(r["complete_validated_measurement"] for r in cohort),
                "numeric_eligible": len(measured),
                "degraded": sum(r["status"] == "degraded" for r in cohort),
                "supported_isolation": all(r["supported_isolation"] for r in cohort),
                "metrics": {name: s.distribution([r["metrics"][name] for r in measured]) for name in METRICS},
                "measurement_gauges": {name: {stat: s.distribution([
                    r.get("stats", {}).get("measurement_gauges", {}).get(name, {}).get(stat) for r in measured])
                    for stat in ("mean", "max")} for name in GAUGES},
                "memory": {role: {name: s.distribution([r["memory"][role][name] for r in measured])
                                   for name in s.MEMORY_FIELDS} for role in s.ROLES}})
    failed = bool(errors or any(r["artifact_errors"] or r["invariant_errors"] for r in rows))
    return {"schema_version": 1, "phase": phase, "plan_sha256": plan_hash, "runtime_revision": plan["runtime_revision"],
            "validator_sha256": {name: digest(repo / "benmark" / name) for name in ("summarize.py", "experiment.py")},
            "errors": errors, "analysis_pass": not failed, "planned_runs": len(expected_stages) * 2,
            "complete_validated_measurements": sum(r["complete_validated_measurement"] for r in rows),
            "generator_quality_failed_runs": [r["stage"] + "/" + r["snapshot"] for r in rows if not r["generator_quality_pass"]],
            "host_context": host_context,
            "supported_isolation": not errors and all(r["supported_isolation"] for r in rows),
            "notes": ["Degraded is a complete measurement, not an analysis failure.",
                      "HTTP503 equality is attribution evidence, not a server contract; engine Busy can also produce 503.",
                      "Loss fraction includes busy, late, and failed requests; latency includes attempted requests only.",
                      "QPS includes drain time; per-run P99 medians are not pooled P99s; gauge means are unweighted sample means.",
                      "All planned rows remain; distributions exclude artifact/invariant failures, retain generator/hypothesis failures, and report eligible n.",
                      "Admitted subsets have different settled key counts; cross-arm RSS is descriptive, not isolated per-request memory cost.",
                      "RSS is a measurement sample maximum; HWM is observed process lifetime; missing values remain null."],
            "limits": plan["limits"], "stages": stages, "runs": rows, "groups": groups}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--phase", choices=("pilot", "formal"), default="formal")
    args = parser.parse_args()
    sys.path.insert(0, str(args.repo.resolve() / "benmark"))
    try:
        import summarize
        result = analyze(args.root.resolve(), args.phase, args.repo.resolve(), summarize)
    except Exception as error:
        result = {"analysis_pass": False, "supported_isolation": False, "errors": [str(error)]}
    json.dump(result, sys.stdout, indent=2, allow_nan=False)
    sys.stdout.write("\n")
    return 0 if result["analysis_pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
