#!/usr/bin/env python3
"""Recompute the frozen snapshot-vector comparison; never modify raw artifacts."""
import argparse
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import statistics
import sys

sys.dont_write_bytecode = True


def load(path):
    return json.loads(path.read_text(encoding="utf-8"),
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def save(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
                    encoding="utf-8")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def option_map(arguments):
    require(len(arguments) % 2 == 0, "unpaired command option")
    return {arguments[i][2:].replace("-", "_"): arguments[i + 1]
            for i in range(0, len(arguments), 2)}


def ratio_test(numerator, denominator, threshold, direction):
    result = {"numerator": numerator, "denominator": denominator,
              "ratio": None, "threshold": threshold, "comparison": direction, "pass": False}
    if type(numerator) is not int or type(denominator) is not int or numerator < 0 or denominator <= 0:
        result["error"] = "missing, invalid or zero-denominator measurement"
        return result
    limit = Fraction(str(threshold))
    left, right = numerator * limit.denominator, denominator * limit.numerator
    result.update(ratio=numerator / denominator,
                  pass_=(left <= right if direction == "<=" else left >= right))
    result["pass"] = result.pop("pass_")
    result["cross_products"] = {"observed": left, "threshold": right}
    return result


def distribution(values):
    values = [value for value in values if value is not None]
    return {"n": len(values), "min": min(values) if values else None,
            "median": statistics.median(values) if values else None,
            "max": max(values) if values else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output-dir", type=Path, help="derived output directory; defaults to ROOT/analysis")
    parser.add_argument("--repo", type=Path, help="repository containing the original analysis modules")
    parser.add_argument("--storage-checks", type=Path,
                        help="historical post-run-checks.json attachment; never rerun storage checks")
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output_dir.resolve() if args.output_dir else root / "analysis"
    output.mkdir(parents=True, exist_ok=True)
    plan = load(root / "plan.json")
    modules = args.repo.resolve() / "benmark" if args.repo else Path(plan["collector"]).parent
    sys.path.insert(0, str(modules))
    import summarize
    import snapshot_report

    thresholds = plan["thresholds"]
    errors, warnings = [], []
    expected_stages = [f"{scenario}-{number:02d}-{version}"
                       for scenario in ("primary", "stress")
                       for number, version in enumerate("ABBA", 1)]
    expected_runs = ["r01-throughput-snapshot-" + state for state in ("off", "on")]
    identities = {item["path"]: item["sha256"] for item in plan["frozen_files"]}
    try:
        require([stage["name"] for stage in plan["stages"]] == expected_stages,
                "frozen plan does not contain the expected eight ABBA stages")
        require(sha(root / "plan.json") == (root / "PLAN_SHA256").read_text().split()[0],
                "plan hash differs from PLAN_SHA256")
        execution = load(root / "execution.json")
        require(execution["plan_sha256"] == sha(root / "plan.json"), "executed plan hash differs")
        controller = next(item for item in plan["frozen_files"] if Path(item["path"]).name == "run_plan.py")
        require(execution["controller_sha256"] == controller["sha256"],
                "executed controller hash differs")
        require(execution["status"] == "complete", "matrix execution was not complete")
        require([stage["name"] for stage in execution["stages"]] == expected_stages,
                "executed stage order differs")
        require(all(stage["status"] == "complete" and stage.get("exit_code") == 0
                    for stage in execution["stages"]), "a stage did not finish successfully")
        for filename in ("summarize.py", "snapshot_report.py", "experiment.py", "experiment_support.py"):
            recorded = next(item for item in plan["frozen_files"] if Path(item["path"]).name == filename)
            require(sha(modules / filename) == recorded["sha256"], "analysis module changed: " + filename)
    except (KeyError, OSError, ValueError, StopIteration) as error:
        errors.append(str(error))

    summary_stages, snapshot_stages, rows = [], [], []
    coverage_limits = {"min_completed": thresholds["min_completed_snapshots"],
                       "min_measurement_seconds": thresholds["min_measurement_seconds"]}
    for stage in plan["stages"]:
        directory = root / stage["name"]
        base, detailed, manifest = None, None, None
        stage_errors = []
        try:
            _, manifest = summarize.manifest_for(directory)
            base = summarize.summarize_experiment(directory, manifest)
            detailed = snapshot_report.analyze_stage(directory, coverage_limits)
            summary_stages.append(base)
            snapshot_stages.append(detailed)
            stage_errors.extend(base["errors"])
            stage_errors.extend(detailed["errors"])
            require([case["name"] for case in manifest["plan"]] == expected_runs,
                    "stage is not exactly OFF then ON")
            for name, value in option_map(plan["common_args"] + stage["scenario_args"]).items():
                actual = manifest["arguments"].get(name, 0 if name in ("rate", "max_data_bytes") else None)
                expected = value.split(",") if name == "modes" else value
                matches = actual == expected or str(actual) == expected
                if type(actual) in (int, float):
                    matches = Fraction(str(actual)) == Fraction(value)
                require(matches,
                        "manifest differs from plan: " + name)
            for role in ("engine", "gateway", "bench"):
                binary = plan["engines"][stage["version"]] if role == "engine" else plan[role]
                require(base["binary_sha256"][role] == identities[binary],
                        "wrong frozen binary: " + role)
        except (KeyError, OSError, ValueError, TypeError) as error:
            stage_errors.append(str(error))
            if base is None:
                summary_stages.append({"path": str(directory), "errors": stage_errors, "groups": []})
            if detailed is None:
                snapshot_stages.append({"path": str(directory), "errors": stage_errors, "runs": []})

        for name in expected_runs:
            state = name.rsplit("-", 1)[1]
            row = {"stage": stage["name"], "scenario": stage["scenario"], "version": stage["version"],
                   "snapshot": state, "name": name, "errors": list(stage_errors), "warnings": [],
                   "valid": False, "memory": None, "hold_delta_ns": None, "hold_acquisitions": None}
            rows.append(row)
            try:
                require(base is not None and detailed is not None and manifest is not None,
                        "stage artifacts unavailable")
                measured = next(item for group in base["groups"] for item in group["runs"] if item["name"] == name)
                detail = next(item for item in detailed["runs"] if item["name"] == name)["snapshot"]
                row["warnings"] = base["warnings"] + measured["warnings"] + detail["warnings"]
                row["errors"].extend(measured["errors"] + detail["errors"])
                require(measured["status"] == "valid", "client artifacts are not valid")
                case = next(item for item in manifest["plan"] if item["name"] == name)
                report = summarize.read_report(directory / name / "report.json",
                                               summarize.config_for(directory, manifest, case))
                final = load(directory / name / "stats-settled.json")
                size = report["config"]["value_size"]
                key_bytes = 588890  # sum(len('k' + str(i)) for i in range(100000)).
                data_bytes = 100000 * size + key_bytes
                snapshot_bytes = 28 + 100000 * (25 + size) + key_bytes
                sequence = 100000 + report["operations"]["put"]
                row.update(elapsed_ns=report["elapsed_ns"], requests=report["outcomes"]["requests"],
                           successes=report["outcomes"]["successes"], operations=report["operations"],
                           qps_successful=report["qps_successful"], latency_ns=report["latency_ns"],
                           expected_sequence=sequence, expected_data_bytes=data_bytes,
                           expected_snapshot_bytes=snapshot_bytes, memory=measured["memory"],
                           coverage=detail["coverage"], phases=detail["phases"], accounting=detail["accounting"])
                require(report["elapsed_ns"] >= thresholds["min_measurement_seconds"] * 1000000000,
                        "measurement shorter than frozen duration threshold")
                require(report["outcomes"]["requests"] == report["outcomes"]["successes"] == 1000000
                        and report["outcomes"]["logical_misses"] == report["outcomes"]["failures"] == 0,
                        "request count, success, miss or failure mismatch")
                require(report["preload"]["completed_keys"] == 100000 and report["operations"]["delete"] == 0,
                        "preload or DELETE count mismatch")
                engine = final["engine"]
                require(engine["keys"] == 100000 and engine["data_bytes"] == data_bytes,
                        "final key count or data size mismatch")
                require(engine["applied_sequence"] == engine["durable_sequence"] == sequence
                        and engine["wal_pending_bytes"] == 0, "final durable prefix mismatch")
                require(engine["snapshot_failures_total"] == engine["wal_commit_failures_total"] == 0
                        and engine["io_failed"] is False, "storage reported failure")
                for role in summarize.ROLES:
                    require(all(type(value) is int and value > 0 for value in measured["memory"][role].values()),
                            "missing or invalid memory observations: " + role)
                require(detail["status"] == "valid", "invalid snapshot observations")
                require(detail["coverage"]["failed_queries"] == 0, "a recorded status query failed")
                fields = list(detail["phases"].values()) + list(detail["accounting"].values())
                require(all(field["availability"] == "available" for field in fields),
                        "missing/invalid snapshot phase or accounting field")
                count = detail["coverage"]["completed"]
                acquisitions = detail["accounting"][snapshot_report.ACQUISITIONS]["delta"]
                hold = detail["accounting"][snapshot_report.HOLD_DURATION]["delta"]
                capture = detail["phases"]["capture"]["delta"]
                written = detail["accounting"]["snapshot_file_written_bytes_total"]["delta"]
                installed = detail["accounting"]["snapshot_file_installed_bytes_total"]["delta"]
                row.update(hold_delta_ns=hold, hold_acquisitions=acquisitions,
                           hold_mean_ns=hold / acquisitions if acquisitions else None)
                require(acquisitions == count and hold <= capture, "capture count/hold conservation failed")
                require(written == installed == count * snapshot_bytes, "snapshot byte conservation failed")
                if state == "on":
                    require(detail["coverage"]["status"] == "sufficient" and count >= thresholds["min_completed_snapshots"],
                            "ON common idle coverage is insufficient")
                else:
                    require(count == 0 and hold == 0, "OFF case captured an automatic snapshot")
                row["valid"] = not row["errors"]
            except (KeyError, OSError, ValueError, TypeError, StopIteration) as error:
                row["errors"].append(str(error))

    aggregate_counts = {name: sum(stage.get("counts", {}).get(name, 0) for stage in summary_stages)
                        for name in ("planned", *summarize.STATUSES, "unfinished", "snapshot_evidence_insufficient")}
    summary = {"schema_version": 1, "units": summarize.UNITS, "experiments": summary_stages,
               "counts": aggregate_counts}
    strict_counts = {"planned": 16, "valid": sum(row["valid"] for row in rows),
                     "invalid_or_insufficient": sum(not row["valid"] for row in rows)}
    snapshot = {"schema_version": 1, "thresholds": coverage_limits, "experiments": snapshot_stages}
    save(output / "summary.json", summary)
    save(output / "snapshot.json", snapshot)

    indexed = {(row["stage"], row["snapshot"]): row for row in rows}
    pairs = []
    for scenario in ("primary", "stress"):
        for state in ("off", "on"):
            for b_name, a_name in (("02-B", "01-A"), ("03-B", "04-A")):
                a = indexed.get((scenario + "-" + a_name, state))
                b = indexed.get((scenario + "-" + b_name, state))
                pair = {"scenario": scenario, "snapshot": state, "A": scenario + "-" + a_name,
                        "B": scenario + "-" + b_name, "measurements_valid": bool(a and b and a["valid"] and b["valid"]),
                        "metrics": {}, "pass": False, "errors": []}
                pairs.append(pair)
                try:
                    require(a is not None and b is not None, "missing pair member")
                    m = pair["metrics"]
                    m["qps"] = ratio_test(b["successes"] * a["elapsed_ns"], a["successes"] * b["elapsed_ns"], thresholds["qps_ratio_min"], ">=")
                    for field, limit in (("p99", "p99_ratio_max"), ("p99_9", "p99_9_ratio_max"), ("max", "max_latency_ratio_max")):
                        m[field] = ratio_test(b["latency_ns"][field], a["latency_ns"][field], thresholds[limit], "<=")
                    rss = "measurement_sampled_rss_max_bytes"
                    m["engine_rss"] = ratio_test(b["memory"]["engine"][rss], a["memory"]["engine"][rss], thresholds["engine_measurement_sampled_rss_max_ratio_max"], "<=")
                    if state == "on":
                        m["capture_hold"] = ratio_test(b["hold_delta_ns"] * a["hold_acquisitions"], a["hold_delta_ns"] * b["hold_acquisitions"], thresholds["capture_hold_mean_ratio_max"], "<=")
                    pair["pass"] = pair["measurements_valid"] and all(test["pass"] for test in m.values())
                except (KeyError, ValueError, TypeError) as error:
                    pair["errors"].append(str(error))

    groups = []
    for scenario in ("primary", "stress"):
        for state in ("off", "on"):
            for version in ("A", "B"):
                selected = [row for row in rows if (row["scenario"], row["snapshot"], row["version"]) == (scenario, state, version)]
                groups.append({"scenario": scenario, "snapshot": state, "version": version,
                               "valid": sum(row["valid"] for row in selected),
                               "qps": distribution([row.get("qps_successful") for row in selected]),
                               "p99_ns": distribution([row.get("latency_ns", {}).get("p99") for row in selected]),
                               "p99_9_ns": distribution([row.get("latency_ns", {}).get("p99_9") for row in selected]),
                               "max_ns": distribution([row.get("latency_ns", {}).get("max") for row in selected]),
                               "hold_mean_ns": distribution([row.get("hold_mean_ns") for row in selected]),
                               "engine_rss_bytes": distribution([(row.get("memory") or {}).get("engine", {}).get("measurement_sampled_rss_max_bytes") for row in selected]),
                               "engine_hwm_bytes": distribution([(row.get("memory") or {}).get("engine", {}).get("observed_lifetime_hwm_bytes") for row in selected])})
    failures = [{"scenario": pair["scenario"], "snapshot": pair["snapshot"], "B": pair["B"], "A": pair["A"], "metric": metric, **test}
                for pair in pairs for metric, test in pair["metrics"].items() if not test["pass"]]
    performance_pass = not errors and len(rows) == 16 and all(row["valid"] for row in rows) and len(pairs) == 8 and all(pair["pass"] for pair in pairs)
    storage_path = args.storage_checks.resolve() if args.storage_checks else root / "analysis" / "post-run-checks.json"
    storage = {"status": "pending_independent_check", "pass": False,
               "evidence": "recorded_historical_checks", "analyzer_reexecuted_checks": False,
               "current_database_directories_present": sum((root / row["stage"] / row["name"] / "data").is_dir()
                                                            for row in rows),
               "note": "This analyzer reads the saved independent check report; it does not rescan databases or authenticate omitted executable bytes."}
    if storage_path.exists():
        try:
            checks = load(storage_path)
            storage.update(path=storage_path.name, sha256=sha(storage_path),
                           status=checks["status"], counts=checks["counts"], errors=checks["errors"])
            require(checks["schema_version"] == 1 and checks["status"] == "pass" and checks["errors"] == [],
                    "independent storage checks failed")
            require(checks["plan"]["sha256"] == sha(root / "plan.json")
                    and checks["plan"]["execution_json_sha256"] == sha(root / "execution.json"),
                    "historical storage report belongs to different execution artifacts")
            expected_counts = {"stages": 8, "cases": 16, "frozen_files": 10, "stage_binary_copies": 24,
                               "storage_checks": 32, "valid_storage_checks": 32, "normal_process_exits": 48,
                               "original_processes_absent": 48, "unchanged_databases": 16}
            require(all(checks["counts"].get(name) == value for name, value in expected_counts.items()),
                    "independent storage check count mismatch")
            require(len(checks["cases"]) == 16 and all(case["data_unchanged"] is True for case in checks["cases"]),
                    "missing or modified data in independent storage checks")
            by_name = {row["stage"] + "/" + row["name"]: row for row in rows}
            require({case["name"] for case in checks["cases"]} == set(by_name), "historical storage case list differs")
            for case in checks["cases"]:
                require(case["status"] == "pass" and case["errors"] == [], "historical case did not pass")
                row = by_name[case["name"]]
                require(case["expected"] == {"keys": 100000, "data_bytes": row.get("expected_data_bytes"),
                                              "applied_sequence": row.get("expected_sequence")},
                        "historical storage expectations differ from measured workload")
                for filename, digest in case["input_sha256"].items():
                    require(sha(root / case["name"] / filename) == digest,
                            "historical storage input hash differs: " + case["name"] + "/" + filename)
                require(len(case["checks"]) == 2 and {check["version"] for check in case["checks"]} == {"A", "B"},
                        "historical A/B check evidence is incomplete")
                for check in case["checks"]:
                    require(check["valid"] is True and check["exit_code"] == 0 and check["stderr"] == "",
                            "historical engine check failed")
                    decoded = json.loads(check["stdout"])
                    require(decoded == check["parsed_json"] and decoded["schema_version"] == 1
                            and decoded["status"] == "valid" and decoded["wal_trailing_bytes"] == 0
                            and all(decoded[name] == value for name, value in case["expected"].items()),
                            "historical engine output disagrees with the expected recovered state")
            storage["pass"] = True
        except (KeyError, OSError, ValueError, TypeError) as error:
            storage.update(status="invalid", error=str(error))
    adoption = "adopted" if performance_pass and storage["pass"] else (
        "pending_storage_checks" if performance_pass and storage["status"] == "pending_independent_check" else "not_adopted")
    metric_counts = {metric: {"planned": 4 if metric == "capture_hold" else 8,
                             "observed": sum(metric in pair["metrics"] for pair in pairs),
                             "pass": sum(pair["measurements_valid"] and pair["metrics"].get(metric, {}).get("pass", False)
                                         for pair in pairs)}
                     for metric in ("qps", "p99", "p99_9", "max", "engine_rss", "capture_hold")}
    compact = {"adoption": adoption, "case_counts": strict_counts, "metric_counts": metric_counts,
               "threshold_failure_count": len(failures), "scenario_summary": []}
    for scenario in ("primary", "stress"):
        on_pairs = [pair for pair in pairs if pair["scenario"] == scenario and pair["snapshot"] == "on"]
        on_groups = {group["version"]: group for group in groups if group["scenario"] == scenario and group["snapshot"] == "on"}
        compact["scenario_summary"].append({
            "scenario": scenario,
            "on_hold_mean_ms_run_medians": {version: group["hold_mean_ns"]["median"] / 1e6 if group["hold_mean_ns"]["median"] is not None else None
                                            for version, group in on_groups.items()},
            "on_engine_rss_mib_run_medians": {version: group["engine_rss_bytes"]["median"] / 2**20 if group["engine_rss_bytes"]["median"] is not None else None
                                              for version, group in on_groups.items()},
            "on_pair_ratios": {metric: [pair["metrics"].get(metric, {}).get("ratio") for pair in on_pairs]
                                for metric in metric_counts},
        })
    result = {"schema_version": 1, "plan_sha256": sha(root / "plan.json"),
              "analyzer_sha256": sha(Path(__file__)), "thresholds": thresholds,
              "errors": errors, "warnings": warnings, "case_counts": strict_counts,
              "rows": rows, "pairs": pairs, "groups": groups, "threshold_failures": failures,
              "current_stage_binary_verification": {Path(stage["path"]).name: stage.get("binary_verification")
                                                    for stage in summary_stages},
              "performance_thresholds_pass": performance_pass,
              "post_run_storage_checks": storage, "compact_summary": compact,
              "adoption": adoption,
              "interpretation": "Frozen engineering gate; failure does not establish causal regression or statistical significance. Group medians are distributions of per-run metrics, never pooled quantiles."}
    save(output / "analysis.json", result)
    save(output / "compact-summary.json", compact)
    table = ["stage snapshot valid QPS P99_ms P99.9_ms max_ms RSS_MiB lifetime_HWM_MiB hold_ms"]
    for row in rows:
        latency = row.get("latency_ns", {})
        rss = (row.get("memory") or {}).get("engine", {}).get("measurement_sampled_rss_max_bytes")
        hwm = (row.get("memory") or {}).get("engine", {}).get("observed_lifetime_hwm_bytes")
        values = [row.get("qps_successful"), *[latency.get(field) / 1e6 if latency.get(field) is not None else None for field in ("p99", "p99_9", "max")],
                  rss / 2**20 if rss is not None else None,
                  hwm / 2**20 if hwm is not None else None,
                  row.get("hold_mean_ns") / 1e6 if row.get("hold_mean_ns") is not None else None]
        table.append(" ".join([row["stage"], row["snapshot"], str(row["valid"])] +
                              ["null" if value is None else "{:.6f}".format(value) for value in values]))
    (output / "table.txt").write_text("\n".join(table) + "\n", encoding="utf-8")
    print("\n".join(table))
    print(json.dumps({"case_counts": result["case_counts"], "threshold_failure_count": len(failures),
                      "performance_thresholds_pass": performance_pass, "adoption": result["adoption"]}, sort_keys=True))
    # A valid analysis can conclude not_adopted. Exit status validates the
    # evidence, not whether the frozen performance adoption thresholds passed.
    return 0 if not errors and all(row["valid"] for row in rows) and storage["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
