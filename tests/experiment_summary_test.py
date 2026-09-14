#!/usr/bin/env python3
"""Read-only CLI checks for aggregating recorded experiment artifacts."""

import copy
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SUMMARY = ROOT / "benmark" / "summarize.py"
ROLES = ("engine", "gateway", "bench")


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def file_snapshot(directory):
    snapshot = {}
    for path in sorted(directory.rglob("*")):
        relative = str(path.relative_to(directory))
        if path.is_symlink():
            snapshot[relative] = ("symlink", os.readlink(path), path.lstat().st_mtime_ns)
        elif path.is_file():
            snapshot[relative] = (path.read_bytes(), path.stat().st_mtime_ns, path.stat().st_mode)
        else:
            snapshot[relative] = ("directory",)
    return snapshot


class ExperimentSummaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="minikv-summary-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def experiment(self, name="experiment", seed=1, binary_version="A"):
        directory = self.directory / name
        directory.mkdir()
        binaries = directory / "binaries"
        binaries.mkdir()
        executables, identities = {}, {}
        for role in ROLES:
            binary = binaries / role
            payload = ("fixture-%s-%s\n" % (role, binary_version)).encode()
            binary.write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            executables[role] = {"path": str(binary), "sha256": digest}
            identities[role] = {"path": str(binary), "sha256": digest, "available": True,
                                "size_bytes": len(payload), "errors": []}
        arguments = {
            "output": str(directory), "engine": executables["engine"]["path"],
            "gateway": executables["gateway"]["path"], "bench": executables["bench"]["path"],
            "modes": ["throughput"], "repeats": 1, "requests": 100, "workers": 2,
            "keyspace": 10, "op": "mixed", "write_ratio": 20, "delete_ratio": 5, "value_size": 128,
            "seed": seed, "engine_workers": 4, "rpc_pool": 32, "gomaxprocs": 4,
            "wal_batch": 64, "wal_flush_ms": 2, "snapshot_ms": 1000, "sample_ms": 100,
            "stats_ms": 250, "run_timeout": 120, "startup_timeout": 10,
            "shutdown_timeout": 15, "settle_timeout": 10,
        }
        manifest = {
            "schema_version": 1, "started_at": "2026-09-14T08:00:00Z", "arguments": arguments,
            "plan": [], "executables": executables,
            "runtime_environment": {"PATH": os.defpath, "LANG": "C", "LC_ALL": "C", "TZ": "UTC",
                                    "GOMAXPROCS": "4", "GOGC": "100", "GOMEMLIMIT": "off", "GODEBUG": ""},
            "metadata": {"schema_version": 1, "binaries": identities,
                         "git": {"head": "a" * 40, "dirty": False, "tracked_diff_sha256": "b" * 64}},
        }
        save_json(directory / "manifest.json", manifest)
        save_json(directory / "index.json", {"schema_version": 1, "status": "complete", "runs": [],
                                             "successful_runs": 0, "failed_runs": 0})
        return directory

    def add_run(self, directory, repeat=1, mode="throughput", interval=0, qps=100, p99_ms=3,
                status="ok", rss=20, hwm=100, snapshot_evidence=True, missing=False):
        manifest = json.loads((directory / "manifest.json").read_text())
        name = "r%02d-%s-snapshot-%s" % (repeat, mode, "on" if interval else "off")
        old_names = {case["name"] for case in manifest["plan"]}
        arguments = manifest["arguments"]
        arguments["repeats"] = max(arguments["repeats"], repeat)
        if mode not in arguments["modes"]:
            arguments["modes"].append(mode)
        complete = []
        for run_repeat in range(1, arguments["repeats"] + 1):
            for run_mode in arguments["modes"]:
                for run_interval in (0, arguments["snapshot_ms"]):
                    run_name = "r%02d-%s-snapshot-%s" % (run_repeat, run_mode, "on" if run_interval else "off")
                    complete.append({"name": run_name, "wal_mode": run_mode, "snapshot_interval_ms": run_interval})
        # Fixture construction always records the whole experiment matrix.
        # Preserve existing order (and deliberately missing run directories),
        # putting the first explicit case first for convenient group assertions.
        new_cases = [case for case in complete if case["name"] not in old_names]
        new_cases.sort(key=lambda case: case["name"] != name)
        manifest["plan"].extend(new_cases)
        save_json(directory / "manifest.json", manifest)
        for case in new_cases:
            if case["name"] != name:
                self.write_run(directory, case["name"], case["wal_mode"], case["snapshot_interval_ms"])
        return self.write_run(directory, name, mode, interval, qps, p99_ms, status, rss, hwm, snapshot_evidence, missing)

    def write_run(self, directory, name, mode, interval, qps=100, p99_ms=3,
                  status="ok", rss=20, hwm=100, snapshot_evidence=True, missing=False):
        manifest = json.loads((directory / "manifest.json").read_text())
        run = directory / name
        if run.exists():
            shutil.rmtree(run)
        if missing:
            return run
        run.mkdir()
        args = manifest["arguments"]
        case_number = next(index for index, case in enumerate(manifest["plan"]) if case["name"] == name)
        engine_port, http_port = 19000 + case_number, 18000 + case_number
        endpoint = "http://127.0.0.1:%d/kv" % http_port
        config = {"url": endpoint, "workers": args["workers"], "requests": args["requests"],
                  "operation": args["op"], "keyspace": args["keyspace"], "timeout_ns": 2000000000,
                  "write_ratio": args["write_ratio"], "delete_ratio": args["delete_ratio"],
                  "preload": True, "preload_count": 0, "seed": args["seed"], "value_size": args["value_size"]}
        command = [manifest["executables"]["bench"]["path"], "-url", endpoint,
                   "-workers", str(args["workers"]), "-requests", str(args["requests"]), "-op", args["op"],
                   "-keyspace", str(args["keyspace"]), "-timeout", "2s", "-write-ratio", str(args["write_ratio"]),
                   "-delete-ratio", str(args["delete_ratio"]), "-preload=true", "-preload-count", "0",
                   "-seed", str(args["seed"]), "-value-size", str(args["value_size"]), "-format", "json"]
        commands = {"cwd": str(ROOT), "runtime_environment": manifest["runtime_environment"],
                    "engine": {"argv": [manifest["executables"]["engine"]["path"]], "environment": {
                        "MINIKV_DATA_DIR": str(run / "data"), "MINIKV_ENGINE_HOST": "127.0.0.1",
                        "MINIKV_ENGINE_PORT": str(engine_port), "MINIKV_WAL_MODE": mode,
                        "MINIKV_WAL_BATCH_SIZE": "64", "MINIKV_WAL_FLUSH_MS": "2",
                        "MINIKV_WAL_QUEUE_BYTES": "16777216", "MINIKV_SNAPSHOT_INTERVAL_MS": str(interval),
                        "MINIKV_WORKERS": "4", "MINIKV_REQUEST_QUEUE_SIZE": "128",
                        "MINIKV_MAX_CONNECTIONS": "256", "MINIKV_CLIENT_IDLE_MS": "30000"}},
                    "gateway": {"argv": [manifest["executables"]["gateway"]["path"]], "environment": {
                        "MINIKV_ENGINE_ADDR": "127.0.0.1:%d" % engine_port,
                        "MINIKV_HTTP_ADDR": "127.0.0.1:%d" % http_port,
                        "MINIKV_RPC_POOL_SIZE": "32", "MINIKV_RPC_TIMEOUT_MS": "2000"}},
                    "bench": {"argv": command, "environment": {}}}
        save_json(run / "commands.json", commands)
        result = {
            "schema_version": 1, "name": name, "wal_mode": mode, "snapshot_interval_ms": interval,
            "started_at": "2026-09-14T08:00:00Z", "finished_at": "2026-09-14T08:00:02Z",
            "status": status, "errors": [] if status == "ok" else ["fixture " + status],
            "observations": {"alignment": "wall_clock_approximate", "stats_samples": 4, "stats_errors": 0,
                             "measurement_stats_samples": 2, "measurement_observed_snapshot_completions": 1 if interval and snapshot_evidence else None,
                             "measurement_snapshot_in_progress_observed": False,
                             "snapshot_activity_observed": not snapshot_evidence if interval else None,
                             "memory": {role: {"measurement_sampled_rss_max_bytes": 999999,
                                               "observed_lifetime_hwm_bytes": 999999,
                                               "measurement_rss_samples": 99} for role in ROLES}},
            # A summarizer must derive measurements from the raw report rather
            # than blindly trusting stale cached metrics in result or index.
            "metrics": {"qps_successful": 999999, "p99_ns": 999999999, "failures": 0, "logical_misses": 0},
            "processes": {role: {"pid": 1234 + i, "forced": False, "returncode": 0} for i, role in enumerate(ROLES)},
        }
        save_json(run / "result.json", result)
        count, p99 = args["requests"], int(p99_ms * 1000000)
        preload_count = args["keyspace"] if args["op"] in ("get", "mixed") else 0
        operations = ({"put": 20, "get": 75, "delete": 5} if args["op"] == "mixed"
                      else {name: count if name == args["op"] else 0 for name in ("put", "get", "delete")})
        report = {
            "schema_version": 1, "started_at": "2026-09-14T08:00:00Z", "measurement_started_at": "2026-09-14T08:00:00Z",
            "complete": True, "load_model": "closed_loop", "workload_generator": "indexed-pcg-v1",
            "config": config, "client_build": {"go_version": "go1.22.0", "goos": "linux", "goarch": "amd64"},
            "preload": {"target_keys": preload_count, "completed_keys": preload_count, "elapsed_ns": 1000},
            "elapsed_ns": int(count * 1000000000 / qps),
            "outcomes": {"requests": count, "successes": count, "logical_misses": 0, "failures": 0,
                         "network_errors": 0, "timeouts": 0, "transport_errors": 0, "http_failures": 0, "protocol_failures": 0},
            "operations": operations, "http_statuses": {"200": count},
            "latency_ns": {"samples": count, "mean": 1500000, "min": 1000000, "p50": 1000000,
                           "p95": 2000000, "p99": p99, "p99_9": p99, "max": p99 + 1000000},
            "qps_total": qps, "qps_successful": qps, "system_success_rate_pct": 100,
        }
        save_json(run / "report.json", report)
        expected_sequence = report["preload"]["completed_keys"] + report["operations"]["put"] + report["operations"]["delete"]
        stats, resources = [], []
        for number, millisecond in enumerate((50, 100)):
            timestamp = "2026-09-14T08:00:00.%03dZ" % millisecond
            body = {"schema_version": 1,
                    "engine": {"wal_mode": mode, "keys": 10, "applied_sequence": expected_sequence, "durable_sequence": expected_sequence,
                               "wal_pending_bytes": 0, "snapshot_successes_total": 1 + (number if interval and snapshot_evidence else 0),
                               "snapshot_failures_total": 0, "snapshot_in_progress": False, "io_failed": False, "stopping": False},
                    "server": {"workers_capacity": 4}, "gateway": {"rpc": {"pool_capacity": 32}}}
            stats.append({"started_at": timestamp, "finished_at": timestamp, "monotonic_start_ns": millisecond * 1000000,
                          "monotonic_end_ns": millisecond * 1000000, "http_status": 200, "body": body, "error": None})
            resources.append({"sampled_at": timestamp, "monotonic_ns": millisecond * 1000000,
                              "processes": {role: {"pid": 1234 + i, "sampled_at_utc": timestamp,
                                                   "monotonic_ns": millisecond * 1000000, "starttime_ticks": 100,
                                                   "available": rss is not None and hwm is not None,
                                                   "errors": [] if rss is not None and hwm is not None else ["fixture memory unavailable"],
                                                   "rss_bytes": rss, "hwm_bytes": hwm, "cpu_user_ticks": 2,
                                                   "cpu_system_ticks": 1, "read_bytes": 0, "write_bytes": 0,
                                                   "cancelled_write_bytes": 0} for i, role in enumerate(ROLES)}})
        (run / "stats.jsonl").write_text("".join(json.dumps(sample) + "\n" for sample in stats))
        (run / "resources.jsonl").write_text("".join(json.dumps(sample) + "\n" for sample in resources))
        before = copy.deepcopy(stats[0]["body"])
        before["engine"].update(keys=0, applied_sequence=0, durable_sequence=0)
        save_json(run / "stats-before.json", before)
        save_json(run / "stats-after.json", stats[-1]["body"])
        save_json(run / "stats-settled.json", stats[-1]["body"])
        index = json.loads((directory / "index.json").read_text())
        index["runs"] = [row for row in index["runs"] if row["name"] != name]
        index["runs"].append(result)
        index["successful_runs"] = sum(row["status"] == "ok" for row in index["runs"])
        index["failed_runs"] = len(index["runs"]) - index["successful_runs"]
        save_json(directory / "index.json", index)
        return run

    def invoke(self, directories, format="json", expected=0):
        before = {directory: file_snapshot(directory) for directory in directories}
        result = subprocess.run([sys.executable, str(SUMMARY), *map(str, directories), "--format", format],
                                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=8)
        self.assertEqual(result.returncode, expected, "stdout=%s\nstderr=%s" % (result.stdout, result.stderr))
        for directory in directories:
            self.assertEqual(file_snapshot(directory), before[directory], "summarizer changed source experiment artifacts")
        if format == "json" and result.stdout:
            parsed = json.loads(result.stdout)
            self.assertEqual(parsed["schema_version"], 1)
            return parsed
        return result

    def test_repeated_runs_preserve_values_and_report_distributions(self):
        directory = self.experiment()
        for repeat, qps, p99, rss, hwm in ((1, 100, 3, None, 100), (2, 200, 9, 20, 300), (3, 500, 6, 40, 200)):
            self.add_run(directory, repeat=repeat, qps=qps, p99_ms=p99, rss=rss, hwm=hwm)
        summary = self.invoke([directory])
        self.assertEqual(summary["units"], {"qps_successful": "requests/s", "p99_ms": "ms", "memory": "bytes"})
        experiment = summary["experiments"][0]
        self.assertEqual(Path(experiment["path"]), directory)
        self.assertEqual(len(experiment["groups"]), 2)
        group = experiment["groups"][0]
        self.assertEqual(group["counts"]["planned"], 3)
        self.assertEqual(group["counts"]["valid"], 3)
        self.assertEqual(sorted(run["qps_successful"] for run in group["runs"]), [100, 200, 500])
        self.assertEqual(group["distributions"]["qps_successful"], {"n": 3, "min": 100, "median": 200, "max": 500})
        self.assertEqual(group["distributions"]["p99_ms"], {"n": 3, "min": 3, "median": 6, "max": 9})
        for role in ROLES:
            memory = group["distributions"]["memory"][role]
            self.assertEqual(memory["measurement_sampled_rss_max_bytes"], {"n": 2, "min": 20, "median": 30, "max": 40})
            self.assertEqual(memory["observed_lifetime_hwm_bytes"], {"n": 3, "min": 100, "median": 200, "max": 300})

    def test_failed_missing_and_unfinished_runs_stay_visible(self):
        directory = self.experiment()
        self.add_run(directory, repeat=1)
        self.add_run(directory, repeat=2, status="failed", qps=500)
        self.add_run(directory, repeat=3, missing=True)
        self.add_run(directory, repeat=4, status="running")
        self.add_run(directory, repeat=5, status="interrupted")
        summary = self.invoke([directory], expected=1)
        group = summary["experiments"][0]["groups"][0]
        self.assertEqual(group["counts"]["planned"], 5)
        for status in ("valid", "failed", "missing", "incomplete", "interrupted"):
            self.assertEqual(group["counts"][status], 1)
        self.assertEqual(len(group["runs"]), 5)
        self.assertEqual(group["distributions"]["qps_successful"], {"n": 1, "min": 100, "median": 100, "max": 100})
        self.assertEqual(summary["counts"]["valid"], 6)

    def test_absent_resources_are_not_reported_as_zero(self):
        directory = self.experiment()
        self.add_run(directory, rss=None, hwm=None)
        summary = self.invoke([directory])
        group = summary["experiments"][0]["groups"][0]
        for role in ROLES:
            for field in ("measurement_sampled_rss_max_bytes", "observed_lifetime_hwm_bytes"):
                self.assertIsNone(group["runs"][0]["memory"][role][field])
                self.assertEqual(group["distributions"]["memory"][role][field], {"n": 0, "min": None, "median": None, "max": None})

    def test_missing_optional_samples_do_not_turn_cached_observations_into_evidence(self):
        directory = self.experiment()
        run = self.add_run(directory, interval=1000)
        (run / "resources.jsonl").unlink()
        (run / "stats.jsonl").unlink()
        summary = self.invoke([directory])
        group = summary["experiments"][0]["groups"][0]
        self.assertEqual(group["counts"]["valid"], 1)
        self.assertEqual(group["counts"]["snapshot_evidence_insufficient"], 1)
        self.assertFalse(group["runs"][0]["snapshot_comparison_eligible"])
        self.assertTrue(group["runs"][0]["warnings"])
        self.assertEqual(group["distributions"]["memory"]["engine"]["measurement_sampled_rss_max_bytes"]["n"], 0)

    def test_snapshot_mode_groups_and_missing_evidence_are_explicit(self):
        directory = self.experiment()
        self.add_run(directory)
        self.add_run(directory, interval=1000, snapshot_evidence=False)
        self.add_run(directory, mode="reliable", interval=1000, snapshot_evidence=True)
        summary = self.invoke([directory])
        groups = {(group["wal_mode"], group["snapshot_interval_ms"]): group for group in summary["experiments"][0]["groups"]}
        self.assertEqual(set(groups), {("throughput", 0), ("throughput", 1000), ("reliable", 0), ("reliable", 1000)})
        self.assertEqual(groups[("throughput", 1000)]["counts"]["snapshot_evidence_insufficient"], 1)
        self.assertFalse(groups[("throughput", 1000)]["runs"][0]["snapshot_activity_observed"])
        self.assertEqual(groups[("throughput", 0)]["counts"]["snapshot_evidence_insufficient"], 0)
        self.assertTrue(groups[("reliable", 1000)]["runs"][0]["snapshot_activity_observed"])
        self.assertTrue(groups[("reliable", 1000)]["runs"][0]["snapshot_comparison_eligible"])
        text = self.invoke([directory], format="text").stdout.lower()
        self.assertIn("snapshot", text)
        self.assertTrue("evidence" in text or "insufficient" in text or "证据" in text, text)

    def test_optional_sampling_sources_preserve_independent_evidence(self):
        for missing in ("stats.jsonl", "resources.jsonl"):
            with self.subTest(missing=missing):
                directory = self.experiment("without-" + missing)
                run = self.add_run(directory, interval=1000)
                (run / missing).unlink()
                summary = self.invoke([directory])
                group = summary["experiments"][0]["groups"][0]
                row = group["runs"][0]
                self.assertEqual(row["status"], "valid")
                self.assertTrue(row["warnings"])
                if missing == "stats.jsonl":
                    self.assertEqual(row["memory"]["engine"]["measurement_sampled_rss_max_bytes"], 20)
                    self.assertEqual(row["memory"]["engine"]["observed_lifetime_hwm_bytes"], 100)
                    self.assertFalse(row["snapshot_comparison_eligible"])
                else:
                    self.assertIsNone(row["memory"]["engine"]["measurement_sampled_rss_max_bytes"])
                    self.assertTrue(row["snapshot_activity_observed"])
                    self.assertTrue(row["snapshot_comparison_eligible"])

    def test_out_of_range_memory_samples_do_not_crash_or_pollute_distributions(self):
        directory = self.experiment()
        for repeat in (1, 2):
            self.add_run(directory, repeat=repeat, rss=10 ** 1000, hwm=10 ** 1000)
        summary = self.invoke([directory])
        group = summary["experiments"][0]["groups"][0]
        self.assertEqual(group["counts"]["valid"], 2)
        for row in group["runs"]:
            self.assertTrue(row["warnings"])
        for role in ROLES:
            for field in ("measurement_sampled_rss_max_bytes", "observed_lifetime_hwm_bytes"):
                self.assertEqual(group["distributions"]["memory"][role][field], {"n": 0, "min": None, "median": None, "max": None})

    def test_resource_identity_mismatch_discards_only_the_affected_role(self):
        faults = ("wrong_pid", "missing_pid", "boolean_pid", "missing_starttime",
                  "boolean_starttime", "negative_starttime", "changed_starttime")
        for fault in faults:
            with self.subTest(fault=fault):
                directory = self.experiment("resource-identity-" + fault)
                run = self.add_run(directory, interval=1000)
                path = run / "resources.jsonl"
                samples = [json.loads(line) for line in path.read_text().splitlines()]
                # A later sample with an untrustworthy identity must invalidate
                # even the earlier measurements for this role, without erasing
                # independent snapshot evidence or other process measurements.
                process = samples[-1]["processes"]["engine"]
                if fault == "wrong_pid":
                    process["pid"] += 100
                elif fault == "missing_pid":
                    del process["pid"]
                elif fault == "boolean_pid":
                    process["pid"] = True
                elif fault == "missing_starttime":
                    del process["starttime_ticks"]
                elif fault == "boolean_starttime":
                    for sample in samples:
                        sample["processes"]["engine"]["starttime_ticks"] = False
                elif fault == "negative_starttime":
                    for sample in samples:
                        sample["processes"]["engine"]["starttime_ticks"] = -1
                else:
                    process["starttime_ticks"] += 1
                path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
                summary = self.invoke([directory])
                group = summary["experiments"][0]["groups"][0]
                row = group["runs"][0]
                self.assertEqual(row["status"], "valid")
                self.assertEqual(row["qps_successful"], 100)
                self.assertTrue(row["snapshot_activity_observed"])
                self.assertTrue(row["snapshot_comparison_eligible"])
                self.assertTrue(row["warnings"])
                for field in ("measurement_sampled_rss_max_bytes", "observed_lifetime_hwm_bytes"):
                    self.assertIsNone(row["memory"]["engine"][field])
                    self.assertEqual(group["distributions"]["memory"]["engine"][field]["n"], 0)
                for role in ("gateway", "bench"):
                    self.assertEqual(row["memory"][role]["measurement_sampled_rss_max_bytes"], 20)
                    self.assertEqual(row["memory"][role]["observed_lifetime_hwm_bytes"], 100)

    def test_unknown_recorded_process_identity_is_invalid_without_resource_sampling(self):
        for fault in ("missing_pid", "boolean_pid"):
            with self.subTest(fault=fault):
                directory = self.experiment("recorded-identity-" + fault)
                run = self.add_run(directory, interval=1000)
                path = run / "result.json"
                result = json.loads(path.read_text())
                if fault == "missing_pid":
                    del result["processes"]["engine"]["pid"]
                else:
                    result["processes"]["engine"]["pid"] = True
                (run / "resources.jsonl").unlink()
                save_json(path, result)
                summary = self.invoke([directory], expected=1)
                row = summary["experiments"][0]["groups"][0]["runs"][0]
                self.assertEqual(row["status"], "invalid")
                self.assertIsNone(row["qps_successful"])
                self.assertTrue(row["errors"])

    def test_zero_starttime_and_partial_resource_samples_remain_usable(self):
        directory = self.experiment()
        run = self.add_run(directory, rss=None, hwm=100)
        path = run / "resources.jsonl"
        samples = [json.loads(line) for line in path.read_text().splitlines()]
        for sample in samples:
            for process in sample["processes"].values():
                process["starttime_ticks"] = 0
                self.assertFalse(process["available"])
        path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
        summary = self.invoke([directory])
        group = summary["experiments"][0]["groups"][0]
        self.assertEqual(group["counts"]["valid"], 1)
        for role in ROLES:
            self.assertIsNone(group["runs"][0]["memory"][role]["measurement_sampled_rss_max_bytes"])
            self.assertEqual(group["runs"][0]["memory"][role]["observed_lifetime_hwm_bytes"], 100)
            self.assertEqual(group["distributions"]["memory"][role]["observed_lifetime_hwm_bytes"],
                             {"n": 1, "min": 100, "median": 100, "max": 100})

    def test_unavailable_process_exit_sample_preserves_identified_memory_history(self):
        for position in ("before", "after"):
            with self.subTest(position=position):
                directory = self.experiment("process-disappeared-" + position)
                run = self.add_run(directory)
                path = run / "resources.jsonl"
                samples = [json.loads(line) for line in path.read_text().splitlines()]
                disappeared = copy.deepcopy(samples[-1])
                process = disappeared["processes"]["bench"]
                process.update(available=False, errors=["stat_after: process disappeared or identity changed"],
                               starttime_ticks=None, rss_bytes=None, hwm_bytes=None,
                               cpu_user_ticks=None, cpu_system_ticks=None, read_bytes=None,
                               write_bytes=None, cancelled_write_bytes=None)
                if position == "before":
                    samples.insert(0, disappeared)
                else:
                    samples.append(disappeared)
                path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
                summary = self.invoke([directory])
                group = summary["experiments"][0]["groups"][0]
                row = group["runs"][0]
                self.assertEqual(row["status"], "valid")
                self.assertTrue(any("bench resource sample unavailable" in warning for warning in row["warnings"]))
                for role in ROLES:
                    self.assertEqual(row["memory"][role]["measurement_sampled_rss_max_bytes"], 20)
                    self.assertEqual(row["memory"][role]["observed_lifetime_hwm_bytes"], 100)
                self.assertEqual(group["distributions"]["memory"]["bench"]["observed_lifetime_hwm_bytes"],
                                 {"n": 1, "min": 100, "median": 100, "max": 100})

    def test_different_workload_generators_with_the_same_binary_cannot_be_pooled(self):
        directory = self.experiment()
        self.add_run(directory, repeat=1)
        changed = self.add_run(directory, repeat=2)
        report = json.loads((changed / "report.json").read_text())
        report["workload_generator"] = "different-indexed-stream-v2"
        save_json(changed / "report.json", report)
        summary = self.invoke([directory], expected=1)
        self.assertEqual(summary["counts"]["valid"], 0)
        self.assertEqual(summary["counts"]["invalid"], 4)
        self.assertEqual(summary["experiments"][0]["groups"][0]["distributions"]["qps_successful"]["n"], 0)

    def test_experiments_with_different_configuration_or_binary_are_never_pooled(self):
        directories = [self.experiment("original"), self.experiment("different seed", seed=2),
                       self.experiment("different binary", binary_version="B"), self.experiment("same config new run")]
        for directory, qps in zip(directories, (100, 200, 500, 400)):
            self.add_run(directory, qps=qps)
        summary = self.invoke(directories)
        self.assertEqual(len(summary["experiments"]), 4)
        by_path = {Path(experiment["path"]): experiment for experiment in summary["experiments"]}
        self.assertEqual(set(by_path), set(directories))
        for directory, qps in zip(directories, (100, 200, 500, 400)):
            self.assertEqual(by_path[directory]["groups"][0]["distributions"]["qps_successful"],
                             {"n": 1, "min": qps, "median": qps, "max": qps})

    def test_bad_or_missing_reports_are_excluded_even_when_index_says_ok(self):
        for fault in ("missing", "json", "counts", "rates", "nonfinite", "incomplete"):
            with self.subTest(fault=fault):
                directory = self.experiment(fault)
                run = self.add_run(directory)
                path = run / "report.json"
                if fault == "missing":
                    path.unlink()
                elif fault == "json":
                    path.write_text("not a report\n")
                else:
                    report = json.loads(path.read_text())
                    if fault == "counts":
                        report["outcomes"]["requests"] += 1
                    elif fault == "rates":
                        report["qps_successful"] = 1
                    elif fault == "nonfinite":
                        report["qps_successful"] = float("nan")
                    else:
                        report["complete"] = False
                    save_json(path, report)
                summary = self.invoke([directory], expected=1)
                group = summary["experiments"][0]["groups"][0]
                self.assertEqual(group["counts"]["valid"], 0)
                self.assertNotEqual(group["runs"][0]["status"], "valid")
                self.assertTrue(group["runs"][0]["errors"])
                self.assertEqual(group["distributions"]["qps_successful"]["n"], 0)

    def test_duration_overflow_produces_invalid_runs_and_complete_json(self):
        for fault in ("elapsed", "preload", "latency"):
            with self.subTest(fault=fault):
                directory = self.experiment("duration-" + fault)
                for repeat in (1, 2):
                    run = self.add_run(directory, repeat=repeat)
                    path = run / "report.json"
                    report = json.loads(path.read_text())
                    if fault == "elapsed":
                        report["elapsed_ns"] = 1 << 63
                        report["qps_total"] = report["qps_successful"] = report["outcomes"]["requests"] * 1000000000 / report["elapsed_ns"]
                    elif fault == "preload":
                        report["preload"]["elapsed_ns"] = 1 << 63
                    else:
                        # Each P99_ms is finite, but their unvalidated median
                        # would overflow and leave a partially written JSON file.
                        for field in ("mean", "min", "p50", "p95", "p99", "p99_9", "max"):
                            report["latency_ns"][field] = 10 ** 314
                    save_json(path, report)
                summary = self.invoke([directory], expected=1)
                group = summary["experiments"][0]["groups"][0]
                self.assertEqual(group["counts"]["invalid"], 2)
                self.assertEqual(group["distributions"]["p99_ms"], {"n": 0, "min": None, "median": None, "max": None})
                self.assertTrue(all(row["errors"] for row in group["runs"]))

    def test_pure_operation_counts_must_match_the_configured_workload(self):
        for operation in ("put", "get", "delete"):
            with self.subTest(operation=operation):
                directory = self.experiment("pure-" + operation)
                manifest_path = directory / "manifest.json"
                manifest = json.loads(manifest_path.read_text())
                manifest["arguments"]["op"] = operation
                save_json(manifest_path, manifest)
                run = self.add_run(directory)
                self.invoke([directory])
                path = run / "report.json"
                report = json.loads(path.read_text())
                wrong = "get" if operation != "get" else "put"
                report["operations"] = {name: report["outcomes"]["requests"] if name == wrong else 0
                                        for name in ("put", "get", "delete")}
                save_json(path, report)
                expected_sequence = report["preload"]["completed_keys"] + report["operations"]["put"] + report["operations"]["delete"]
                for phase in ("after", "settled"):
                    path = run / ("stats-" + phase + ".json")
                    stats = json.loads(path.read_text())
                    stats["engine"].update(applied_sequence=expected_sequence, durable_sequence=expected_sequence)
                    save_json(path, stats)
                summary = self.invoke([directory], expected=1)
                row = summary["experiments"][0]["groups"][0]["runs"][0]
                self.assertEqual(row["status"], "invalid")
                self.assertTrue(any("configured workload" in error for error in row["errors"]))

    def test_per_run_commands_cannot_silently_change_the_controlled_load_or_binary(self):
        for fault in ("load", "binary", "runtime_environment"):
            with self.subTest(fault=fault):
                directory = self.experiment("commands-" + fault)
                run = self.add_run(directory)
                commands = json.loads((run / "commands.json").read_text())
                if fault == "load":
                    # Change both the command and report coherently; the
                    # experiment's original fixed seed must still be enforced.
                    args = commands["bench"]["argv"]
                    args[args.index("-seed") + 1] = "2"
                    report = json.loads((run / "report.json").read_text())
                    report["config"]["seed"] = 2
                    save_json(run / "report.json", report)
                elif fault == "binary":
                    commands["engine"]["argv"] = [str(directory / "rebuilt-engine")]
                else:
                    commands["runtime_environment"]["GOMAXPROCS"] = "64"
                save_json(run / "commands.json", commands)
                summary = self.invoke([directory], expected=1)
                group = summary["experiments"][0]["groups"][0]
                self.assertEqual(group["runs"][0]["status"], "invalid")
                self.assertEqual(group["distributions"]["qps_successful"]["n"], 0)

    def test_changed_preserved_binary_is_rejected(self):
        directory = self.experiment()
        self.add_run(directory)
        (directory / "binaries" / "engine").write_bytes(b"different binary\n")
        summary = self.invoke([directory], expected=1)
        self.assertEqual(summary["counts"]["valid"], 0)
        self.assertEqual(summary["counts"]["invalid"], 2)
        self.assertTrue(summary["experiments"][0]["errors"])

    def test_portable_archive_without_copies_uses_recorded_hashes_with_warning(self):
        original = self.experiment("original archive location")
        self.add_run(original)
        archive = self.directory / "portable archive"
        original.rename(archive)
        shutil.rmtree(archive / "binaries")
        # The old absolute locations are unrelated now and contain deliberately
        # wrong bytes. Only the archive's preserved metadata should be consulted.
        (original / "binaries").mkdir(parents=True)
        for role in ROLES:
            (original / "binaries" / role).write_bytes(b"not the recorded executable\n")
        summary = self.invoke([archive])
        experiment = summary["experiments"][0]
        self.assertEqual(experiment["binary_verification"], "recorded_hashes_only")
        self.assertTrue(experiment["warnings"])
        self.assertEqual(experiment["counts"]["valid"], 2)

    def test_missing_or_failed_cleanup_cannot_masquerade_as_a_valid_run(self):
        for fault in ("missing_role", "nonzero_exit", "forced", "boolean_exit", "integer_forced", "zero_pid", "negative_pid"):
            with self.subTest(fault=fault):
                directory = self.experiment("cleanup-" + fault)
                run = self.add_run(directory)
                path = run / "result.json"
                result = json.loads(path.read_text())
                if fault == "missing_role":
                    del result["processes"]["gateway"]
                elif fault == "nonzero_exit":
                    result["processes"]["gateway"]["returncode"] = 1
                elif fault == "forced":
                    result["processes"]["engine"]["forced"] = True
                elif fault == "boolean_exit":
                    result["processes"]["engine"]["returncode"] = False
                elif fault == "integer_forced":
                    result["processes"]["engine"]["forced"] = 0
                else:
                    result["processes"]["engine"]["pid"] = 0 if fault == "zero_pid" else -1
                save_json(path, result)
                summary = self.invoke([directory], expected=1)
                self.assertEqual(summary["counts"]["invalid"], 1)
                self.assertEqual(summary["counts"]["valid"], 1)

    def test_required_stats_must_be_healthy_and_prove_wal_drain(self):
        for fault in ("missing_before", "snapshot_failure", "not_drained", "pending_bytes", "wrong_mode", "wrong_workers", "wrong_pool"):
            with self.subTest(fault=fault):
                directory = self.experiment("stats-" + fault)
                run = self.add_run(directory)
                if fault == "missing_before":
                    (run / "stats-before.json").unlink()
                else:
                    path = run / "stats-settled.json"
                    stats = json.loads(path.read_text())
                    if fault == "snapshot_failure":
                        stats["engine"]["snapshot_failures_total"] = 1
                    elif fault == "not_drained":
                        stats["engine"]["durable_sequence"] = 9
                    elif fault == "pending_bytes":
                        stats["engine"]["wal_pending_bytes"] = 8
                    elif fault == "wrong_mode":
                        stats["engine"]["wal_mode"] = "reliable"
                    elif fault == "wrong_workers":
                        stats["server"]["workers_capacity"] = 1
                    else:
                        stats["gateway"]["rpc"]["pool_capacity"] = 1
                    save_json(path, stats)
                summary = self.invoke([directory], expected=1)
                self.assertEqual(summary["counts"]["valid"], 1)
                self.assertNotEqual(summary["experiments"][0]["groups"][0]["runs"][0]["status"], "valid")

    def test_stats_sequences_must_account_for_preload_puts_and_deletes(self):
        faults = ("old_run", "before_applied", "before_keys", "before_durable", "before_pending", "after_missing_delete",
                  "after_durable_ahead", "settled_applied_behind", "settled_applied_ahead",
                  "settled_durable_behind", "settled_durable_ahead")
        for fault in faults:
            with self.subTest(fault=fault):
                directory = self.experiment("stats-accounting-" + fault)
                run = self.add_run(directory)
                if fault == "old_run":
                    # These healthy stats have the same mode and capacities,
                    # but describe only a preload from a different execution.
                    for name in ("stats-after.json", "stats-settled.json"):
                        path = run / name
                        stats = json.loads(path.read_text())
                        stats["engine"].update(applied_sequence=10, durable_sequence=10)
                        save_json(path, stats)
                else:
                    phase = "before" if fault.startswith("before_") else "after" if fault.startswith("after_") else "settled"
                    path = run / ("stats-" + phase + ".json")
                    stats = json.loads(path.read_text())
                    if fault == "before_applied":
                        stats["engine"].update(applied_sequence=1, durable_sequence=1)
                    elif fault == "before_keys":
                        stats["engine"]["keys"] = 1
                    elif fault == "before_durable":
                        stats["engine"]["durable_sequence"] = 1
                    elif fault == "before_pending":
                        stats["engine"]["wal_pending_bytes"] = 1
                    elif fault == "after_missing_delete":
                        stats["engine"].update(applied_sequence=30, durable_sequence=30)
                    elif fault == "after_durable_ahead":
                        stats["engine"]["durable_sequence"] = 36
                    elif fault == "settled_applied_behind":
                        stats["engine"].update(applied_sequence=34, durable_sequence=34)
                    elif fault == "settled_applied_ahead":
                        stats["engine"].update(applied_sequence=36, durable_sequence=36)
                    elif fault == "settled_durable_behind":
                        stats["engine"]["durable_sequence"] = 34
                    else:
                        stats["engine"]["durable_sequence"] = 36
                    save_json(path, stats)
                summary = self.invoke([directory], expected=1)
                group = summary["experiments"][0]["groups"][0]
                self.assertEqual(group["runs"][0]["status"], "invalid")
                self.assertTrue(group["runs"][0]["errors"])
                self.assertEqual(group["distributions"]["qps_successful"]["n"], 0)

    def test_throughput_durability_can_lag_until_the_settled_sample(self):
        directory = self.experiment()
        run = self.add_run(directory)
        path = run / "stats-after.json"
        stats = json.loads(path.read_text())
        stats["engine"].update(durable_sequence=30, wal_pending_bytes=64)
        save_json(path, stats)
        summary = self.invoke([directory])
        group = summary["experiments"][0]["groups"][0]
        self.assertEqual(group["counts"]["valid"], 1)
        self.assertEqual(group["runs"][0]["qps_successful"], 100)

    def test_reliable_acknowledgements_require_immediate_durability(self):
        for fault in ("durable_behind", "pending_bytes"):
            with self.subTest(fault=fault):
                directory = self.experiment("reliable-" + fault)
                run = self.add_run(directory, mode="reliable")
                path = run / "stats-after.json"
                stats = json.loads(path.read_text())
                if fault == "durable_behind":
                    stats["engine"]["durable_sequence"] -= 1
                else:
                    stats["engine"]["wal_pending_bytes"] = 64
                save_json(path, stats)
                summary = self.invoke([directory], expected=1)
                row = summary["experiments"][0]["groups"][0]["runs"][0]
                self.assertEqual(row["status"], "invalid")
                self.assertTrue(any("reliable acknowledgements" in error for error in row["errors"]))

    def test_moved_artifacts_still_match_original_recorded_commands(self):
        directory = self.experiment("original location")
        self.add_run(directory)
        moved = self.directory / "moved location"
        directory.rename(moved)
        summary = self.invoke([moved])
        self.assertEqual(summary["counts"]["valid"], 2)
        self.assertEqual(Path(summary["experiments"][0]["path"]), moved)

    def test_csv_is_parseable_and_retains_run_and_aggregate_rows(self):
        directory = self.experiment('csv,experiment')
        self.add_run(directory, repeat=1, qps=100)
        self.add_run(directory, repeat=2, qps=200)
        failed = self.add_run(directory, repeat=3, status="failed")
        failed_result = json.loads((failed / "result.json").read_text())
        failed_result["errors"] = ["fixture disk, error\nnext line"]
        save_json(failed / "result.json", failed_result)
        result = self.invoke([directory], format="csv", expected=1)
        rows = list(csv.DictReader(io.StringIO(result.stdout)))
        self.assertTrue(rows)
        self.assertEqual({row["record_type"] for row in rows}, {"run", "aggregate"})
        self.assertEqual({row["experiment"] for row in rows}, {str(directory)})
        run_rows = [row for row in rows if row["record_type"] == "run"]
        self.assertEqual({row["run"] for row in run_rows}, {
            "r%02d-throughput-snapshot-%s" % (repeat, state)
            for repeat in (1, 2, 3) for state in ("off", "on")})
        failures = [row for row in run_rows if row["status"] == "failed"]
        self.assertEqual(len(failures), 1)
        self.assertIn("disk, error", failures[0]["errors"])
        rates = [row for row in rows if row["record_type"] == "aggregate" and row["metric"] == "qps_successful"
                 and row["snapshot_interval_ms"] == "0"]
        self.assertEqual(len(rates), 1)
        self.assertEqual(int(rates[0]["n"]), 2)
        self.assertEqual(float(rates[0]["median"]), 150)

    def test_plan_path_traversal_is_rejected_before_loading_outside_artifacts(self):
        directory = self.experiment()
        self.add_run(directory)
        manifest = json.loads((directory / "manifest.json").read_text())
        manifest["plan"][0]["name"] = "../outside"
        save_json(directory / "manifest.json", manifest)
        self.invoke([directory], expected=2)

    def test_manifest_plan_must_match_the_entire_configured_matrix(self):
        for fault in ("missing_case", "extra_case", "wrong_case_id", "replaced_case", "changed_repeats"):
            with self.subTest(fault=fault):
                directory = self.experiment("plan-" + fault)
                self.add_run(directory)
                path = directory / "manifest.json"
                manifest = json.loads(path.read_text())
                if fault == "missing_case":
                    manifest["plan"].pop()
                elif fault == "extra_case":
                    manifest["plan"].append({"name": "r02-throughput-snapshot-off", "wal_mode": "throughput",
                                             "snapshot_interval_ms": 0})
                elif fault == "wrong_case_id":
                    manifest["plan"][0]["name"] = "r99-throughput-snapshot-off"
                elif fault == "replaced_case":
                    manifest["plan"][0]["snapshot_interval_ms"] = 1000
                else:
                    manifest["arguments"]["repeats"] += 1
                save_json(path, manifest)
                # The optimistic complete index and all existing files must
                # not make an incomplete or substituted root plan acceptable.
                result = self.invoke([directory], expected=2)
                self.assertEqual(result.stdout, "")

    def test_complete_matrix_plan_order_does_not_change_measurements(self):
        directory = self.experiment()
        self.add_run(directory, repeat=1, qps=100)
        self.add_run(directory, repeat=2, qps=200)
        self.add_run(directory, mode="reliable", qps=400)
        original = self.invoke([directory])
        path = directory / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["plan"].reverse()
        save_json(path, manifest)
        reordered = self.invoke([directory])
        self.assertEqual(reordered["counts"], original["counts"])
        self.assertEqual(reordered["counts"]["planned"], 8)
        self.assertEqual(reordered["counts"]["valid"], 8)
        for summary in (original, reordered):
            summary["experiments"][0]["groups"].sort(key=lambda group: (group["wal_mode"], group["snapshot_interval_ms"]))
            for group in summary["experiments"][0]["groups"]:
                group["runs"].sort(key=lambda row: row["name"])
        self.assertEqual(reordered, original)

    def test_run_artifact_symlink_cannot_escape_experiment(self):
        directory = self.experiment()
        run = self.add_run(directory)
        outside = self.directory / "outside-report.json"
        (run / "report.json").rename(outside)
        (run / "report.json").symlink_to(outside)
        content = outside.read_bytes()
        summary = self.invoke([directory], expected=1)
        self.assertEqual(summary["counts"]["valid"], 1)
        self.assertEqual(outside.read_bytes(), content)


if __name__ == "__main__":
    unittest.main()
