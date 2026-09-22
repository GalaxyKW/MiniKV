#!/usr/bin/env python3
"""Fixed-arrival artifact accounting and isolated experiment lifecycle checks."""

import copy
import csv
import io
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

import experiment_summary_test as summary_fixtures
import experiment_test as runner_fixtures

save_json = summary_fixtures.save_json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benmark"))
import experiment
import summarize


class FixedArrivalReportTests(unittest.TestCase):
    def setUp(self):
        self.fixture = summary_fixtures.ExperimentSummaryTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.number = 0

    def make_run(self, operation="mixed", mode="throughput"):
        self.number += 1
        directory = self.fixture.experiment("fixed-%d" % self.number, rate=1000)
        manifest = json.loads((directory / "manifest.json").read_text())
        manifest["arguments"]["op"] = operation
        manifest["arguments"]["modes"] = [mode]
        save_json(directory / "manifest.json", manifest)
        run = self.fixture.add_run(directory, mode=mode)
        report = json.loads((run / "report.json").read_text())
        return directory, run, report

    def write_loss(self, run, report, started=60, busy=25, late=15, failures=0, network=0, body_error=False):
        report["arrivals"].update(started=started, dropped_busy=busy, dropped_late=late)
        report["outcomes"].update(requests=started, successes=started-failures, failures=failures,
                                  network_errors=network, timeouts=network, http_failures=failures-network)
        operation = report["config"]["operation"]
        observed = "get" if operation == "mixed" else operation
        report["operations"] = {name: started if name == observed else 0 for name in ("put", "get", "delete")}
        report["http_statuses"] = {"200": started-failures + (network if body_error else 0), "503": failures-network}
        for field in ("latency_ns", "dispatch_delay_ns", "scheduled_latency_ns"):
            report[field]["samples"] = started
            if not started:
                report[field] = dict.fromkeys(report[field], 0)
        report.update(qps_total=started*1000000000/report["elapsed_ns"],
                      qps_successful=(started-failures)*1000000000/report["elapsed_ns"],
                      system_success_rate_pct=(started-failures)*100/started if started else 0,
                      offered_success_rate_pct=(started-failures)*100/report["config"]["requests"])
        save_json(run / "report.json", report)
        result = json.loads((run / "result.json").read_text())
        degraded = bool(busy or late or failures)
        result["status"] = "degraded" if degraded else "ok"
        result["processes"]["bench"]["returncode"] = int(degraded)
        save_json(run / "result.json", result)
        sequence = report["preload"]["completed_keys"] + report["operations"]["put"] + report["operations"]["delete"]
        for phase in ("after", "settled"):
            path = run / ("stats-" + phase + ".json")
            stats = json.loads(path.read_text())
            stats["engine"].update(applied_sequence=sequence, durable_sequence=sequence)
            save_json(path, stats)
        summary_fixtures.save_quiet_confirmation(run)

    def test_complete_fixed_reports_preserve_drops_service_failures_and_empty_populations(self):
        cases = ({"started": 100, "busy": 0, "late": 0}, {},
                 {"failures": 20}, {"failures": 20, "network": 20},
                 {"failures": 20, "network": 20, "body_error": True},
                 {"started": 0, "busy": 0, "late": 100})
        for options in cases:
            with self.subTest(options=options):
                _, run, report = self.make_run()
                self.write_loss(run, report, **options)
                self.assertEqual(experiment.read_report(run / "report.json", report["config"]), report)

    def test_invalid_arrival_and_response_accounting_is_rejected(self):
        _, run, original = self.make_run()
        self.write_loss(run, original, failures=10, network=5, body_error=True)
        mutations = {
            "planned": lambda r: r["arrivals"].update(planned=99),
            "started": lambda r: r["arrivals"].update(started=59),
            "sum": lambda r: r["arrivals"].update(dropped_late=14),
            "boolean": lambda r: r["arrivals"].update(dropped_busy=True),
            "negative": lambda r: r["arrivals"].update(dropped_late=-1),
            "duration": lambda r: r["arrivals"].update(schedule_duration_ns=1),
            "early_end": lambda r: r.update(elapsed_ns=1),
            "dispatch_samples": lambda r: r["dispatch_delay_ns"].update(samples=100),
            "scheduled_samples": lambda r: r["scheduled_latency_ns"].update(samples=0),
            "scheduled_before_service": lambda r: r["scheduled_latency_ns"].update(mean=1000),
            "scheduled_mean_sum": lambda r: r["scheduled_latency_ns"].update(mean=r["scheduled_latency_ns"]["mean"]+2),
            "scheduled_min_sum": lambda r: r["scheduled_latency_ns"].update(min=r["latency_ns"]["min"]),
            "scheduled_max_sum": lambda r: r["scheduled_latency_ns"].update(max=r["scheduled_latency_ns"]["max"]+1),
            "scheduled_overflow": lambda r: r["scheduled_latency_ns"].update(max=1 << 63),
            "offered_rate": lambda r: r.update(offered_success_rate_pct=100),
            "nonfinite": lambda r: r.update(offered_success_rate_pct=float("nan")),
            "missing_statuses": lambda r: r.update(http_statuses={"200": 1}),
            "too_many_statuses": lambda r: r.update(http_statuses={"200": 100}),
            "http_failure_without_status": lambda r: r.update(http_statuses={"200": 60}),
            "status_format": lambda r: r["http_statuses"].update({"0200": 0}),
            "wrong_model": lambda r: r.update(load_model="closed_loop"),
            "fractional_config_rate": lambda r: r["config"].update(rate=1000.0),
            "missing_arrivals": lambda r: r.pop("arrivals"),
            "incomplete": lambda r: r.update(complete=False),
        }
        expected = copy.deepcopy(original["config"])
        for name, mutate in mutations.items():
            with self.subTest(fault=name):
                report = copy.deepcopy(original)
                mutate(report)
                save_json(run / "report.json", report)
                with self.assertRaises(experiment.ExperimentError):
                    experiment.read_report(run / "report.json", expected)

    def test_empty_populations_cannot_publish_synthetic_latencies(self):
        _, run, report = self.make_run()
        self.write_loss(run, report, started=0, busy=100, late=0)
        for field in ("latency_ns", "dispatch_delay_ns", "scheduled_latency_ns"):
            with self.subTest(field=field):
                changed = copy.deepcopy(report)
                changed[field]["max"] = 1
                save_json(run / "report.json", changed)
                with self.assertRaises(experiment.ExperimentError):
                    experiment.read_report(run / "report.json", report["config"])

    def test_summary_keeps_degraded_metrics_and_prints_losses_in_all_formats(self):
        for empty in (False, True):
            with self.subTest(empty=empty):
                directory, run, report = self.make_run()
                self.write_loss(run, report, **({"started": 0, "busy": 0, "late": 100} if empty else {"failures": 10}))
                summary = self.fixture.invoke([directory], expected=1)
                group = summary["experiments"][0]["groups"][0]
                row = group["runs"][0]
                self.assertEqual(row["status"], "degraded")
                self.assertEqual(group["counts"]["degraded"], 1)
                self.assertEqual(group["distributions"]["qps_successful"]["n"], 1)
                self.assertEqual(row["arrival_started"], 0 if empty else 60)
                self.assertEqual(row["failures"], 0 if empty else 10)
                self.assertEqual(row["offered_success_rate_pct"], 0 if empty else 50)
                self.assertFalse(row["snapshot_comparison_eligible"])
                self.assertEqual(group["distributions"]["p99_ms"]["n"], 0 if empty else 1)
                self.assertEqual(group["distributions"]["scheduled_p99_ms"]["n"], 0 if empty else 1)
                text = self.fixture.invoke([directory], format="text", expected=1).stdout
                self.assertIn("degraded", text)
                self.assertIn("dropped busy/late", text)
                if empty:
                    self.assertIn("P99=unavailable", text)
                rows = list(csv.DictReader(io.StringIO(self.fixture.invoke([directory], format="csv", expected=1).stdout)))
                self.assertTrue(any(row["status"] == "degraded" and row["metric"] == "dropped_late" for row in rows))

    def test_summary_rejects_status_exit_and_settle_contradictions(self):
        for fault in ("ok_status", "bench_exit_zero", "bench_exit_two", "forced", "engine_exit", "missing_inflight",
                      "boolean_inflight", "inflight", "pool_busy", "pending", "below_minimum", "above_maximum"):
            with self.subTest(fault=fault):
                directory, run, report = self.make_run("put")
                self.write_loss(run, report, failures=10)
                path = run / "result.json"
                result = json.loads(path.read_text())
                if fault == "ok_status":
                    result["status"] = "ok"
                elif fault.startswith("bench_exit"):
                    result["processes"]["bench"]["returncode"] = 0 if fault.endswith("zero") else 2
                elif fault == "forced":
                    result["processes"]["bench"]["forced"] = True
                elif fault == "engine_exit":
                    result["processes"]["engine"]["returncode"] = 1
                save_json(path, result)
                path = run / "stats-settled.json"
                stats = json.loads(path.read_text())
                if fault == "missing_inflight":
                    del stats["server"]["requests_inflight"]
                elif fault == "boolean_inflight":
                    stats["server"]["requests_inflight"] = False
                elif fault == "inflight":
                    stats["server"]["requests_inflight"] = 1
                elif fault == "pool_busy":
                    stats["gateway"]["rpc"]["pool_in_use"] = 1
                elif fault == "pending":
                    stats["engine"]["wal_pending_bytes"] = 1
                elif fault in ("below_minimum", "above_maximum"):
                    value = 49 if fault == "below_minimum" else 61
                    stats["engine"].update(applied_sequence=value, durable_sequence=value)
                    save_json(run / "stats-after.json", stats)
                save_json(path, stats)
                summary = self.fixture.invoke([directory], expected=1)
                group = summary["experiments"][0]["groups"][0]
                self.assertEqual(group["runs"][0]["status"], "invalid")
                self.assertEqual(group["distributions"]["qps_successful"]["n"], 0)

    def test_failed_write_may_commit_after_client_timeout(self):
        directory, run, report = self.make_run("put")
        self.write_loss(run, report, failures=10, network=10)
        path = run / "stats-after.json"
        stats = json.loads(path.read_text())
        stats["engine"].update(applied_sequence=50, durable_sequence=50)
        stats["server"]["requests_inflight"] = 10
        save_json(path, stats)
        samples_path = run / "stats.jsonl"
        samples = [json.loads(line) for line in samples_path.read_text().splitlines()]
        samples[-2]["body"]["engine"].update(applied_sequence=50, durable_sequence=50)
        samples_path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
        summary = self.fixture.invoke([directory], expected=1)
        self.assertEqual(summary["experiments"][0]["groups"][0]["runs"][0]["status"], "degraded")

    def test_reliable_failed_writes_may_drain_later_but_acknowledged_writes_must_be_durable(self):
        directory, run, report = self.make_run("put", "reliable")
        self.write_loss(run, report, failures=10, network=10)
        path = run / "stats-after.json"
        stats = json.loads(path.read_text())
        stats["engine"].update(durable_sequence=50, wal_pending_bytes=100)
        save_json(path, stats)
        summary = self.fixture.invoke([directory], expected=1)
        self.assertEqual(summary["experiments"][0]["groups"][0]["runs"][0]["status"], "degraded")
        stats["engine"]["durable_sequence"] = 49
        save_json(path, stats)
        summary = self.fixture.invoke([directory], expected=1)
        self.assertEqual(summary["experiments"][0]["groups"][0]["runs"][0]["status"], "invalid")

    def test_clean_fixed_reports_keep_exact_mutation_accounting(self):
        for mode in ("throughput", "reliable"):
            with self.subTest(mode=mode):
                directory, run, report = self.make_run("put", mode)
                self.fixture.invoke([directory])
                for phase in ("after", "settled"):
                    path = run / ("stats-" + phase + ".json")
                    stats = json.loads(path.read_text())
                    stats["engine"].update(applied_sequence=99, durable_sequence=99)
                    save_json(path, stats)
                summary = self.fixture.invoke([directory], expected=1)
                self.assertEqual(summary["experiments"][0]["groups"][0]["runs"][0]["status"], "invalid")

    def test_fixed_mode_cannot_be_inferred_from_tampered_report_or_command(self):
        for fault in ("missing_rate", "wrong_rate", "missing_flag", "boolean_rate", "closed_degraded"):
            with self.subTest(fault=fault):
                directory, run, report = self.make_run()
                self.write_loss(run, report)
                if fault == "missing_rate":
                    del report["config"]["rate"]
                elif fault == "wrong_rate":
                    report["config"]["rate"] = 1001
                elif fault == "missing_flag":
                    path = run / "commands.json"
                    commands = json.loads(path.read_text())
                    commands["bench"]["argv"] = commands["bench"]["argv"][:-2]
                    save_json(path, commands)
                else:
                    path = directory / "manifest.json"
                    manifest = json.loads(path.read_text())
                    if fault == "boolean_rate":
                        manifest["arguments"]["rate"] = True
                    else:
                        del manifest["arguments"]["rate"]
                    save_json(path, manifest)
                save_json(run / "report.json", report)
                if fault == "boolean_rate":
                    result = self.fixture.invoke([directory], expected=2)
                    self.assertFalse(result.stdout)
                else:
                    summary = self.fixture.invoke([directory], expected=1)
                    self.assertEqual(summary["experiments"][0]["groups"][0]["runs"][0]["status"], "invalid")

    def test_invalidated_rows_do_not_retain_fixed_measurements(self):
        for fault in ("generator", "observation", "missing_observation"):
            with self.subTest(fault=fault):
                directory, run, report = self.make_run()
                self.write_loss(run, report)
                manifest = json.loads((directory / "manifest.json").read_text())
                if fault == "generator":
                    report["workload_generator"] = "another-generator"
                    save_json(run / "report.json", report)
                    summary = self.fixture.invoke([directory], expected=1)
                    rows = [row for group in summary["experiments"][0]["groups"] for row in group["runs"]]
                else:
                    error = FileNotFoundError("observation missing") if fault == "missing_observation" else RuntimeError("observation failed")
                    with mock.patch.object(summarize, "observe", side_effect=error):
                        rows = [summarize.read_run(directory, manifest, manifest["plan"][0], [])]
                for row in rows:
                    self.assertEqual(row["status"], "invalid")
                    self.assertEqual(row["rate"], 1000)
                    self.assertIsNone(row["qps_successful"])
                    self.assertTrue(all(row[name] is None for name in summarize.FIXED_METRICS if name != "rate"))

    def test_quiet_confirmation_must_be_raw_distinct_ordered_and_current(self):
        faults = ("missing", "single", "duplicate", "reversed", "busy", "failed_query", "mismatch",
                  "missing_boundary", "boolean_boundary", "before_boundary")
        for fault in faults:
            with self.subTest(fault=fault):
                directory, run, report = self.make_run()
                self.write_loss(run, report, failures=10)
                path = run / "stats.jsonl"
                samples = [json.loads(line) for line in path.read_text().splitlines()]
                if fault == "missing":
                    path.unlink()
                else:
                    if fault == "single":
                        samples = samples[-1:]
                    elif fault == "duplicate":
                        samples[-2] = copy.deepcopy(samples[-1])
                    elif fault == "reversed":
                        samples[-2:] = list(reversed(samples[-2:]))
                    elif fault == "busy":
                        samples[-2]["body"]["server"]["requests_inflight"] = 1
                    elif fault == "failed_query":
                        samples[-2]["error"] = "timeout"
                    elif fault == "mismatch":
                        samples[-1]["body"]["engine"]["keys"] += 1
                    path.write_text("".join(json.dumps(sample) + "\n" for sample in samples))
                if fault.endswith("boundary"):
                    result_path = run / "result.json"
                    result = json.loads(result_path.read_text())
                    if fault == "missing_boundary":
                        del result["wal_drain_started_monotonic_ns"]
                    elif fault == "boolean_boundary":
                        result["wal_drain_started_monotonic_ns"] = True
                    else:
                        result["wal_drain_started_monotonic_ns"] = samples[-2]["monotonic_start_ns"] + 1
                    save_json(result_path, result)
                summary = self.fixture.invoke([directory], expected=1)
                row = summary["experiments"][0]["groups"][0]["runs"][0]
                self.assertNotIn(row["status"], summarize.MEASURED)
                self.assertIsNone(row["qps_successful"])


class FixedArrivalRunnerTests(unittest.TestCase):
    def setUp(self):
        self.fixture = runner_fixtures.ExperimentRunnerTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_runner_preserves_clean_and_degraded_fixed_reports(self):
        cases = ({}, {"arrival_started": 3, "dropped_busy": 4, "dropped_late": 2},
                 {"service_failures": 2}, {"arrival_started": 0, "dropped_busy": 0, "dropped_late": 9})
        for number, control in enumerate(cases):
            with self.subTest(control=control):
                self.fixture.output = self.fixture.directory / ("fixed-%d" % number)
                degraded = bool(control)
                self.fixture.invoke(control=control, extra=("--rate", "100"), expected=int(degraded))
                index = json.loads((self.fixture.output / "index.json").read_text())
                self.assertEqual(index["status"], "complete")
                self.assertEqual(index["degraded_runs"], 2 if degraded else 0)
                self.assertEqual(index["failed_runs"], 0)
                self.assertEqual(index["successful_runs"] + index["degraded_runs"] + index["failed_runs"], len(index["runs"]))
                for result in index["runs"]:
                    self.assertEqual(result["status"], "degraded" if degraded else "ok")
                    self.assertEqual(result["errors"], [])
                    self.assertEqual(result["processes"]["bench"]["returncode"], int(degraded))
                    self.assertIn("offered_success_rate_pct", result["metrics"])

    def test_status_sampling_does_not_swallow_experiment_interrupts(self):
        with mock.patch.object(experiment.StatsConnection, "request", side_effect=experiment.Interrupted("received SIGINT")):
            with self.assertRaisesRegex(experiment.Interrupted, "SIGINT"):
                experiment.fetch_stats(1)

    def test_runner_waits_for_late_writes_instead_of_trusting_initial_empty_wal(self):
        for stale in (False, True):
            with self.subTest(stale_first_quiet_sample=stale):
                self.fixture.output = self.fixture.directory / ("late-write-%s" % stale)
                self.fixture.invoke(control={"service_failures": 9, "operations": {"put": 9, "get": 0, "delete": 0},
                                             "delayed_apply": .2, "stale_quiet_first": stale},
                                    extra=("--rate", "100", "--stats-ms", "0"), expected=1)
                for run in self.fixture.output.glob("r*"):
                    result = json.loads((run / "result.json").read_text())
                    self.assertEqual(result["status"], "degraded")
                    self.assertGreater(result["wal_drain_elapsed_ns"], 100000000)
                    after = json.loads((run / "stats-after.json").read_text())
                    self.assertEqual(after["engine"]["applied_sequence"], 3)
                    self.assertEqual(after["server"]["requests_inflight"], 0 if stale else 1)
                    self.assertEqual(json.loads((run / "stats-settled.json").read_text())["engine"]["applied_sequence"], 12)
                    samples = [json.loads(line) for line in (run / "stats.jsonl").read_text().splitlines()]
                    self.assertTrue(all(experiment.fixed_arrival_settled(sample["body"]) for sample in samples[-2:]))
                    self.assertLess(samples[-2]["monotonic_start_ns"], samples[-1]["monotonic_start_ns"])

    def test_fixed_loss_never_excuses_invalid_exit_or_unfinished_drain(self):
        for number, (control, extra) in enumerate((({"service_failures": 1, "bench_exit": 0}, ()),
                                                  ({"service_failures": 1, "bench_exit": 2}, ()),
                                                  ({"delayed_apply": 3}, ("--settle-timeout", ".05")))):
            with self.subTest(control=control):
                self.fixture.output = self.fixture.directory / ("bad-fixed-%d" % number)
                self.fixture.invoke(control=control, extra=("--rate", "100") + extra, expected=1)
                index = json.loads((self.fixture.output / "index.json").read_text())
                self.assertEqual(index["degraded_runs"], 0)
                self.assertTrue(all(result["status"] == "failed" and result["errors"] for result in index["runs"]))

    def test_invalid_rate_and_schedule_are_rejected_before_artifact_creation(self):
        for extra in (("--rate", "-1"), ("--rate", "1000000001"), ("--rate", "1.5"),
                      ("--rate", "nan"), ("--rate", "1000000000", "--requests", str(sys.maxsize // 24 + 1)),
                      ("--rate", "1", "--requests", "9223372037")):
            with self.subTest(extra=extra):
                self.fixture.invoke(extra=extra, expected=2)
                self.assertFalse(self.fixture.output.exists())
                self.assertEqual(self.fixture.pid_records(), [])

    def test_explicit_zero_rate_preserves_legacy_artifact_shape(self):
        self.fixture.invoke(extra=("--rate", "0"), expected=0)
        manifest = json.loads((self.fixture.output / "manifest.json").read_text())
        self.assertNotIn("rate", manifest["arguments"])
        for run in self.fixture.output.glob("r*"):
            report = json.loads((run / "report.json").read_text())
            command = json.loads((run / "commands.json").read_text())["bench"]["argv"]
            self.assertEqual(report["load_model"], "closed_loop")
            self.assertNotIn("rate", report["config"])
            self.assertNotIn("arrivals", report)
            self.assertNotIn("-rate", command)


if __name__ == "__main__":
    unittest.main()
