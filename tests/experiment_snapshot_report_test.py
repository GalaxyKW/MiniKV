#!/usr/bin/env python3
"""Black-box snapshot evidence checks using small, recorded experiment fixtures."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import experiment_summary_test as summary_fixtures


ROOT = Path(__file__).resolve().parents[1]
REPORTER = ROOT / "benmark" / "snapshot_report.py"
ACQUISITIONS = "snapshot_capture_state_lock_acquisitions_total"
HOLD = "snapshot_capture_state_lock_duration_ns_total"
MAXIMUM = "snapshot_capture_state_lock_duration_ns_max"
CALLS = "snapshot_file_write_calls_total"
WRITTEN = "snapshot_file_written_bytes_total"
INSTALLED = "snapshot_file_installed_bytes_total"
COMPACT = "snapshot_compact_written_bytes_total"
ACCOUNTING = (ACQUISITIONS, HOLD, MAXIMUM, CALLS, WRITTEN, INSTALLED, COMPACT)
PHASES = {name: "snapshot_" + name + "_duration_ns_total" for name in ("capture", "write", "compact")}
UINT64_MAX = (1 << 64) - 1
EPOCH = datetime(2026, 9, 14, 8, tzinfo=timezone.utc)


class _Artifacts:
    # Reuse construction only. Importing a module rather than its TestCase, and
    # using a plain adapter, keeps unittest from discovering the old tests here.
    experiment = summary_fixtures.ExperimentSummaryTests.experiment
    add_run = summary_fixtures.ExperimentSummaryTests.add_run
    write_run = summary_fixtures.ExperimentSummaryTests.write_run

    def __init__(self, directory):
        self.directory = directory


def timestamp(milliseconds):
    return (EPOCH + timedelta(milliseconds=milliseconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ExperimentSnapshotReportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="minikv-snapshot-report-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.artifacts = _Artifacts(self.directory)

    def sample(self, run, milliseconds, completed=10, busy=False, values=None, finished=None):
        body = json.loads((run / "stats-after.json").read_text())
        engine = body["engine"]
        engine.update(snapshot_successes_total=completed, snapshot_failures_total=0,
                      snapshot_in_progress=busy)
        engine.update({PHASES["capture"]: completed * 1000,
                       PHASES["write"]: completed * 2000,
                       PHASES["compact"]: completed * 3000,
                       ACQUISITIONS: completed, HOLD: completed * 100,
                       MAXIMUM: 900, CALLS: completed * 10,
                       WRITTEN: completed * 1000, INSTALLED: completed * 1000,
                       COMPACT: completed * 500})
        engine.update(values or {})
        finished = milliseconds if finished is None else finished
        return {"started_at": timestamp(milliseconds), "finished_at": timestamp(finished),
                "monotonic_start_ns": 1000000000 + milliseconds * 1000000,
                "monotonic_end_ns": 1000000000 + finished * 1000000,
                "error": None, "http_status": 200, "body": body}

    def write_samples(self, run, samples):
        (run / "stats.jsonl").write_text("".join(json.dumps(sample) + "\n" for sample in samples))

    def new_run(self, name="experiment", **kwargs):
        directory = self.artifacts.experiment(name)
        run = self.artifacts.add_run(directory, interval=1000, **kwargs)
        samples = [self.sample(run, 100, completed=10), self.sample(run, 800, completed=11)]
        self.write_samples(run, samples)
        # Keep later boundary observations coherent with the ordinary stream.
        # Fault tests change periodic samples only, retaining valid client/LSN
        # evidence so a damaged metric cannot be mistaken for a failed client.
        for phase in ("after", "settled"):
            summary_fixtures.save_json(run / ("stats-" + phase + ".json"), samples[-1]["body"])
        return directory, run, samples

    def fixed_arrival_run(self, name="fixed-arrival", dropped_busy=0, dropped_late=0, failures=0):
        directory, run, _ = self.new_run(name)
        manifest = json.loads((directory / "manifest.json").read_text())
        manifest["arguments"]["rate"] = 100
        summary_fixtures.save_json(directory / "manifest.json", manifest)
        for case in manifest["plan"]:
            current = directory / case["name"]
            command = json.loads((current / "commands.json").read_text())
            command["bench"]["argv"].extend(("-rate", "100"))
            summary_fixtures.save_json(current / "commands.json", command)
            report = json.loads((current / "report.json").read_text())
            busy, late, failed = (dropped_busy, dropped_late, failures) if current == run else (0, 0, 0)
            started = report["config"]["requests"] - busy - late
            succeeded = started - failed
            report["config"]["rate"] = 100
            report.update(load_model="fixed_arrival", arrivals={
                "planned": 100, "started": started, "dropped_busy": busy, "dropped_late": late,
                "schedule_duration_ns": 1000000000})
            report["outcomes"].update(requests=started, successes=succeeded, failures=failed,
                                      http_failures=failed)
            writes = min(report["operations"]["put"], started)
            deletes = min(report["operations"]["delete"], started - writes)
            report["operations"] = {"put": writes, "get": started - writes - deletes, "delete": deletes}
            expected_sequence = report["preload"]["completed_keys"] + writes + deletes
            report["http_statuses"] = {"200": succeeded} if succeeded else {}
            if failed:
                report["http_statuses"]["503"] = failed
            report["latency_ns"]["samples"] = started
            if not started:
                report["latency_ns"] = dict.fromkeys(report["latency_ns"], 0)
            report["dispatch_delay_ns"] = {
                key: started if key == "samples" else (1000000 if started else 0)
                for key in report["latency_ns"]}
            report["scheduled_latency_ns"] = {
                key: value if key == "samples" or not started else value + 1000000
                for key, value in report["latency_ns"].items()}
            report.update(qps_total=started, qps_successful=succeeded,
                          system_success_rate_pct=100 * succeeded / started if started else 0,
                          offered_success_rate_pct=succeeded)
            summary_fixtures.save_json(current / "report.json", report)
            result = json.loads((current / "result.json").read_text())
            result["status"] = "degraded" if busy or late or failed else "ok"
            result["processes"]["bench"]["returncode"] = 1 if result["status"] == "degraded" else 0
            summary_fixtures.save_json(current / "result.json", result)
            for phase in ("before", "after", "settled"):
                path = current / ("stats-" + phase + ".json")
                body = json.loads(path.read_text())
                sequence = 0 if phase == "before" else expected_sequence
                body["engine"].update(applied_sequence=sequence, durable_sequence=sequence)
                body["server"]["requests_inflight"] = 0
                body["gateway"]["rpc"]["pool_in_use"] = 0
                summary_fixtures.save_json(path, body)
            samples = [json.loads(line) for line in (current / "stats.jsonl").read_text().splitlines()]
            for sample in samples:
                sample["body"]["engine"].update(applied_sequence=expected_sequence,
                                                 durable_sequence=expected_sequence)
            self.write_samples(current, samples)
            summary_fixtures.save_quiet_confirmation(current)
        return directory, run

    def shared_window_samples(self, run):
        # There is a cheap inner idle interval, but all fields must use the
        # earliest/latest idle observations. Queries crossing the measurement
        # boundaries and busy edge samples must not change those endpoints.
        return [
            self.sample(run, -20, completed=900000, finished=50),
            self.sample(run, 100, completed=10, busy=True,
                        values={ACQUISITIONS: 11, HOLD: 1100}),
            self.sample(run, 200, completed=11, values={HOLD: 1100}),
            self.sample(run, 300, completed=11, busy=True,
                        values={ACQUISITIONS: 12, HOLD: 1120}),
            self.sample(run, 400, completed=12,
                        values={HOLD: 1120, PHASES["capture"]: 11010, PHASES["write"]: 22020,
                                PHASES["compact"]: 33030, CALLS: 111, WRITTEN: 11100,
                                INSTALLED: 11100, COMPACT: 5510}),
            self.sample(run, 700, completed=14,
                        values={HOLD: 1500, PHASES["capture"]: 19000, PHASES["write"]: 34000,
                                PHASES["compact"]: 57000, CALLS: 150, WRITTEN: 15000,
                                INSTALLED: 15000, COMPACT: 7500}),
            self.sample(run, 800, completed=14, busy=True,
                        values={ACQUISITIONS: 15, HOLD: 1700, PHASES["capture"]: 50000,
                                PHASES["write"]: 34000, PHASES["compact"]: 57000,
                                CALLS: 150, WRITTEN: 15000, INSTALLED: 15000, COMPACT: 7500}),
            self.sample(run, 950, completed=999999, finished=1010),
        ]

    def invoke(self, directories, expected=0, format="json", extra=()):
        before = {directory: summary_fixtures.file_snapshot(directory) for directory in directories}
        command = [sys.executable, str(REPORTER), *map(str, directories)]
        if format is not None:
            command.extend(("--format", format))
        command.extend(extra)
        result = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, timeout=10)
        self.assertEqual(result.returncode, expected, "stdout=%s\nstderr=%s" % (result.stdout, result.stderr))
        for directory in directories:
            self.assertEqual(summary_fixtures.file_snapshot(directory), before[directory],
                             "snapshot reporter changed input bytes, permissions or timestamps")
        if format == "json" and expected != 2:
            report = json.loads(result.stdout)
            self.assertEqual(report["schema_version"], 1)
            return report
        return result

    def row(self, report, run):
        for experiment in report["experiments"]:
            if Path(experiment["path"]) == run.parent:
                return next(row for row in experiment["runs"] if row["name"] == run.name)
        self.fail("run absent from snapshot report: " + str(run))

    def assert_client_valid(self, row):
        self.assertEqual(row["client"]["status"], "valid")
        self.assertEqual(row["client"]["qps_successful"], 100)
        self.assertEqual(row["client"]["p99_ms"], 3)

    def test_all_metrics_share_the_earliest_and_latest_internal_idle_window(self):
        directory, run, _ = self.new_run()
        self.write_samples(run, self.shared_window_samples(run))
        report = self.invoke([directory])
        row = self.row(report, run)
        self.assert_client_valid(row)
        self.assertEqual(report["thresholds"], {"min_completed": 1, "min_measurement_seconds": 0})
        snapshot = row["snapshot"]
        self.assertEqual(snapshot["status"], "valid")
        self.assertEqual(snapshot["coverage"]["status"], "sufficient")
        self.assertEqual(snapshot["coverage"]["internal_samples"], 6)
        self.assertEqual(snapshot["coverage"]["idle_samples"], 3)
        self.assertEqual(snapshot["coverage"]["completed"], 3)
        window = snapshot["coverage"]["window"]
        self.assertEqual((window["first"]["line"], window["last"]["line"]), (3, 6))
        self.assertEqual((window["trimmed_head_samples"], window["trimmed_tail_samples"]), (1, 1))
        self.assertEqual(window["first"]["finished_at"], timestamp(200))
        self.assertEqual(window["last"]["finished_at"], timestamp(700))
        for phase, start, end in (("capture", 11000, 19000), ("write", 22000, 34000), ("compact", 33000, 57000)):
            value = snapshot["phases"][phase]
            self.assertEqual((value["start"], value["end"], value["delta"]), (start, end, end - start))
            self.assertAlmostEqual(value["mean_ns"], (end - start) / 3)
        for field, delta in ((ACQUISITIONS, 3), (HOLD, 400), (CALLS, 40),
                             (WRITTEN, 4000), (INSTALLED, 4000), (COMPACT, 2000)):
            self.assertEqual(snapshot["accounting"][field]["delta"], delta)
        self.assertAlmostEqual(snapshot["capture_hold_mean_ns"], 400 / 3)

    def test_lifetime_maximum_is_absolute_even_when_it_does_not_increase(self):
        directory, run, samples = self.new_run()
        for sample in samples:
            sample["body"]["engine"][MAXIMUM] = 9007199254740993
        self.write_samples(run, samples)
        value = self.row(self.invoke([directory]), run)["snapshot"]["accounting"][MAXIMUM]
        self.assertEqual(value["availability"], "available")
        self.assertEqual(value["kind"], "lifetime_max")
        self.assertEqual(value["start"], 9007199254740993)
        self.assertEqual(value["end"], 9007199254740993)
        self.assertIsNone(value["delta"])

    def test_thresholds_use_full_measurement_and_keep_insufficient_deltas(self):
        directory, run, _ = self.new_run()
        self.write_samples(run, self.shared_window_samples(run))
        for count, seconds, sufficient in ((3, "1", True), (4, "0", False), (1, "1.001", False)):
            with self.subTest(count=count, seconds=seconds):
                report = self.invoke([directory], extra=("--min-completed", str(count), "--min-measurement-seconds", seconds))
                row = self.row(report, run)
                self.assert_client_valid(row)
                snapshot = row["snapshot"]
                self.assertEqual(snapshot["coverage"]["measurement_elapsed_ns"], 1000000000)
                self.assertEqual(snapshot["coverage"]["status"], "sufficient" if sufficient else "insufficient")
                self.assertEqual(snapshot["phases"]["capture"]["delta"], 8000)
                if sufficient:
                    self.assertAlmostEqual(snapshot["phases"]["capture"]["mean_ns"], 8000 / 3)
                else:
                    self.assertTrue(snapshot["coverage"]["reasons"])
                    self.assertIsNone(snapshot["capture_hold_mean_ns"])
                    self.assertTrue(all(value["mean_ns"] is None for value in snapshot["phases"].values()))

    def test_zero_completed_snapshots_preserve_measured_zeros_without_a_mean(self):
        directory, run, samples = self.new_run()
        for sample in samples:
            sample["body"]["engine"]["snapshot_successes_total"] = 10
            sample["body"]["engine"].update({field: 0 for field in ACCOUNTING + tuple(PHASES.values())})
        self.write_samples(run, samples)
        snapshot = self.row(self.invoke([directory]), run)["snapshot"]
        self.assertEqual(snapshot["coverage"]["completed"], 0)
        self.assertEqual(snapshot["coverage"]["status"], "insufficient")
        self.assertIsNone(snapshot["capture_hold_mean_ns"])
        for field, value in snapshot["accounting"].items():
            self.assertEqual(value["availability"], "available")
            self.assertEqual((value["start"], value["end"]), (0, 0))
            self.assertEqual(value["delta"], None if field == MAXIMUM else 0)
        for value in snapshot["phases"].values():
            self.assertEqual(value["delta"], 0)
            self.assertIsNone(value["mean_ns"])

    def test_fewer_than_two_idle_or_internal_samples_never_fabricate_a_window(self):
        for variant in ("all_busy", "one_idle", "outside_only"):
            with self.subTest(variant=variant):
                directory, run, samples = self.new_run(variant)
                if variant == "outside_only":
                    samples = [self.sample(run, -20, finished=-10), self.sample(run, 1010, completed=11)]
                else:
                    for number, sample in enumerate(samples):
                        sample["body"]["engine"]["snapshot_in_progress"] = variant == "all_busy" or number == 0
                self.write_samples(run, samples)
                row = self.row(self.invoke([directory]), run)
                self.assert_client_valid(row)
                snapshot = row["snapshot"]
                self.assertEqual(snapshot["status"], "valid")
                self.assertEqual(snapshot["coverage"]["status"], "insufficient")
                self.assertIsNone(snapshot["coverage"]["window"])
                self.assertIsNone(snapshot["coverage"]["completed"])
                self.assertTrue(all(value["availability"] == "no_window" for value in snapshot["phases"].values()))

    def test_missing_and_empty_periodic_sampling_do_not_use_boundary_queries(self):
        for variant in ("missing", "empty"):
            with self.subTest(variant=variant):
                directory, run, _ = self.new_run(variant)
                path = run / "stats.jsonl"
                if variant == "missing":
                    path.unlink()
                else:
                    path.write_text("")
                row = self.row(self.invoke([directory]), run)
                self.assert_client_valid(row)
                self.assertEqual(row["snapshot"]["status"], "missing" if variant == "missing" else "valid")
                self.assertIsNone(row["snapshot"]["coverage"]["window"])
                self.assertIsNone(row["snapshot"]["phases"]["capture"]["delta"])

    def test_absent_or_null_old_accounting_fields_do_not_invalidate_client_or_phases(self):
        for availability in ("missing", "null"):
            with self.subTest(availability=availability):
                directory, run, samples = self.new_run(availability)
                for sample in samples:
                    for field in ACCOUNTING:
                        if availability == "missing":
                            del sample["body"]["engine"][field]
                        else:
                            sample["body"]["engine"][field] = None
                self.write_samples(run, samples)
                row = self.row(self.invoke([directory]), run)
                self.assert_client_valid(row)
                self.assertEqual(row["snapshot"]["status"], "valid")
                self.assertEqual(row["snapshot"]["phases"]["capture"]["mean_ns"], 1000)
                for value in row["snapshot"]["accounting"].values():
                    self.assertEqual(value["availability"], availability)
                    self.assertIsNone(value["delta"])

    def test_partial_missing_or_null_fields_cannot_choose_different_endpoints(self):
        for variant in ("missing", "null"):
            with self.subTest(variant=variant):
                directory, run, _ = self.new_run(variant)
                samples = self.shared_window_samples(run)
                if variant == "missing":
                    del samples[3]["body"]["engine"][CALLS]
                else:
                    samples[3]["body"]["engine"][CALLS] = None
                self.write_samples(run, samples)
                row = self.row(self.invoke([directory]), run)
                self.assert_client_valid(row)
                snapshot = row["snapshot"]
                self.assertEqual(snapshot["accounting"][CALLS]["availability"], "partial")
                self.assertIsNone(snapshot["accounting"][CALLS]["delta"])
                self.assertEqual(snapshot["accounting"][WRITTEN]["delta"], 4000)
                self.assertEqual(snapshot["coverage"]["window"]["first"]["line"], 3)
                self.assertEqual(snapshot["coverage"]["window"]["last"]["line"], 6)

    def test_uint64_boundaries_are_exact_and_not_converted_through_float(self):
        directory, run, samples = self.new_run()
        for number, sample in enumerate(samples):
            sample["body"]["engine"].update({field: UINT64_MAX - 1 + number for field in ACCOUNTING})
        self.write_samples(run, samples)
        snapshot = self.row(self.invoke([directory]), run)["snapshot"]
        for field, value in snapshot["accounting"].items():
            self.assertEqual(value["availability"], "available")
            self.assertEqual((value["start"], value["end"]), (UINT64_MAX - 1, UINT64_MAX))
            self.assertEqual(value["delta"], None if field == MAXIMUM else 1)
        self.assertEqual(snapshot["capture_hold_mean_ns"], 1)

    def test_invalid_optional_values_disable_only_the_affected_metric(self):
        invalids = (True, -1, UINT64_MAX + 1, 1.5, "12", {}, [])
        for field, invalid in zip(ACCOUNTING, invalids):
            with self.subTest(field=field, invalid=invalid):
                directory, run, samples = self.new_run(field)
                samples[0]["body"]["engine"][field] = invalid
                self.write_samples(run, samples)
                row = self.row(self.invoke([directory], expected=1), run)
                self.assert_client_valid(row)
                snapshot = row["snapshot"]
                self.assertEqual(snapshot["status"], "valid")
                self.assertEqual(snapshot["accounting"][field]["availability"], "invalid")
                self.assertIsNone(snapshot["accounting"][field]["delta"])
                self.assertTrue(snapshot["errors"])
                self.assertEqual(snapshot["phases"]["capture"]["mean_ns"], 1000)
                self.assertTrue(all(value["availability"] == "available" for key, value in snapshot["accounting"].items() if key != field))

    def test_decreases_in_trimmed_busy_samples_and_across_nulls_remain_invalid(self):
        for field in ACCOUNTING:
            with self.subTest(field=field):
                directory, run, _ = self.new_run(field)
                samples = self.shared_window_samples(run)
                samples[1]["body"]["engine"][field] = UINT64_MAX
                self.write_samples(run, samples)
                row = self.row(self.invoke([directory], expected=1), run)
                self.assert_client_valid(row)
                snapshot = row["snapshot"]
                self.assertEqual(snapshot["accounting"][field]["availability"], "invalid")
                self.assertEqual(snapshot["coverage"]["window"]["first"]["line"], 3)
        directory, run, _ = self.new_run("decrease-across-null")
        samples = [self.sample(run, 100, values={CALLS: 10}),
                   self.sample(run, 300, values={CALLS: None}),
                   self.sample(run, 800, completed=11, values={CALLS: 9})]
        self.write_samples(run, samples)
        snapshot = self.row(self.invoke([directory], expected=1), run)["snapshot"]
        self.assertEqual(snapshot["accounting"][CALLS]["availability"], "invalid")

    def test_bad_successful_middle_sample_invalidates_the_stream_instead_of_joining_its_neighbors(self):
        faults = ("busy_type", "counter_decrease", "sequence", "workers", "http_status", "schema")
        for fault in faults:
            with self.subTest(fault=fault):
                directory, run, _ = self.new_run(fault)
                samples = [self.sample(run, 100), self.sample(run, 400), self.sample(run, 800, completed=11)]
                bad = samples[1]
                if fault == "busy_type":
                    bad["body"]["engine"]["snapshot_in_progress"] = "false"
                elif fault == "counter_decrease":
                    bad["body"]["engine"]["snapshot_successes_total"] = 9
                elif fault == "sequence":
                    bad["body"]["engine"]["applied_sequence"] = 100
                elif fault == "workers":
                    bad["body"]["server"]["workers_capacity"] = 5
                elif fault == "http_status":
                    bad["http_status"] = 503
                else:
                    bad["body"]["schema_version"] = True
                self.write_samples(run, samples)
                row = self.row(self.invoke([directory], expected=1), run)
                self.assert_client_valid(row)
                self.assertEqual(row["snapshot"]["status"], "invalid")
                self.assertIsNone(row["snapshot"]["coverage"]["window"])
                self.assertIsNone(row["snapshot"]["phases"]["capture"]["delta"])
                self.assertTrue(row["snapshot"]["errors"])

    def test_bad_json_between_good_samples_is_not_silently_dropped(self):
        directory, run, samples = self.new_run()
        (run / "stats.jsonl").write_text(json.dumps(samples[0]) + "\n{broken\n" + json.dumps(samples[1]) + "\n")
        row = self.row(self.invoke([directory], expected=1), run)
        self.assert_client_valid(row)
        self.assertEqual(row["snapshot"]["status"], "invalid")
        self.assertIsNone(row["snapshot"]["coverage"]["window"])

    def test_invalid_or_overlapping_query_clocks_do_not_produce_a_window(self):
        for fault in ("reversed", "overlap", "monotonic_decrease", "partial_monotonic"):
            with self.subTest(fault=fault):
                directory, run, samples = self.new_run(fault)
                if fault == "reversed":
                    samples[1]["started_at"] = timestamp(900)
                elif fault == "overlap":
                    samples[1]["started_at"] = timestamp(99)
                elif fault == "monotonic_decrease":
                    samples[1]["monotonic_start_ns"] = samples[1]["monotonic_end_ns"] = 1
                else:
                    del samples[1]["monotonic_end_ns"]
                self.write_samples(run, samples)
                row = self.row(self.invoke([directory], expected=1), run)
                self.assert_client_valid(row)
                self.assertEqual(row["snapshot"]["status"], "invalid")
                self.assertIsNone(row["snapshot"]["coverage"]["window"])

    def test_recorded_failed_queries_are_disclosed_without_corrupting_successful_samples(self):
        directory, run, samples = self.new_run()
        failed = self.sample(run, 400)
        failed.update(error="timed out", http_status=None, body=None)
        self.write_samples(run, [samples[0], failed, samples[1]])
        row = self.row(self.invoke([directory]), run)
        self.assert_client_valid(row)
        snapshot = row["snapshot"]
        self.assertEqual(snapshot["status"], "valid")
        self.assertEqual(snapshot["coverage"]["failed_queries"], 1)
        self.assertEqual(snapshot["coverage"]["internal_samples"], 2)
        self.assertEqual(snapshot["phases"]["capture"]["delta"], 1000)
        self.assertTrue(snapshot["warnings"])

    def test_phase_availability_is_independent_of_other_phases_and_accounting(self):
        directory, run, samples = self.new_run()
        for sample in samples:
            del sample["body"]["engine"][PHASES["capture"]]
        samples[0]["body"]["engine"][PHASES["write"]] = None
        samples[1]["body"]["engine"][PHASES["compact"]] = -1
        self.write_samples(run, samples)
        row = self.row(self.invoke([directory], expected=1), run)
        self.assert_client_valid(row)
        snapshot = row["snapshot"]
        self.assertEqual(snapshot["phases"]["capture"]["availability"], "missing")
        self.assertEqual(snapshot["phases"]["write"]["availability"], "partial")
        self.assertEqual(snapshot["phases"]["compact"]["availability"], "invalid")
        self.assertEqual(snapshot["accounting"][WRITTEN]["delta"], 1000)
        self.assertEqual(snapshot["capture_hold_mean_ns"], 100)

    def test_capture_hold_mean_uses_acquisitions_and_discloses_count_mismatch(self):
        directory, run, samples = self.new_run()
        samples[-1]["body"]["engine"].update({ACQUISITIONS: 15, HOLD: 1500})
        self.write_samples(run, samples)
        snapshot = self.row(self.invoke([directory]), run)["snapshot"]
        self.assertEqual(snapshot["coverage"]["completed"], 1)
        self.assertEqual(snapshot["accounting"][ACQUISITIONS]["delta"], 5)
        self.assertEqual(snapshot["capture_hold_mean_ns"], 100)
        self.assertTrue(any("acquisition" in warning.lower() for warning in snapshot["warnings"]))
        samples[-1]["body"]["engine"][ACQUISITIONS] = 10
        self.write_samples(run, samples)
        snapshot = self.row(self.invoke([directory]), run)["snapshot"]
        self.assertIsNone(snapshot["capture_hold_mean_ns"])

    def test_failed_missing_and_incomplete_clients_remain_visible_and_are_not_evaluated(self):
        directory = self.artifacts.experiment()
        expected = {}
        for repeat, status in enumerate(("failed", "interrupted", "running", "missing"), 1):
            run = self.artifacts.add_run(directory, repeat=repeat, interval=1000,
                                         status="ok" if status == "missing" else status, missing=status == "missing")
            expected[run.name] = "incomplete" if status == "running" else status
        report = self.invoke([directory], expected=1)
        rows = report["experiments"][0]["runs"]
        self.assertEqual(len(rows), 8)
        for row in rows:
            if row["name"] in expected:
                self.assertEqual(row["client"]["status"], expected[row["name"]])
                self.assertIsNone(row["client"]["qps_successful"])
                self.assertEqual(row["snapshot"]["status"], "not_evaluated")
                self.assertIsNone(row["snapshot"]["coverage"]["window"])
                self.assertEqual(set(row["snapshot"]["accounting"]), set(ACCOUNTING))

    def test_clean_fixed_arrival_keeps_client_metrics_and_snapshot_evidence(self):
        directory, run = self.fixed_arrival_run()
        row = self.row(self.invoke([directory]), run)
        self.assert_client_valid(row)
        self.assertEqual(row["client"]["load_model"], "fixed_arrival")
        self.assertEqual(row["client"]["rate"], 100)
        self.assertEqual(row["client"]["arrival_planned"], 100)
        self.assertEqual(row["client"]["arrival_started"], 100)
        self.assertEqual(row["client"]["offered_success_rate_pct"], 100)
        self.assertEqual(row["client"]["dispatch_p99_ms"], 1)
        self.assertEqual(row["client"]["scheduled_p99_ms"], 4)
        self.assertEqual(row["snapshot"]["coverage"]["status"], "sufficient")
        self.assertEqual(row["snapshot"]["phases"]["capture"]["mean_ns"], 1000)

    def test_degraded_fixed_arrival_discloses_losses_and_excludes_snapshot_comparisons(self):
        for name, busy, late, failures in (("drops", 7, 3, 0), ("http-failures", 0, 0, 2),
                                          ("zero-attempts", 40, 60, 0)):
            with self.subTest(name=name):
                directory, run = self.fixed_arrival_run(name, busy, late, failures)
                row = self.row(self.invoke([directory], expected=1), run)
                client, snapshot = row["client"], row["snapshot"]
                started = 100 - busy - late
                self.assertEqual(client["status"], "degraded")
                self.assertEqual(client["arrival_planned"], 100)
                self.assertEqual(client["arrival_started"], started)
                self.assertEqual(client["dropped_busy"], busy)
                self.assertEqual(client["dropped_late"], late)
                self.assertEqual(client["failures"], failures)
                self.assertEqual(client["offered_success_rate_pct"], started - failures)
                self.assertEqual(client["qps_successful"], started - failures)
                self.assertEqual(client["p99_ms"], 3 if started else None)
                self.assertEqual(client["scheduled_p99_ms"], 4 if started else None)
                self.assertEqual(snapshot["status"], "not_evaluated")
                self.assertIsNone(snapshot["coverage"]["window"])
                self.assertTrue(any("excluded from clean snapshot comparisons" in reason
                                    for reason in snapshot["coverage"]["reasons"]))
                self.assertTrue(all(value["mean_ns"] is None for value in snapshot["phases"].values()))
                text = self.invoke([directory], expected=1, format="text")
                for value in ("client=degraded", "rate=100 req/s", "planned=100", "started=" + str(started),
                              "dropped_busy=" + str(busy), "dropped_late=" + str(late),
                              "failures=" + str(failures), "offered_success_rate=", "scheduled P99=",
                              "excluded from clean snapshot comparisons"):
                    self.assertIn(value, text.stdout)
                self.assertIn("degraded", text.stderr)

    def test_closed_loop_client_shape_stays_unchanged(self):
        directory, run, _ = self.new_run()
        client = self.row(self.invoke([directory]), run)["client"]
        self.assertEqual(set(client), {"status", "qps_successful", "p99_ms", "errors", "warnings"})

    def test_invalid_client_report_cannot_be_rehabilitated_by_good_snapshot_samples(self):
        directory, run, _ = self.new_run()
        path = run / "report.json"
        report = json.loads(path.read_text())
        report["outcomes"]["requests"] += 1
        summary_fixtures.save_json(path, report)
        row = self.row(self.invoke([directory], expected=1), run)
        self.assertEqual(row["client"]["status"], "invalid")
        self.assertIsNone(row["client"]["qps_successful"])
        self.assertEqual(row["snapshot"]["status"], "not_evaluated")

    def test_moved_archive_without_data_or_binaries_remains_read_only(self):
        directory, run, _ = self.new_run()
        data = run / "data"
        data.mkdir()
        (data / "snapshot.v1").write_bytes(b"not needed by the statistics report")
        shutil.rmtree(data)
        shutil.rmtree(directory / "binaries")
        moved = self.directory / "moved archive with spaces"
        directory.rename(moved)
        report = self.invoke([moved])
        experiment = report["experiments"][0]
        self.assertEqual(experiment["binary_verification"], "recorded_hashes_only")
        self.assertTrue(experiment["warnings"])
        row = self.row(report, moved / run.name)
        self.assert_client_valid(row)
        self.assertEqual(row["snapshot"]["phases"]["capture"]["delta"], 1000)

    def test_bad_directory_is_reported_alongside_a_valid_stage(self):
        directory, run, _ = self.new_run()
        bad = self.directory / "bad-stage"
        bad.mkdir()
        (bad / "manifest.json").write_text("not JSON\n")
        report = self.invoke([bad, directory], expected=1)
        self.assertEqual(len(report["experiments"]), 2)
        broken = report["experiments"][0]
        self.assertTrue(broken["errors"])
        self.assertIsNone(broken["arguments"])
        self.assertIsNone(broken["counts"])
        self.assertEqual(broken["runs"], [])
        self.assert_client_valid(self.row(report, run))

    def test_cli_rejects_invalid_thresholds_and_duplicate_directories(self):
        directory, _, _ = self.new_run()
        invalid = (("--min-completed", "0"), ("--min-completed", "1.5"),
                   ("--min-measurement-seconds", "-1"), ("--min-measurement-seconds", "nan"),
                   ("--min-measurement-seconds", "inf"), ("--min-measurement-seconds", "1e999"),
                   ("--format", "csv"))
        for options in invalid:
            with self.subTest(options=options):
                result = self.invoke([directory], expected=2, extra=options)
                self.assertEqual(result.stdout, "")
                self.assertTrue(result.stderr)
        result = self.invoke([directory, directory / "."], expected=2)
        self.assertEqual(result.stdout, "")

    def test_default_text_includes_client_evidence_and_unavailable_metric_explanations(self):
        directory, run, samples = self.new_run()
        for sample in samples:
            del sample["body"]["engine"][CALLS]
        self.write_samples(run, samples)
        result = self.invoke([directory], format=None)
        for text in ("MiniKV snapshot report", run.name, "client=valid", "coverage=sufficient",
                     "shared window", CALLS, "missing", "Lifetime maxima", "per acquisition"):
            self.assertIn(text, result.stdout)


if __name__ == "__main__":
    unittest.main()
