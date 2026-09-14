"""Keep preload/after-run activity out of measurement-window summaries."""

import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benmark"))
from experiment import observations, timestamp_ns


class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.report = {"measurement_started_at": "2026-09-14T08:00:10Z", "elapsed_ns": 10_000_000_000}

    def stats(self, start, finish, completed, in_progress=False, error=None):
        return {"started_at": f"2026-09-14T08:00:{start:02d}Z",
                "finished_at": f"2026-09-14T08:00:{finish:02d}Z", "error": error,
                "body": {"engine": {"snapshot_successes_total": completed,
                                    "snapshot_in_progress": in_progress}}}

    def summarize(self, stats, resources=(), interval=1000):
        for name, samples in (("stats", stats), ("resources", resources)):
            (self.directory / (name + ".jsonl")).write_text("".join(json.dumps(sample) + "\n" for sample in samples))
        return observations(self.directory, self.report, interval)

    def test_rfc3339_fraction_timezone_and_epoch_boundaries_are_exact(self):
        self.assertEqual(timestamp_ns("1970-01-01T00:00:00Z"), 0)
        self.assertEqual(timestamp_ns("1970-01-01T08:00:00.000000001+08:00"), 1)
        self.assertEqual(timestamp_ns("1969-12-31T23:59:59.999999999Z"), -1)
        self.assertEqual(timestamp_ns("2026-09-14T08:00:10.123456789Z") -
                         timestamp_ns("2026-09-14T08:00:10Z"), 123456789)
        self.assertEqual(timestamp_ns("1970-01-01T00:00:00.1Z"), 100000000)
        for value in ("2026-09-14T08:00:10", "2026-09-14T08:00:10.1234567890Z", "invalid"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                timestamp_ns(value)

    def test_preload_and_boundary_queries_do_not_prove_measurement_snapshots(self):
        result = self.summarize([
            self.stats(1, 2, 1), self.stats(8, 9, 9), self.stats(9, 11, 9, True),
            self.stats(12, 13, 9), self.stats(17, 18, 9), self.stats(19, 21, 10, True),
        ])
        self.assertEqual(result["measurement_stats_samples"], 2)
        self.assertEqual(result["measurement_observed_snapshot_completions"], 0)
        self.assertFalse(result["snapshot_activity_observed"])

    def test_interior_completion_and_in_progress_are_separate_evidence(self):
        completed = self.summarize([self.stats(11, 12, 8), self.stats(18, 19, 10)])
        self.assertEqual(completed["measurement_observed_snapshot_completions"], 2)
        self.assertTrue(completed["snapshot_activity_observed"])
        active = self.summarize([self.stats(12, 13, 8, True)])
        self.assertIsNone(active["measurement_observed_snapshot_completions"])
        self.assertTrue(active["snapshot_activity_observed"])

    def test_missing_or_failed_samples_remain_unknown(self):
        for samples in ([], [self.stats(12, 13, 5, True, "timeout")]):
            with self.subTest(samples=samples):
                result = self.summarize(samples)
                self.assertIsNone(result["measurement_observed_snapshot_completions"])
                self.assertFalse(result["snapshot_activity_observed"])
                self.assertEqual(result["stats_errors"], len(samples))
                for memory in result["memory"].values():
                    self.assertIsNone(memory["observed_lifetime_hwm_bytes"])
                    self.assertIsNone(memory["measurement_sampled_rss_max_bytes"])
                    self.assertEqual(memory["measurement_rss_samples"], 0)
        self.assertIsNone(self.summarize([], interval=0)["snapshot_activity_observed"])

    def test_lifetime_hwm_is_not_measurement_peak_and_null_is_not_zero(self):
        def resource(second, rss, hwm):
            return {"processes": {"engine": {"sampled_at_utc": f"2026-09-14T08:00:{second:02d}Z",
                                              "rss_bytes": rss, "hwm_bytes": hwm}}}
        result = self.summarize([], [resource(8, 900, 900), resource(12, 100, 900),
                                     resource(13, None, None), resource(18, 200, 900), resource(22, 950, 950)])
        self.assertEqual(result["memory"]["engine"], {"observed_lifetime_hwm_bytes": 950,
                         "measurement_sampled_rss_max_bytes": 200, "measurement_rss_samples": 2})


if __name__ == "__main__":
    unittest.main()
