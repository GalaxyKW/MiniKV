#!/usr/bin/env python3
"""Offline trust-boundary checks for the archived fixed-arrival measurements."""
from collections import Counter
import importlib.util
import json
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "benmark/baselines/2026-09-22-fixed-arrival"
sys.path.insert(0, str(ROOT / "benmark"))
import summarize

spec = importlib.util.spec_from_file_location("fixed_arrival_evidence", BASELINE / "analyze.py")
analysis = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analysis)


def save_json(path, value):
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")


class FixedArrivalEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary = tempfile.TemporaryDirectory(prefix="minikv-arrival-archive-")
        cls.addClassCleanup(temporary.cleanup)
        destination = Path(temporary.name)
        # Manually copy regular files: no tar link targets, permissions, or traversal.
        with tarfile.open(BASELINE / "fixed-arrival.tar.gz", "r:gz") as archive:
            for member in archive.getmembers():
                relative = PurePosixPath(member.name)
                if (relative.is_absolute() or ".." in relative.parts or "\\" in member.name
                        or not relative.parts or relative.parts[0] != "fixed-arrival"
                        or not (member.isdir() or member.isfile())):
                    raise ValueError("unsafe evidence archive member: " + member.name)
                target = destination.joinpath(*relative.parts)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.extractfile(member) as source, target.open("xb") as output:
                        shutil.copyfileobj(source, output)
        cls.fixture = destination / "fixed-arrival"

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="minikv-arrival-case-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name) / "fixed-arrival"
        shutil.copytree(self.fixture, self.directory)

    def analyze(self, phase="formal"):
        return analysis.analyze(self.directory, phase, ROOT, summarize)

    @staticmethod
    def row(result, stage, snapshot):
        return next(row for row in result["runs"] if row["stage"] == stage and row["snapshot"] == snapshot)

    def assert_complete(self, result, count, per_group):
        self.assertTrue(result["analysis_pass"], result["errors"])
        self.assertTrue(result["supported_isolation"])
        self.assertEqual(len(result["runs"]), count)
        self.assertEqual(result["complete_validated_measurements"], count)
        self.assertEqual(len(result["groups"]), 6)
        self.assertTrue(all(group["numeric_eligible"] == per_group for group in result["groups"]))
        self.assertEqual({stage["binary_verification"] for stage in result["stages"]}, {"recorded_hashes_only"})
        self.assertFalse(result["generator_quality_failed_runs"])
        self.assertLessEqual(max(row["metrics"]["late_fraction"] for row in result["runs"]), .01)

    def test_formal_preserves_expected_degradation_as_valid_measurement(self):
        result = self.analyze()
        self.assert_complete(result, 18, 3)
        self.assertEqual(Counter(row["status"] for row in result["runs"]), {"valid": 5, "degraded": 13})
        late_reference = self.row(result, "A3", "off")
        self.assertEqual(late_reference["metrics"]["late"], 1)
        self.assertTrue(late_reference["complete_validated_measurement"])

    def test_pilot_is_validated_separately_from_formal(self):
        result = self.analyze("pilot")
        self.assert_complete(result, 6, 1)
        self.assertEqual({row["metrics"]["planned"] for row in result["runs"]}, {1000})

    def test_capacity_missing_report_and_source_faults_retain_rows_but_exclude_groups(self):
        path = self.directory / "formal/A1/r01-reliable-snapshot-off/stats.jsonl"
        samples = [json.loads(line) for line in path.read_text().splitlines()]
        samples[len(samples) // 2]["body"]["server"]["requests_inflight"] = 133
        path.write_text("".join(json.dumps(sample) + "\n" for sample in samples), encoding="utf-8")
        (self.directory / "formal/B1/r01-reliable-snapshot-on/report.json").unlink()
        path = self.directory / "formal/C1/manifest.json"
        manifest = json.loads(path.read_text())
        manifest["metadata"]["git"]["tracked_dirty"] = True
        save_json(path, manifest)
        result = self.analyze()
        self.assertFalse(result["analysis_pass"])
        self.assertEqual(len(result["runs"]), 18)
        self.assertTrue(self.row(result, "A1", "off")["invariant_errors"])
        self.assertEqual(self.row(result, "A1", "off")["metrics"]["planned"], 5000)
        self.assertTrue(self.row(result, "B1", "on")["artifact_errors"])
        self.assertIsNone(self.row(result, "B1", "on")["metrics"]["successes"])
        self.assertTrue(self.row(result, "C1", "off")["artifact_errors"])
        self.assertEqual({(group["arm"], group["snapshot"]): group["numeric_eligible"] for group in result["groups"]},
                         {("A", "off"): 2, ("A", "on"): 3, ("B", "off"): 3,
                          ("B", "on"): 2, ("C", "off"): 2, ("C", "on"): 2})

    def test_missing_or_malformed_auxiliary_evidence_preserves_all_reports(self):
        for name in ("host-context.json", "formal/execution.json"):
            for replacement in (None, "[{}]" if name.startswith("formal/") else "[]"):
                with self.subTest(name=name, replacement=replacement):
                    path = self.directory / name
                    original = path.read_bytes()
                    try:
                        if replacement is None:
                            path.unlink()
                        else:
                            path.write_text(replacement, encoding="utf-8")
                        result = self.analyze()
                        self.assertFalse(result["analysis_pass"])
                        self.assertEqual(len(result["runs"]), 18)
                        self.assertEqual(self.row(result, "A1", "off")["metrics"]["successes"], 5000)
                        self.assertTrue(all(group["numeric_eligible"] == 0 for group in result["groups"]))
                    finally:
                        path.write_bytes(original)

    def test_unattributed_503_prevents_isolation_without_invalidating_measurement(self):
        directory = self.directory / "formal/C1/r01-reliable-snapshot-off"

        def remove_one_server_rejection(body):
            server = body["server"]
            server["requests_rejected_total"] = max(0, server["requests_rejected_total"] - 1)

        # Model a 503 from engine Busy: keep counters monotone and quiet evidence equal.
        for phase in ("before", "after", "settled"):
            path = directory / ("stats-" + phase + ".json")
            body = json.loads(path.read_text())
            remove_one_server_rejection(body)
            save_json(path, body)
        path = directory / "stats.jsonl"
        samples = [json.loads(line) for line in path.read_text().splitlines()]
        for sample in samples:
            if sample["error"] is None:
                remove_one_server_rejection(sample["body"])
        path.write_text("".join(json.dumps(sample) + "\n" for sample in samples), encoding="utf-8")
        result = self.analyze()
        row = self.row(result, "C1", "off")
        self.assertTrue(result["analysis_pass"])
        self.assertFalse(result["supported_isolation"])
        self.assertTrue(row["complete_validated_measurement"])
        self.assertFalse(row["artifact_errors"] or row["invariant_errors"])
        self.assertFalse(row["attribution_supported"])
        self.assertIn("settled_http503_equals_server_rejected_delta", row["attribution_failed_checks"])
        self.assertTrue(all(group["numeric_eligible"] == 3 for group in result["groups"]))


if __name__ == "__main__":
    unittest.main()
