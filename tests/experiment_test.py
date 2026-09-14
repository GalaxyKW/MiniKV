#!/usr/bin/env python3
"""Black-box checks for the isolated benchmark experiment runner."""

import contextlib
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "benmark" / "experiment.py"

# Real child processes exercise subprocess lifetime, OS ports and output files.
# A fixture file supplies failure modes without requiring custom environment
# variables to survive the runner's child-environment filtering.
FAKE_PROGRAM = r'''
import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import sys
import time
from urllib.parse import urlsplit

fixtures = Path(FIXTURE_ROOT)
role = Path(__file__).name
control = json.loads((fixtures / "control.json").read_text())
pid = os.getpid()
(fixtures / "pids" / (str(pid) + ".json")).write_text(json.dumps({"pid": pid, "role": role, "script": str(Path(__file__).resolve())}))

def stop(signum, frame):
    marker = fixtures / "stops" / (str(pid) + ".json")
    event = {"pid": pid, "role": role, "signal": signum, "entered_ns": time.monotonic_ns()}
    marker.write_text(json.dumps(event))
    time.sleep(control.get("shutdown_delay", {}).get(role, 0))
    event["finished_ns"] = time.monotonic_ns()
    marker.write_text(json.dumps(event))
    raise SystemExit(0)

signal.signal(signal.SIGTERM, signal.SIG_IGN if role in control.get("ignore_term", []) else stop)
signal.signal(signal.SIGINT, stop)

if control.get(role + "_startup") == "exit":
    print("injected " + role + " startup failure", flush=True)
    sys.exit(7)
if control.get(role + "_startup") == "hang":
    time.sleep(3600)

if role == "engine":
    address = (os.environ.get("MINIKV_ENGINE_HOST", "127.0.0.1"), int(os.environ["MINIKV_ENGINE_PORT"]))
    data = Path(os.environ["MINIKV_DATA_DIR"])
    data.mkdir(parents=True, exist_ok=True)
    (data / "fake-storage").write_text("isolated test data")
    state = {"mode": os.environ["MINIKV_WAL_MODE"], "snapshot_ms": int(os.environ["MINIKV_SNAPSHOT_INTERVAL_MS"]), "started": time.monotonic(),
             "workers": int(os.environ.get("MINIKV_WORKERS", "20")),
             "queue": int(os.environ.get("MINIKV_REQUEST_QUEUE_SIZE", "128")),
             "connections": int(os.environ.get("MINIKV_MAX_CONNECTIONS", "256")),
             "wal_queue": int(os.environ.get("MINIKV_WAL_QUEUE_BYTES", "16777216"))}
    (fixtures / ("engine-" + str(address[1]) + ".json")).write_text(json.dumps(state))
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(address)
        server.listen()
        print("MiniKV engine listening on %s:%d" % address, flush=True)
        while True:
            connection, _ = server.accept()
            connection.close()
elif role == "gateway":
    host, port = os.environ["MINIKV_HTTP_ADDR"].rsplit(":", 1)
    engine_port = os.environ["MINIKV_ENGINE_ADDR"].rsplit(":", 1)[1]
    state = json.loads((fixtures / ("engine-" + engine_port + ".json")).read_text())

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path != "/stats":
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"NOT_FOUND\n")
                return
            snapshots = 1
            if state["snapshot_ms"]:
                snapshots += int((time.monotonic() - state["started"]) * 1000 / state["snapshot_ms"])
            payload = {
                "schema_version": 1,
                "engine": {
                    "wal_mode": state["mode"], "keys": 0, "applied_sequence": 0, "durable_sequence": 0,
                    "wal_pending_bytes": 0, "wal_inflight_bytes": 0, "wal_queued_records": 0,
                    "wal_queue_capacity_bytes": state["wal_queue"], "wal_commits_total": 0, "wal_commit_failures_total": 0,
                    "wal_commit_duration_ns_total": 0, "wal_commit_last_duration_ns": 0,
                    "snapshot_successes_total": snapshots,
                    "snapshot_failures_total": int(bool(control.get("snapshot_failure")) and (fixtures / ("benchmark-started-" + port)).exists()),
                    "snapshot_in_progress": False, "snapshot_sequence": 0,
                    "snapshot_capture_duration_ns_total": 0, "snapshot_write_duration_ns_total": 0,
                    "snapshot_compact_duration_ns_total": 0, "io_failed": False, "stopping": False,
                },
                "server": {"connections": 0, "connection_capacity": state["connections"], "request_queue_depth": 0,
                    "request_queue_capacity": state["queue"], "workers_active": 0, "workers_capacity": state["workers"],
                    "requests_rejected_total": 0, "connections_rejected_total": 0},
                "gateway": {"uptime_seconds": time.monotonic() - state["started"], "rpc": {
                    "pool_capacity": int(os.environ.get("MINIKV_RPC_POOL_SIZE", "64")), "pool_in_use": 0, "connections": 0, "idle_connections": 0,
                    "closed": False, "calls_total": 0, "errors_total": 0, "retries_total": 0,
                    "pool_acquires_total": 0, "pool_wait_duration_ns_total": 0, "exchanges_total": 0,
                    "exchange_errors_total": 0, "exchange_duration_ns_total": 0,
                }},
            }
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    HTTPServer((host, int(port)), Handler).serve_forever()
elif role == "bench":
    parser = argparse.ArgumentParser()
    for flag, default in [("url", ""), ("workers", "1"), ("requests", "1"), ("op", "mixed"),
                          ("keyspace", "1"), ("timeout", "2s"), ("write-ratio", "20"), ("delete-ratio", "5"),
                          ("preload-count", "0"), ("seed", "1"), ("value-size", "128"), ("format", "json")]:
        parser.add_argument("-" + flag, default=default)
    parser.add_argument("-preload", default="true")
    args = vars(parser.parse_args())
    (fixtures / "pids" / (str(pid) + ".args.json")).write_text(json.dumps(args))
    (fixtures / ("benchmark-started-" + str(urlsplit(args["url"]).port))).touch()
    if control.get("replace_sources") and not (fixtures / "sources-replaced").exists():
        for name in ("engine", "gateway", "bench"):
            (fixtures / name).write_text("#!" + sys.executable + "\nraise SystemExit(93)\n")
        (fixtures / "sources-replaced").touch()
    behavior = control.get("bench", "valid")
    if behavior == "timeout":
        time.sleep(3600)
    time.sleep(0.15)
    if behavior == "malformed":
        print("not a JSON experiment")
        sys.exit(0)
    now = datetime.now(timezone.utc).isoformat()
    count = int(args["requests"])
    durations = {"ns": 1, "us": 1000, "ms": 1000000, "s": 1000000000, "m": 60000000000}
    timeout = next(int(float(args["timeout"][:-len(unit)]) * multiplier)
                   for unit, multiplier in durations.items() if args["timeout"].endswith(unit))
    preloaded = 0
    if args["preload"] == "true" and args["op"] in ("get", "mixed"):
        preloaded = min(int(args["preload_count"]) or int(args["keyspace"]), int(args["keyspace"]))
    config = {"url": args["url"], "workers": int(args["workers"]), "requests": count,
              "operation": args["op"], "keyspace": int(args["keyspace"]), "timeout_ns": timeout,
              "write_ratio": int(args["write_ratio"]), "delete_ratio": int(args["delete_ratio"]),
              "preload": args["preload"] == "true", "preload_count": int(args["preload_count"]),
              "seed": int(args["seed"]), "value_size": int(args["value_size"])}
    payload = {"schema_version": 1, "started_at": now, "measurement_started_at": now,
               "complete": True, "load_model": "closed_loop", "workload_generator": "indexed-pcg-v1",
               "config": config, "client_build": {"go_version": "fixture", "goos": "linux", "goarch": "amd64"},
               "preload": {"target_keys": preloaded, "completed_keys": preloaded, "elapsed_ns": 1}, "elapsed_ns": 100000000,
               "outcomes": {"requests": count, "successes": count, "logical_misses": 0, "failures": 0,
                            "network_errors": 0, "timeouts": 0, "transport_errors": 0, "http_failures": 0, "protocol_failures": 0},
               "operations": {"put": 0, "get": count, "delete": 0}, "http_statuses": {"200": count},
               "latency_ns": {"samples": count, "mean": 1000, "min": 1000, "p50": 1000,
                              "p95": 1000, "p99": 1000, "p99_9": 1000, "max": 1000},
               "qps_total": count * 10, "qps_successful": count * 10, "system_success_rate_pct": 100}
    if behavior == "incomplete":
        payload["complete"] = False
        payload["error"] = "preload_failed"
    elif behavior == "inconsistent":
        payload["outcomes"]["requests"] = count + 1
    elif behavior == "failed_requests":
        payload["outcomes"]["successes"] = count - 1
        payload["outcomes"]["failures"] = 1
        payload["outcomes"]["http_failures"] = 1
    elif behavior == "nan_rate":
        payload["qps_total"] = float("nan")
    elif behavior == "infinite_rate":
        payload["qps_total"] = float("inf")
    elif behavior == "huge_rate":
        payload["qps_total"] = 10 ** 1000
    elif behavior == "config_mismatch":
        payload["config"]["seed"] += 1
    print(json.dumps(payload))
    if behavior == "trailing_json":
        print("{}")
    print("fixture benchmark progress", file=sys.stderr)
    sys.exit(1 if behavior == "exit_failure" else 0)
else:
    raise RuntimeError("unknown fixture role")
'''


class ExperimentRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="minikv-experiment-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.fixtures = self.directory / "fake binaries"
        self.fixtures.mkdir()
        (self.fixtures / "pids").mkdir()
        (self.fixtures / "stops").mkdir()
        self.control = self.fixtures / "control.json"
        self.control.write_text("{}")
        self.output = self.directory / "new experiment"
        self.guard = self.directory / "ambient data"
        self.guard.mkdir()
        (self.guard / "keep").write_text("untouched")
        self.runner = None
        self.addCleanup(self.cleanup_processes)
        for role in ("engine", "gateway", "bench"):
            binary = self.fixtures / role
            binary.write_text("#!" + sys.executable + "\n# fixture role: " + role +
                              "\nFIXTURE_ROOT = " + repr(str(self.fixtures)) + "\n" + FAKE_PROGRAM)
            binary.chmod(0o700)

    def cleanup_processes(self):
        if self.runner is not None and self.runner.poll() is None:
            self.runner.kill()
            self.runner.wait(timeout=5)
        for record in self.pid_records():
            # Do not signal an unrelated process if a PID has already been reused.
            with contextlib.suppress(ProcessLookupError, FileNotFoundError):
                command = Path("/proc", str(record["pid"]), "cmdline").read_bytes()
                if record["script"].encode() in command.split(b"\0"):
                    os.kill(record["pid"], signal.SIGKILL)

    def pid_records(self):
        return [json.loads(path.read_text()) for path in (self.fixtures / "pids").glob("*.json")
                if not path.name.endswith(".args.json")]

    def assert_children_stopped(self):
        records = self.pid_records()
        for record in records:
            # A zombie is also a leaked child: the runner must reap its handles.
            self.assertFalse(Path("/proc", str(record["pid"])).exists(), "runner left a child process behind: %r" % record)
        return records

    def invoke(self, control=None, extra=(), expected=None, interrupt_signal=None, cleanup_signal=None):
        if control is not None:
            self.control.write_text(json.dumps(control))
        args = [sys.executable, str(RUNNER), "--output", str(self.output),
                "--engine", str(self.fixtures / "engine"), "--gateway", str(self.fixtures / "gateway"),
                "--bench", str(self.fixtures / "bench"), "--requests", "9", "--workers", "2", "--keyspace", "3",
                "--repeats", "1", "--modes", "throughput", "--snapshot-ms", "50", "--run-timeout", "2",
                "--startup-timeout", "1", "--shutdown-timeout", "1", "--sample-ms", "50",
                "--settle-timeout", "1", "--stats-ms", "50"] + list(extra)
        env = dict(os.environ, EXPERIMENT_SECRET_TOKEN="do-not-record-fixture-secret",
                   MINIKV_SECRET_TOKEN="do-not-record-prefixed-secret", MINIKV_DATA_DIR=str(self.guard))
        previous_pids = {record["pid"] for record in self.pid_records()}
        started = time.monotonic()
        self.runner = subprocess.Popen(args, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, start_new_session=True)
        if interrupt_signal is not None:
            deadline = time.monotonic() + 5
            while not any(record["role"] == "bench" and record["pid"] not in previous_pids for record in self.pid_records()):
                if self.runner.poll() is not None or time.monotonic() >= deadline:
                    self.fail("benchmark child never became live before interrupt test")
                time.sleep(0.01)
            # Model a terminal signal for the runner's process group. Child
            # sessions must remain isolated until the runner stops them in order.
            os.killpg(self.runner.pid, interrupt_signal)
        if cleanup_signal is not None:
            deadline = time.monotonic() + 5
            while not any(record["role"] == "gateway" and record["pid"] not in previous_pids
                          and (self.fixtures / "stops" / (str(record["pid"]) + ".json")).is_file()
                          for record in self.pid_records()):
                if self.runner.poll() is not None or time.monotonic() >= deadline:
                    self.fail("gateway never entered graceful shutdown before interrupt test")
                time.sleep(0.01)
            self.cleanup_signal_ns = time.monotonic_ns()
            os.killpg(self.runner.pid, cleanup_signal)
        try:
            stdout, stderr = self.runner.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            self.fail("experiment runner exceeded the test's outer deadline")
        elapsed = time.monotonic() - started
        if expected is not None:
            self.assertEqual(self.runner.returncode, expected, "stdout=%s\nstderr=%s" % (stdout, stderr))
        self.assert_children_stopped()
        self.assertEqual(sorted(path.name for path in self.guard.iterdir()), ["keep"], "runner used ambient MINIKV_DATA_DIR")
        return stdout, stderr, elapsed

    def test_existing_output_is_never_overwritten(self):
        self.output.mkdir()
        marker = self.output / "manifest.json"
        marker.write_text("existing user experiment\n")
        self.invoke(expected=2)
        self.assertEqual(marker.read_text(), "existing user experiment\n")
        self.assertEqual(sorted(path.name for path in self.output.iterdir()), ["manifest.json"])
        self.assertEqual(self.pid_records(), [])

    def test_matrix_keeps_reports_logs_stats_and_private_data_separate(self):
        self.invoke(extra=("--modes", "throughput,reliable"), expected=0)
        self.assertTrue((self.output / "manifest.json").is_file())
        self.assertTrue((self.output / "index.json").is_file())
        index = json.loads((self.output / "index.json").read_text())
        self.assertEqual(index["status"], "complete")
        self.assertEqual(index["successful_runs"], 4)
        self.assertEqual(index["failed_runs"], 0)
        runs = sorted(path for path in self.output.iterdir() if path.is_dir() and path.name.startswith("r"))
        self.assertEqual([path.name for path in runs], [
            "r01-reliable-snapshot-off", "r01-reliable-snapshot-on",
            "r01-throughput-snapshot-off", "r01-throughput-snapshot-on",
        ])
        for run in runs:
            for name in ("data", "report.json", "benchmark.stderr.log", "engine.log", "gateway.log",
                         "stats-before.json", "stats-after.json", "stats-settled.json", "stats.jsonl", "resources.jsonl", "result.json"):
                self.assertTrue((run / name).exists(), "%s missing %s" % (run.name, name))
            report = json.loads((run / "report.json").read_text())
            self.assertEqual(json.loads((run / "result.json").read_text())["status"], "ok")
            self.assertTrue(report["complete"])
            self.assertEqual(report["outcomes"]["requests"], 9)
            self.assertNotIn("fixture benchmark progress", (run / "report.json").read_text())
            self.assertIn("fixture benchmark progress", (run / "benchmark.stderr.log").read_text())
            before = json.loads((run / "stats-before.json").read_text())
            self.assertEqual(before["engine"]["wal_mode"], "reliable" if "reliable" in run.name else "throughput")
            samples = [json.loads(line) for line in (run / "resources.jsonl").read_text().splitlines() if line]
            self.assertGreater(len(samples), 0, "no actual process resources were sampled")
        for path in self.output.rglob("*"):
            if path.is_file():
                content = path.read_bytes()
                self.assertNotIn(b"do-not-record-fixture-secret", content, str(path))
                self.assertNotIn(b"do-not-record-prefixed-secret", content, str(path))

    def test_startup_failure_stops_every_process_already_started(self):
        self.invoke(control={"gateway_startup": "exit"}, expected=1)
        roles = {record["role"] for record in self.pid_records()}
        self.assertIn("engine", roles)
        self.assertIn("gateway", roles)
        self.assertNotIn("bench", roles)

    def test_rebuilding_source_binaries_does_not_change_a_running_matrix(self):
        originals = {role: (self.fixtures / role).read_bytes() for role in ("engine", "gateway", "bench")}
        self.invoke(control={"replace_sources": True}, expected=0)
        self.assertTrue((self.fixtures / "sources-replaced").exists(), "source replacement was not exercised")
        index = json.loads((self.output / "index.json").read_text())
        self.assertEqual(index["successful_runs"], 2, "the next matrix case used rebuilt source executables")
        self.assertEqual(index["failed_runs"], 0)
        manifest = json.loads((self.output / "manifest.json").read_text())
        for role, original in originals.items():
            preserved = self.output / "binaries" / role
            self.assertEqual(preserved.read_bytes(), original, "preserved executable changed during the experiment")
            self.assertEqual(stat.S_IMODE(preserved.stat().st_mode), 0o555)
            self.assertNotEqual((self.fixtures / role).read_bytes(), original)
            digest = hashlib.sha256(original).hexdigest()
            self.assertEqual(manifest["executables"][role]["sha256"], digest)
            self.assertEqual(manifest["metadata"]["binaries"][role]["sha256"], digest)
        for record in self.pid_records():
            self.assertEqual(Path(record["script"]), self.output / "binaries" / record["role"])

    def test_snapshot_failures_disqualify_otherwise_successful_measurements(self):
        self.invoke(control={"snapshot_failure": True}, expected=1)
        reports = list(self.output.glob("r*/report.json"))
        self.assertTrue(reports, "test did not reach the measured workload")
        for report in reports:
            self.assertTrue(json.loads(report.read_text())["complete"], "fake benchmark did not finish normally")
            directory = report.parent
            before = json.loads((directory / "stats-before.json").read_text())
            after = json.loads((directory / "stats-after.json").read_text())
            self.assertEqual(before["engine"]["snapshot_failures_total"], 0)
            self.assertEqual(after["engine"]["snapshot_failures_total"], 1)
            self.assertEqual(json.loads((directory / "result.json").read_text())["status"], "failed")
        self.assertEqual(json.loads((self.output / "index.json").read_text())["successful_runs"], 0)

    def test_startup_timeout_is_bounded_and_reaps_unready_child(self):
        _, _, elapsed = self.invoke(control={"engine_startup": "hang"}, expected=1)
        self.assertLess(elapsed, 12)
        self.assertTrue(self.pid_records(), "test did not start a child")
        self.assertNotIn("bench", {record["role"] for record in self.pid_records()})

    def test_benchmark_timeout_cleans_up_even_children_ignoring_sigterm(self):
        _, _, elapsed = self.invoke(control={"bench": "timeout", "ignore_term": ["engine", "gateway", "bench"]},
                                    extra=("--run-timeout", "1"), expected=1)
        self.assertLess(elapsed, 18)
        self.assertEqual({record["role"] for record in self.pid_records()}, {"engine", "gateway", "bench"})

    def test_bad_or_failed_reports_cannot_be_successful_experiments(self):
        for behavior in ("malformed", "trailing_json", "incomplete", "inconsistent", "failed_requests", "exit_failure",
                         "nan_rate", "infinite_rate", "huge_rate", "config_mismatch"):
            with self.subTest(behavior=behavior):
                self.output = self.directory / behavior
                self.invoke(control={"bench": behavior}, expected=1)
                self.assertTrue(list(self.output.glob("*/result.json")), "failure did not preserve a run result")
                self.assertTrue(list(self.output.glob("*/report.json")), "failure lost the raw benchmark output")
                for path in self.output.glob("*/result.json"):
                    self.assertEqual(json.loads(path.read_text())["status"], "failed", path.read_text())
                self.assertEqual(json.loads((self.output / "index.json").read_text())["successful_runs"], 0)

    def test_terminal_signals_interrupt_runner_and_reap_isolated_children(self):
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signum):
                self.output = self.directory / ("interrupted-" + str(signum))
                self.invoke(control={"bench": "timeout"}, extra=("--run-timeout", "10"), expected=1,
                            interrupt_signal=signum)
                index = json.loads((self.output / "index.json").read_text())
                self.assertEqual(index["status"], "interrupted")
                self.assertEqual(index["successful_runs"], 0)
                results = list(self.output.glob("*/result.json"))
                self.assertEqual(len(results), 1, "runner continued the matrix after interruption")
                self.assertEqual(json.loads(results[0].read_text())["status"], "interrupted")
                for record in self.pid_records():
                    stopped = json.loads((self.fixtures / "stops" / (str(record["pid"]) + ".json")).read_text())
                    self.assertEqual(stopped["signal"], signal.SIGTERM, "terminal signal reached a child session directly")

    def test_nonfinite_timeout_arguments_do_not_launch_children(self):
        for raw in ("nan", "inf", "-inf", "0", "-1"):
            with self.subTest(timeout=raw):
                self.invoke(extra=("--run-timeout=" + raw,), expected=2)
                self.assertFalse(self.output.exists(), "invalid parameters created an experiment directory")
                self.assertEqual(self.pid_records(), [])

    def test_first_terminal_signal_during_cleanup_stops_the_matrix(self):
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signum):
                self.output = self.directory / ("cleanup-interrupted-" + str(signum))
                self.invoke(control={"shutdown_delay": {"gateway": 0.3}}, expected=1, cleanup_signal=signum)
                index = json.loads((self.output / "index.json").read_text())
                self.assertEqual(index["status"], "interrupted")
                self.assertEqual(index["successful_runs"], 0)
                results = list(self.output.glob("r*/result.json"))
                self.assertEqual(len(results), 1, "cleanup signal was lost and the next matrix case started")
                result = json.loads(results[0].read_text())
                self.assertEqual(result["status"], "interrupted")
                report = json.loads((results[0].parent / "report.json").read_text())
                self.assertTrue(report["complete"], "signal did not arrive after a finished benchmark")
                self.assertEqual(report["outcomes"]["failures"], 0)
                events = {}
                for role in ("gateway", "engine"):
                    process = result["processes"][role]
                    self.assertEqual(process["returncode"], 0, "cleanup was interrupted before graceful exit")
                    self.assertFalse(process["forced"], "terminal signal unnecessarily forced child termination")
                    events[role] = json.loads((self.fixtures / "stops" / (str(process["pid"]) + ".json")).read_text())
                    self.assertEqual(events[role]["signal"], signal.SIGTERM)
                self.assertLessEqual(events["gateway"]["entered_ns"], self.cleanup_signal_ns)
                self.assertLess(self.cleanup_signal_ns, events["gateway"]["finished_ns"], "test missed the shutdown delay window")
                self.assertLessEqual(events["gateway"]["finished_ns"], events["engine"]["entered_ns"],
                                     "engine was stopped before gateway finished draining")


if __name__ == "__main__":
    unittest.main()
