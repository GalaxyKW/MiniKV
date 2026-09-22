#!/usr/bin/env python3
"""Exercise the built C++ engine and Go gateway against temporary local data."""

import contextlib
import http.client
import json
import os
from pathlib import Path
import random
import resource
import select
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import unittest
import zlib
from urllib.parse import urlencode

from history_checker import Operation as HistoryOperation, check_history


ROOT = Path(__file__).resolve().parents[1]
ENGINE = Path(os.environ.get("MINIKV_TEST_ENGINE", ROOT / "build" / "engine"))
GATEWAY = ROOT / "bin" / "minikv-go"
BENCH = ROOT / "bin" / "minikv-bench"


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def frame(op, key, value=b""):
    return struct.pack("!4sB3xII", b"MKV1", op, len(key), len(value)) + key + value


def read_exact(sock, length):
    result = b""
    while len(result) < length:
        data = sock.recv(length - len(result))
        if not data:
            raise EOFError("incomplete response")
        result += data
    return result


def read_response(sock):
    magic, status, length = struct.unpack("!4sB3xI", read_exact(sock, 12))
    if magic != b"MKR1" or length > 1024 * 1024:
        raise AssertionError("invalid response header")
    return status, read_exact(sock, length)


class MiniKVIntegration(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="minikv-integration-")
        self.addCleanup(self.directory.cleanup)
        self.engine_port = unused_port()
        self.http_port = unused_port()
        while self.http_port == self.engine_port:
            self.http_port = unused_port()
        self.engine = None
        self.gateway = None
        self.log_files = []
        self.addCleanup(self.cleanup_processes)
        self.engine_env = dict(os.environ, **{
            "MINIKV_DATA_DIR": str(Path(self.directory.name) / "data"),
            "MINIKV_MAX_DATA_BYTES": "0",
            "MINIKV_ENGINE_HOST": "127.0.0.1",
            "MINIKV_ENGINE_PORT": str(self.engine_port),
            "MINIKV_WAL_MODE": "reliable",
            "MINIKV_WAL_BATCH_SIZE": "64",
            "MINIKV_WAL_FLUSH_MS": "2",
            "MINIKV_WAL_QUEUE_BYTES": str(16 * 1024 * 1024),
            "MINIKV_SNAPSHOT_INTERVAL_MS": "50",
            "MINIKV_WORKERS": "2",
            "MINIKV_MAX_CONNECTIONS": "128",
            "MINIKV_REQUEST_QUEUE_SIZE": "128",
            "MINIKV_CLIENT_IDLE_MS": "30000",
        })
        self.gateway_env = dict(os.environ, **{
            "MINIKV_ENGINE_ADDR": f"127.0.0.1:{self.engine_port}",
            "MINIKV_HTTP_ADDR": f"127.0.0.1:{self.http_port}",
            "MINIKV_RPC_POOL_SIZE": "16",
            "MINIKV_RPC_TIMEOUT_MS": "2000",
            # Use the configured pool size as the default even when a test
            # restarts with a smaller pool; do not inherit a host override.
            "MINIKV_HTTP_MAX_INFLIGHT": "",
        })
        self.start_engine()
        self.start_gateway()

    def launch(self, binary, env, port, name, nofile_limit=None):
        log_path = Path(self.directory.name) / f"{name}.log"
        log = log_path.open("ab")
        self.log_files.append(log)
        def limit_files():
            resource.setrlimit(resource.RLIMIT_NOFILE, (nofile_limit, nofile_limit))

        process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log, cwd=ROOT,
                                   preexec_fn=limit_files if nofile_limit is not None else None)
        # Register cleanup before waiting for startup, including failed startup.
        setattr(self, name, process)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if process.poll() is not None:
                self.fail(f"{name} exited: {log_path.read_text(errors='replace')}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    return
            except OSError:
                time.sleep(0.02)
        self.fail(f"{name} did not start: {log_path.read_text(errors='replace')}")

    def start_engine(self, nofile_limit=None):
        self.launch(ENGINE, self.engine_env, self.engine_port, "engine", nofile_limit=nofile_limit)

    def start_gateway(self):
        self.launch(GATEWAY, self.gateway_env, self.http_port, "gateway")

    def stop(self, name, kill=False):
        process = getattr(self, name)
        if process is not None:
            if process.poll() is None:
                process.kill() if kill else process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
                self.fail(f"{name} did not stop")
            setattr(self, name, None)
            if not kill:
                self.assertEqual(process.returncode, 0, f"{name} shutdown failed")

    def cleanup_processes(self):
        for name in ("gateway", "engine"):
            process = getattr(self, name)
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
        for log in self.log_files:
            log.close()

    def request(self, method, key=None, value=None, body=None):
        path = "/kv"
        if method == "POST" and body is None:
            body = json.dumps({"key": key, "value": value}).encode()
        elif key is not None:
            path += "?" + urlencode({"key": key})
        connection = http.client.HTTPConnection("127.0.0.1", self.http_port, timeout=4)
        try:
            connection.request(method, path, body, {"Content-Type": "application/json"})
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def rpc_socket(self):
        return socket.create_connection(("127.0.0.1", self.engine_port), timeout=3)

    def runtime_stats(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.http_port, timeout=4)
        try:
            connection.request("GET", "/stats")
            response = connection.getresponse()
            self.assertEqual(response.getheader("Content-Type"), "application/json")
            self.assertEqual(response.getheader("Cache-Control"), "no-store")
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def wait_stats(self, predicate, message, timeout=2):
        deadline = time.monotonic() + timeout
        while True:
            code, stats = self.runtime_stats()
            self.assertEqual(code, 200)
            if predicate(stats):
                return stats
            self.assertLess(time.monotonic(), deadline, message)
            time.sleep(0.005)

    def test_http_admission_precedes_body_reads_and_recovers_after_disconnect(self):
        self.stop("gateway")
        self.gateway_env["MINIKV_HTTP_MAX_INFLIGHT"] = "1"
        self.start_gateway()
        with socket.create_connection(("127.0.0.1", self.http_port), timeout=4) as upload:
            # The incomplete upload must hold HTTP admission without using an
            # RPC slot. No payload is sent, so only admission can reject the
            # requests below before the server's 15-second body timeout.
            upload.sendall(b"POST /kv HTTP/1.1\r\nHost: localhost\r\n"
                           b"Content-Type: application/json\r\nContent-Length: 40\r\n\r\n")
            stats = self.wait_stats(lambda s: s["gateway"]["http"]["data"]["inflight"] == 1,
                                    "incomplete upload did not acquire HTTP admission")
            self.assertEqual(stats["gateway"]["http"]["data"],
                             {"capacity": 1, "inflight": 1, "rejected_total": 0})
            self.assertEqual(stats["gateway"]["http"]["stats"],
                             {"capacity": 1, "inflight": 1, "rejected_total": 0})
            self.assertEqual(stats["gateway"]["rpc"]["calls_total"], 0)
            self.assertEqual(stats["gateway"]["rpc"]["pool_in_use"], 0)
            self.assertEqual(stats["server"]["requests_started_total"], 0)
            # Cover both the small-body path that net/http might otherwise
            # drain before flushing and a large upload that must not decode.
            for length in (40, 1024 * 1024):
                with self.subTest(content_length=length), socket.create_connection(
                        ("127.0.0.1", self.http_port), timeout=4) as rejected:
                    rejected.sendall(("POST /kv HTTP/1.1\r\nHost: localhost\r\n"
                                      "Content-Type: application/json\r\n"
                                      f"Content-Length: {length}\r\n\r\n").encode("ascii"))
                    response = http.client.HTTPResponse(rejected)
                    try:
                        response.begin()
                        self.assertEqual(response.status, 503)
                        self.assertEqual(response.getheader("Connection"), "close")
                        self.assertEqual(response.read(), b"Gateway request capacity exhausted\n")
                    finally:
                        response.close()
            code, stats = self.runtime_stats()
            self.assertEqual(code, 200)
            self.assertEqual(stats["gateway"]["http"]["data"],
                             {"capacity": 1, "inflight": 1, "rejected_total": 2})
            self.assertEqual(stats["gateway"]["rpc"]["calls_total"], 0)
        stats = self.wait_stats(lambda s: s["gateway"]["http"]["data"]["inflight"] == 0,
                                "disconnected upload retained HTTP admission")
        self.assertEqual(stats["gateway"]["rpc"]["calls_total"], 0)
        self.assertEqual(self.request("POST", "recovered", "value"), (200, b"OK\n"))
        self.assertEqual(self.request("GET", "recovered"), (200, b"VALUE value\n"))
        stats = self.wait_stats(lambda s: s["gateway"]["http"]["data"]["inflight"] == 0,
                                "completed request retained HTTP admission")
        self.assertEqual(stats["gateway"]["rpc"]["calls_total"], 2)
        self.assertEqual(stats["gateway"]["http"]["data"]["rejected_total"], 2)
        self.assertEqual(stats["gateway"]["http"]["stats"]["rejected_total"], 0)

    def test_runtime_stats_and_recovery(self):
        self.assertEqual(self.request("POST", "private-key", "private-value"), (200, b"OK\n"))
        self.assertEqual(self.request("GET", "private-key"), (200, b"VALUE private-value\n"))
        stats = self.wait_stats(lambda s: s["server"]["requests_inflight"] == 0 and
                               s["engine"]["async_requests_inflight"] == 0, "completed requests did not release capacity")
        self.assertEqual(stats["schema_version"], 1)
        self.assertEqual(stats["engine"]["keys"], 1)
        self.assertEqual(stats["engine"]["data_bytes"], len(b"private-keyprivate-value"))
        self.assertEqual(stats["engine"]["data_capacity_bytes"], 0)
        self.assertEqual(stats["engine"]["data_rejections_total"], 0)
        self.assertEqual(stats["engine"]["wal_mode"], "reliable")
        self.assertEqual(stats["engine"]["applied_sequence"], 1)
        self.assertEqual(stats["engine"]["durable_sequence"], 1)
        self.assertEqual(stats["engine"]["wal_pending_bytes"], 0)
        self.assertGreaterEqual(stats["engine"]["wal_commits_total"], 1)
        self.assertEqual(stats["engine"]["wal_capacity_waiters"], 0)
        self.assertEqual(stats["engine"]["wal_capacity_waits_total"], 0)
        self.assertEqual(stats["engine"]["wal_capacity_wait_duration_ns_total"], 0)
        self.assertEqual(stats["engine"]["wal_durable_waiters"], 0)
        self.assertEqual(stats["engine"]["wal_durable_waits_total"], 1)
        self.assertGreater(stats["engine"]["wal_durable_wait_duration_ns_total"], 0)
        self.assertEqual(stats["engine"]["async_requests_inflight"], 0)
        self.assertEqual(stats["engine"]["async_requests_capacity"], 130)
        self.assertEqual(stats["engine"]["async_callback_failures_total"], 0)
        snapshot_fields = (
            "snapshot_capture_state_lock_acquisitions_total", "snapshot_capture_state_lock_duration_ns_total",
            "snapshot_capture_state_lock_duration_ns_max", "snapshot_file_write_calls_total",
            "snapshot_file_written_bytes_total", "snapshot_file_installed_bytes_total",
            "snapshot_compact_written_bytes_total",
        )
        for field in snapshot_fields:
            self.assertIs(type(stats["engine"][field]), int)
            self.assertGreaterEqual(stats["engine"][field], 0)
        self.assertGreaterEqual(stats["engine"]["snapshot_capture_state_lock_acquisitions_total"], 1)
        self.assertLessEqual(stats["engine"]["snapshot_capture_state_lock_duration_ns_max"],
                             stats["engine"]["snapshot_capture_state_lock_duration_ns_total"])
        self.assertGreaterEqual(stats["engine"]["snapshot_file_written_bytes_total"], 28)
        self.assertGreaterEqual(stats["engine"]["snapshot_file_installed_bytes_total"], 28)
        self.assertLessEqual(stats["engine"]["snapshot_file_installed_bytes_total"],
                             stats["engine"]["snapshot_file_written_bytes_total"])
        self.assertFalse(stats["engine"]["io_failed"])
        self.assertEqual(stats["server"]["workers_capacity"], 2)
        self.assertEqual(stats["server"]["requests_inflight"], 0)
        self.assertEqual(stats["server"]["requests_capacity"], 130)
        self.assertEqual(stats["server"]["requests_started_total"], 2)
        self.assertIsInstance(stats["server"]["request_queue_wait_duration_ns_total"], int)
        self.assertEqual(stats["gateway"]["rpc"]["calls_total"], 2)
        self.assertEqual(stats["gateway"]["rpc"]["errors_total"], 0)
        self.assertEqual(stats["gateway"]["rpc"]["pool_capacity"], 16)
        self.assertEqual(stats["gateway"]["http"]["data"],
                         {"capacity": 16, "inflight": 0, "rejected_total": 0})
        self.assertEqual(stats["gateway"]["http"]["stats"],
                         {"capacity": 1, "inflight": 1, "rejected_total": 0})
        self.assertNotIn("private-key", json.dumps(stats))
        self.assertNotIn("private-value", json.dumps(stats))
        self.assertNotIn(self.directory.name, json.dumps(stats))
        # The Stats operation is a framed read: it must preserve pipelining and
        # must not enter the WAL or accept key/value payloads.
        with self.rpc_socket() as sock:
            sock.sendall(frame(4, b"") + frame(2, b"private-key"))
            status, payload = read_response(sock)
            self.assertEqual(status, 1)
            self.assertEqual(json.loads(payload)["engine"]["applied_sequence"], 1)
            self.assertEqual(read_response(sock), (1, b"private-value"))
        with self.rpc_socket() as sock:
            sock.sendall(frame(4, b"private-key"))
            self.assertEqual(read_response(sock)[0], 3)
        self.stop("engine", kill=True)
        code, unavailable = self.runtime_stats()
        self.assertEqual(code, 503)
        self.assertEqual(unavailable["error"], "backend_unavailable")
        self.assertNotIn("engine", unavailable)
        self.assertEqual(unavailable["gateway"]["rpc"]["calls_total"], 2)
        # Keep process-counter reset observable before any new automatic work.
        self.engine_env["MINIKV_SNAPSHOT_INTERVAL_MS"] = "0"
        self.start_engine()
        code, recovered = self.runtime_stats()
        self.assertEqual(code, 200)
        self.assertEqual(recovered["engine"]["keys"], 1)
        self.assertEqual(recovered["engine"]["durable_sequence"], 1)
        self.assertEqual(recovered["engine"]["wal_commits_total"], 0)
        for field in ("wal_capacity_waiters", "wal_capacity_waits_total", "wal_capacity_wait_duration_ns_total",
                      "wal_durable_waiters", "wal_durable_waits_total", "wal_durable_wait_duration_ns_total"):
            self.assertEqual(recovered["engine"][field], 0)
        for field in snapshot_fields:
            self.assertEqual(recovered["engine"][field], 0)
        self.assertEqual(recovered["server"]["requests_started_total"], 0)
        self.assertEqual(recovered["server"]["request_queue_wait_duration_ns_total"], 0)
        self.assertEqual(recovered["gateway"]["rpc"]["calls_total"], 2)

    def test_data_capacity_rejects_writes_and_recovers_released_space(self):
        for mode in ("throughput", "reliable"):
            with self.subTest(mode=mode):
                self.stop("gateway")
                self.stop("engine")
                self.engine_env.update({"MINIKV_WAL_MODE": mode, "MINIKV_MAX_DATA_BYTES": "12",
                                        "MINIKV_DATA_DIR": str(Path(self.directory.name) / ("data-" + mode)),
                                        "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
                self.start_engine()
                self.start_gateway()
                self.assertEqual(self.request("POST", "a", "12345"), (200, b"OK\n"))
                self.assertEqual(self.request("POST", "b", "12345"), (200, b"OK\n"))
                self.wait_stats(lambda s: s["engine"]["durable_sequence"] == 2, "initial writes not durable")
                self.assertEqual(self.request("POST", "c", "")[0], 503)
                self.assertEqual(self.request("POST", "a", "123456")[0], 503)
                _, rejected = self.runtime_stats()
                state = rejected["engine"]
                self.assertEqual((state["keys"], state["data_bytes"], state["data_capacity_bytes"]), (2, 12, 12))
                self.assertEqual((state["applied_sequence"], state["durable_sequence"]), (2, 2))
                self.assertEqual(state["data_rejections_total"], 2)
                self.assertEqual(state["wal_pending_bytes"], 0)
                self.assertFalse(state["io_failed"])
                self.assertEqual(self.request("GET", "a"), (200, b"VALUE 12345\n"))
                self.assertEqual(self.request("GET", "c"), (404, b"NOT_FOUND\n"))
                self.assertEqual(self.request("POST", "a", ""), (200, b"OK\n"))
                # UTF-8 key and value are six bytes together, not two characters.
                self.assertEqual(self.request("POST", "中", "汉")[0], 503)
                self.assertEqual(self.request("POST", "b", ""), (200, b"OK\n"))
                self.assertEqual(self.request("POST", "中", "汉"), (200, b"OK\n"))
                self.assertEqual(self.request("POST", "n\0", "\0z"), (200, b"OK\n"))
                self.assertEqual(self.request("DELETE", "中"), (200, b"OK\n"))
                self.assertEqual(self.request("POST", "c", "12345"), (200, b"OK\n"))
                _, completed = self.runtime_stats()
                self.assertEqual(completed["engine"]["data_bytes"], 12)
                self.assertEqual(completed["engine"]["data_rejections_total"], 3)
                self.assertEqual(completed["engine"]["applied_sequence"], 8)
                self.wait_stats(lambda s: s["engine"]["async_requests_inflight"] == 0,
                                "capacity rejection leaked an async slot")
                self.stop("gateway")
                self.stop("engine")
                self.start_engine()
                self.start_gateway()
                _, recovered = self.runtime_stats()
                self.assertEqual((recovered["engine"]["data_bytes"], recovered["engine"]["data_capacity_bytes"]),
                                 (12, 12))
                self.assertEqual(recovered["engine"]["data_rejections_total"], 0)
                self.assertEqual(recovered["engine"]["durable_sequence"], 8)
                for key, value in (("a", b""), ("b", b""), ("n\0", b"\0z"), ("c", b"12345")):
                    self.assertEqual(self.request("GET", key), (200, b"VALUE " + value + b"\n"))
                self.assertEqual(self.request("GET", "中"), (404, b"NOT_FOUND\n"))

    def test_durable_confirmation_releases_worker_but_keeps_request_capacity(self):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_WORKERS": "1", "MINIKV_REQUEST_QUEUE_SIZE": "1",
                                "MINIKV_WAL_FLUSH_MS": "3000", "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        self.gateway_env.update({"MINIKV_RPC_POOL_SIZE": "1", "MINIKV_RPC_TIMEOUT_MS": "5000"})
        self.start_engine()
        self.start_gateway()
        results = []

        def write():
            try:
                results.append(self.request("POST", "waiting", "durable"))
            except Exception as error:
                results.append(error)

        writer = threading.Thread(target=write)
        writer.start()
        try:
            stats = self.wait_stats(
                lambda s: s["engine"]["wal_durable_waiters"] == 1 and s["server"]["workers_active"] == 0,
                "write did not release its worker while waiting for durability")
            self.assertEqual(stats["engine"]["durable_sequence"], 0)
            self.assertGreater(stats["engine"]["wal_pending_bytes"], 0)
            self.assertEqual(stats["engine"]["wal_durable_waiters"], 1)
            self.assertEqual(stats["engine"]["wal_durable_waits_total"], 0)
            self.assertEqual(stats["engine"]["wal_durable_wait_duration_ns_total"], 0)
            self.assertEqual(stats["engine"]["wal_capacity_waiters"], 0)
            self.assertEqual(stats["server"]["requests_inflight"], 1)
            self.assertEqual(stats["server"]["requests_capacity"], 2)
            self.assertEqual(stats["engine"]["async_requests_inflight"], 1)
            self.assertEqual(stats["engine"]["async_requests_capacity"], 2)
            self.assertEqual(stats["server"]["requests_started_total"], 1)
            first_queue_wait = stats["server"]["request_queue_wait_duration_ns_total"]
            self.assertEqual(stats["gateway"]["rpc"]["pool_in_use"], 1)
            self.assertEqual(stats["gateway"]["http"]["data"],
                             {"capacity": 1, "inflight": 1, "rejected_total": 0})
            self.assertEqual(self.request("GET", "waiting"),
                             (503, b"Gateway request capacity exhausted\n"))
            with self.rpc_socket() as waiting, self.rpc_socket() as rejected:
                waiting.sendall(frame(2, b"waiting"))
                stats = self.wait_stats(
                    lambda s: s["engine"]["wal_durable_waiters"] == 2 and s["server"]["workers_active"] == 0,
                    "the same worker did not submit the second durable wait")
                self.assertEqual(stats["server"]["request_queue_depth"], 0)
                self.assertEqual(stats["server"]["requests_inflight"], 2)
                self.assertEqual(stats["engine"]["async_requests_inflight"], 2)
                rejected.sendall(frame(2, b"waiting"))
                self.assertEqual(read_response(rejected)[0], 5)
                code, stats = self.runtime_stats()
                self.assertEqual(code, 200)
                self.assertGreaterEqual(stats["server"]["requests_rejected_total"], 1)
                self.assertEqual(stats["server"]["requests_started_total"], 2)
                self.assertGreaterEqual(stats["server"]["request_queue_wait_duration_ns_total"], first_queue_wait)
                self.assertEqual(stats["engine"]["durable_sequence"], 0)
                self.assertEqual(stats["gateway"]["rpc"]["calls_total"], 1)
                self.assertEqual(stats["gateway"]["http"]["data"]["rejected_total"], 1)
                self.assertEqual(results, [], "data write completed before Stats sampled its wait")
                waiting.settimeout(5)
                self.assertEqual(read_response(waiting), (1, b"durable"))
        finally:
            writer.join(timeout=6)
        self.assertFalse(writer.is_alive())
        self.assertEqual(results, [(200, b"OK\n")])
        stats = self.wait_stats(lambda s: s["server"]["requests_inflight"] == 0 and
                               s["engine"]["async_requests_inflight"] == 0, "completed requests did not release capacity")
        self.assertEqual(stats["engine"]["durable_sequence"], 1)
        self.assertEqual(stats["engine"]["wal_pending_bytes"], 0)
        self.assertEqual(stats["engine"]["wal_durable_waiters"], 0)
        self.assertEqual(stats["engine"]["wal_durable_waits_total"], 2)
        self.assertGreater(stats["engine"]["wal_durable_wait_duration_ns_total"], 0)
        self.assertEqual(stats["engine"]["async_requests_inflight"], 0)
        self.assertEqual(stats["server"]["requests_inflight"], 0)
        self.assertEqual(stats["server"]["requests_started_total"], 2)
        self.assertGreater(stats["server"]["request_queue_wait_duration_ns_total"], first_queue_wait)

    def test_stats_progress_while_wal_capacity_blocks_worker_and_rpc_pool(self):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_WORKERS": "1", "MINIKV_REQUEST_QUEUE_SIZE": "1",
                                "MINIKV_WAL_MODE": "throughput", "MINIKV_WAL_QUEUE_BYTES": str(2 * 1024 * 1024),
                                "MINIKV_WAL_FLUSH_MS": "3000", "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        self.gateway_env.update({"MINIKV_RPC_POOL_SIZE": "1", "MINIKV_RPC_TIMEOUT_MS": "5000"})
        self.start_engine()
        self.start_gateway()
        value = "x" * (1024 * 1024)
        # Two maximum values plus their WAL headers cannot fit in 2 MiB.
        self.assertEqual(self.request("POST", "first", value), (200, b"OK\n"))
        results = []

        def write():
            try:
                results.append(self.request("POST", "waiting", value))
            except Exception as error:
                results.append(error)

        writer = threading.Thread(target=write)
        writer.start()
        try:
            stats = self.wait_stats(lambda s: s["engine"]["wal_capacity_waiters"] == 1,
                                    "second write did not wait for WAL capacity")
            self.assertEqual(stats["engine"]["wal_durable_waiters"], 0)
            self.assertEqual(stats["engine"]["wal_capacity_waits_total"], 0)
            self.assertEqual(stats["server"]["workers_active"], 1)
            self.assertEqual(stats["gateway"]["rpc"]["pool_in_use"], 1)
            first_queue_wait = stats["server"]["request_queue_wait_duration_ns_total"]
            with self.rpc_socket() as queued, self.rpc_socket() as rejected:
                queued.sendall(frame(2, b"waiting"))
                stats = self.wait_stats(lambda s: s["server"]["request_queue_depth"] == 1,
                                        "read did not queue behind the capacity wait")
                self.assertEqual(stats["server"]["requests_inflight"], 2)
                rejected.sendall(frame(2, b"waiting"))
                self.assertEqual(read_response(rejected)[0], 5)
                code, stats = self.runtime_stats()
                self.assertEqual(code, 200)
                self.assertEqual(stats["server"]["requests_started_total"], 2)
                self.assertEqual(stats["server"]["request_queue_wait_duration_ns_total"], first_queue_wait)
                self.assertGreaterEqual(stats["server"]["requests_rejected_total"], 1)
                self.assertEqual(results, [])
                queued.settimeout(5)
                self.assertEqual(read_response(queued), (1, value.encode()))
        finally:
            writer.join(timeout=6)
        self.assertFalse(writer.is_alive())
        self.assertEqual(results, [(200, b"OK\n")])
        stats = self.wait_stats(lambda s: s["server"]["requests_inflight"] == 0,
                               "completed requests did not release capacity")
        self.assertEqual(stats["engine"]["applied_sequence"], 2)
        self.assertEqual(stats["engine"]["wal_capacity_waiters"], 0)
        self.assertEqual(stats["engine"]["wal_capacity_waits_total"], 1)
        self.assertGreater(stats["engine"]["wal_capacity_wait_duration_ns_total"], 0)
        self.assertEqual(stats["engine"]["wal_durable_waits_total"], 0)
        self.assertEqual(stats["server"]["requests_started_total"], 3)
        self.assertEqual(stats["server"]["requests_inflight"], 0)
        self.assertGreater(stats["server"]["request_queue_wait_duration_ns_total"], first_queue_wait)

    def test_disconnect_does_not_release_pending_request_capacity(self):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_WORKERS": "1", "MINIKV_REQUEST_QUEUE_SIZE": "1",
                                "MINIKV_CLIENT_IDLE_MS": "50", "MINIKV_WAL_FLUSH_MS": "3000",
                                "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        self.start_engine()
        self.start_gateway()
        value = b"v" * 65536
        with self.rpc_socket() as writer:
            writer.sendall(frame(1, b"retained", value))
            self.wait_stats(lambda s: s["engine"]["wal_durable_waiters"] == 1, "write was not admitted")
        with self.rpc_socket() as reader:
            reader.sendall(frame(2, b"retained"))
            self.wait_stats(lambda s: s["engine"]["wal_durable_waiters"] == 2, "read was not admitted")
        # Wait until the reactor has actually removed the old busy sockets.
        # Their captured replies must still consume request capacity.
        stats = self.wait_stats(lambda s: s["server"]["connections"] == 1, "old connections did not expire")
        self.assertEqual(stats["server"]["requests_inflight"], 2)
        for _ in range(12):
            with self.rpc_socket() as rejected:
                rejected.sendall(frame(2, b"retained"))
                self.assertEqual(read_response(rejected)[0], 5)
        code, stats = self.runtime_stats()
        self.assertEqual(code, 200)
        self.assertEqual(stats["engine"]["durable_sequence"], 0)
        self.assertEqual(stats["engine"]["async_requests_inflight"], 2)
        self.assertEqual(stats["server"]["requests_inflight"], 2)
        self.assertEqual(stats["server"]["requests_started_total"], 2)
        self.assertGreaterEqual(stats["server"]["requests_rejected_total"], 12)
        self.wait_stats(lambda s: s["server"]["requests_inflight"] == 0 and
                        s["engine"]["async_requests_inflight"] == 0, "discarded completions retained capacity", timeout=5)
        with self.rpc_socket() as recovered:
            recovered.sendall(frame(2, b"retained"))
            self.assertEqual(read_response(recovered), (1, value))

    def test_binary_values_snapshot_and_restart(self):
        key = "tenant:1 \n\x00"
        value = " \n中文\x00\tline1\nDEL victim\n "
        self.assertEqual(self.request("POST", "victim", "safe"), (200, b"OK\n"))
        self.assertEqual(self.request("POST", key, value), (200, b"OK\n"))
        self.assertEqual(self.request("POST", "empty", ""), (200, b"OK\n"))
        self.assertEqual(self.request("GET", "victim"), (200, b"VALUE safe\n"))
        self.assertEqual(self.request("GET", key), (200, b"VALUE " + value.encode() + b"\n"))
        wal = Path(self.engine_env["MINIKV_DATA_DIR"]) / "wal.v1"
        deadline = time.monotonic() + 3
        while wal.stat().st_size != 24 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(wal.read_bytes()[:8], b"MKVWAL02")
        self.assertEqual(wal.stat().st_size, 24, "periodic checkpoint did not run")
        self.assertEqual(self.request("POST", "after", "checkpoint"), (200, b"OK\n"))
        self.stop("gateway")
        self.stop("engine", kill=True)
        self.start_engine()
        self.start_gateway()
        self.assertEqual(self.request("GET", key), (200, b"VALUE " + value.encode() + b"\n"))
        self.assertEqual(self.request("GET", "empty"), (200, b"VALUE \n"))
        self.assertEqual(self.request("GET", "after"), (200, b"VALUE checkpoint\n"))

    def test_idle_and_partial_connections_do_not_occupy_workers(self):
        with contextlib.ExitStack() as stack:
            for index in range(40):
                sock = stack.enter_context(self.rpc_socket())
                if index % 2:
                    sock.sendall(b"MKV")
            with self.rpc_socket() as active:
                active.sendall(frame(2, b"missing"))
                self.assertEqual(read_response(active), (2, b""))
            self.assertEqual(self.request("POST", "live", "yes"), (200, b"OK\n"))

    def test_fragmentation_pipelining_and_request_order(self):
        key, value = b"tenant:1\x00", b" \nhello\x00\t "
        wire = frame(1, key, value) + frame(2, key) + frame(3, key) + frame(2, key)
        with self.rpc_socket() as sock:
            for offset in range(0, len(wire), 3):
                sock.sendall(wire[offset:offset + 3])
            self.assertEqual(read_response(sock), (0, b""))
            self.assertEqual(read_response(sock), (1, value))
            self.assertEqual(read_response(sock), (0, b""))
            self.assertEqual(read_response(sock), (2, b""))

    def test_large_values_and_client_reset(self):
        value = "x" * (1024 * 1024)
        self.assertEqual(self.request("POST", "large", value), (200, b"OK\n"))
        self.assertEqual(self.request("GET", "large"), (200, b"VALUE " + value.encode() + b"\n"))
        with self.rpc_socket() as sock:
            sock.sendall(frame(2, b"large"))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.assertEqual(self.request("GET", "missing"), (404, b"NOT_FOUND\n"))
        self.assertIsNone(self.engine.poll(), "client reset killed the engine")

    def test_half_closed_slow_reader_waits_without_spinning_and_drains_replies(self):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_WAL_MODE": "throughput", "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        self.start_engine()
        self.start_gateway()
        value = b"v" * (1024 * 1024)
        self.assertEqual(self.request("POST", "slow-reader", value.decode()), (200, b"OK\n"))
        with socket.socket() as sock:
            sock.settimeout(10)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
            sock.connect(("127.0.0.1", self.engine_port))
            sock.sendall(frame(2, b"slow-reader") * 8)
            sock.shutdown(socket.SHUT_WR)
            stats = self.wait_stats(lambda s: s["server"]["requests_started_total"] >= 3,
                                    "pipelined reads did not start")
            # Eight replies cannot fit the bounded receive window and server
            # send buffer. Wait for request progress to stop before measuring.
            deadline = time.monotonic() + 2
            while True:
                started = stats["server"]["requests_started_total"]
                self.assertLess(started, 9, "all replies fit without exercising send backpressure")
                time.sleep(0.05)
                code, stats = self.runtime_stats()
                self.assertEqual(code, 200)
                if stats["server"]["requests_started_total"] == started:
                    break
                self.assertLess(time.monotonic(), deadline, "reply output never became blocked")

            def engine_cpu_seconds():
                fields = Path(f"/proc/{self.engine.pid}/stat").read_text().rsplit(")", 1)[1].split()
                return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")

            cpu_before, wall_before = engine_cpu_seconds(), time.monotonic()
            time.sleep(0.6)
            elapsed = time.monotonic() - wall_before
            cpu_used = engine_cpu_seconds() - cpu_before
            self.assertLess(cpu_used, max(0.2, elapsed / 2),
                            "half-closed socket spun while waiting for the client to read")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
            for _ in range(8):
                self.assertEqual(read_response(sock), (1, value))
            self.assertEqual(sock.recv(1), b"", "half-close did not close after all replies")

    def test_accept_descriptor_exhaustion_backs_off_and_recovers(self):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_SNAPSHOT_INTERVAL_MS": "0", "MINIKV_MAX_CONNECTIONS": "128"})
        self.start_engine(nofile_limit=32)
        log = Path(self.directory.name) / "engine.log"
        with self.rpc_socket() as existing, contextlib.ExitStack() as stack:
            existing.sendall(frame(2, b"missing"))
            self.assertEqual(read_response(existing), (2, b""))
            for _ in range(60):
                stack.enter_context(self.rpc_socket())
            deadline = time.monotonic() + 2
            while b"Too many open files" not in log.read_bytes():
                self.assertLess(time.monotonic(), deadline, "descriptor exhaustion was not reached")
                time.sleep(0.01)
            size_before = log.stat().st_size
            time.sleep(0.5)
            self.assertLess(log.stat().st_size - size_before, 4096,
                            "descriptor exhaustion produced an unbounded accept error loop")
            existing.sendall(frame(2, b"missing"))
            self.assertEqual(read_response(existing), (2, b""), "accept backoff blocked an existing client")
            stack.close()
            with self.rpc_socket() as recovered:
                recovered.sendall(frame(2, b"missing"))
                self.assertEqual(read_response(recovered), (2, b""),
                                 "new connections did not recover after descriptors became available")
            # Shut down during a second exhaustion episode: the retry deadline
            # must not register a listener that begin_shutdown already closed.
            size_before = log.stat().st_size
            for _ in range(60):
                stack.enter_context(self.rpc_socket())
            deadline = time.monotonic() + 2
            while log.stat().st_size == size_before:
                self.assertLess(time.monotonic(), deadline, "second descriptor exhaustion was not reached")
                time.sleep(0.01)
            self.stop("engine")

    def test_invalid_frames_and_truncated_requests(self):
        bad = struct.pack("!4sB3xII", b"MKV1", 1, 4097, 0)
        with self.rpc_socket() as sock:
            sock.sendall(bad)
            self.assertEqual(read_response(sock)[0], 3)
        with self.rpc_socket() as sock:
            sock.sendall(frame(1, b"partial", b"unfinished")[:-3])
        self.assertEqual(self.request("GET", "partial"), (404, b"NOT_FOUND\n"))
        self.assertEqual(self.request("POST", body=b'{"key":"x"}{"key":"y"}')[0], 400)
        self.assertEqual(self.request("POST", "large", "x" * (1024 * 1024 + 1))[0], 413)

    def test_invalid_pipeline_preserves_earlier_reply_before_closing(self):
        value = b"v" * (1024 * 1024)
        self.assertEqual(self.request("POST", "protocol-large", value.decode()), (200, b"OK\n"))
        invalid = struct.pack("!4sB3xII", b"MKV1", 1, 4097, 0)
        for half_close in (False, True):
            with self.subTest(half_close=half_close), socket.socket() as sock:
                code, before = self.runtime_stats()
                self.assertEqual(code, 200)
                sock.settimeout(3)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
                sock.connect(("127.0.0.1", self.engine_port))
                # Keep more than one reactor read of input queued. The invalid
                # header must not cause close() to discard the earlier GET's
                # large response while unread bytes remain in the kernel.
                self.engine.send_signal(signal.SIGSTOP)
                try:
                    deadline = time.monotonic() + 2
                    while "State:\tT" not in Path(f"/proc/{self.engine.pid}/status").read_text():
                        self.assertLess(time.monotonic(), deadline, "reactor did not stop")
                        time.sleep(0.005)
                    sock.sendall(frame(2, b"protocol-large") + invalid +
                                 frame(1, b"protocol-unadmitted", b"bad") * 2000)
                    if half_close:
                        sock.shutdown(socket.SHUT_WR)
                finally:
                    self.engine.send_signal(signal.SIGCONT)
                self.wait_stats(lambda s: s["server"]["requests_started_total"] ==
                                before["server"]["requests_started_total"] + 1 and
                                s["server"]["requests_inflight"] == 0,
                                "GET did not reach the socket output buffer")
                time.sleep(0.05)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
                self.assertEqual(read_response(sock), (1, value))
                self.assertEqual(read_response(sock)[0], 3)
                self.assertEqual(sock.recv(1), b"", "invalid pipeline did not end after its error")
            self.assertEqual(self.request("GET", "protocol-unadmitted"), (404, b"NOT_FOUND\n"))
            self.assertIsNone(self.engine.poll(), "invalid pipeline stopped the engine")

    def test_acknowledged_writes_survive_kill(self):
        acknowledged = {}
        finished = threading.Event()
        errors = []

        def writer():
            try:
                for index in range(1000):
                    key, value = f"kill-{index}", f"value:{index}\n"
                    status, _ = self.request("POST", key, value)
                    if status == 200:
                        acknowledged[key] = value
                    if finished.is_set() or status != 200:
                        return
            except (OSError, http.client.HTTPException) as error:
                if not finished.is_set():
                    errors.append(error)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            deadline = time.monotonic() + 5
            while len(acknowledged) < 40 and thread.is_alive() and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertGreaterEqual(len(acknowledged), 40)
            finished.set()
            self.stop("engine", kill=True)
        finally:
            finished.set()
            thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.stop("gateway")
        self.start_engine()
        self.start_gateway()
        for key, value in acknowledged.items():
            self.assertEqual(self.request("GET", key), (200, b"VALUE " + value.encode() + b"\n"))

    def test_concurrent_history_and_recovery(self):
        # Each case has an empty dataset and no global data-capacity constraint,
        # so the checker may partition the completed history by key. Failed or
        # timed-out calls are test failures, never silently omitted operations.
        for snapshot_ms in (0, 1):
            for seed in (7, 41, 2026):
                with self.subTest(snapshot_ms=snapshot_ms, seed=seed):
                    self.stop("gateway")
                    self.stop("engine")
                    self.engine_env.update({
                        "MINIKV_DATA_DIR": str(Path(self.directory.name) /
                                               f"history-{snapshot_ms}-{seed}"),
                        "MINIKV_WAL_MODE": "reliable",
                        "MINIKV_MAX_DATA_BYTES": "0",
                        "MINIKV_WAL_FLUSH_MS": "5",
                        "MINIKV_SNAPSHOT_INTERVAL_MS": str(snapshot_ms),
                    })
                    self.start_engine()
                    self.start_gateway()
                    before = self.runtime_stats()[1]["engine"]
                    keys = ("hot", "empty", "unicode-键")
                    workers, steps = 4, 10
                    history, errors = [], []
                    lock = threading.Lock()
                    start = threading.Barrier(workers)
                    stop_clients = threading.Event()

                    def client(worker):
                        rng = random.Random(seed * workers + worker)
                        try:
                            start.wait(timeout=5)
                            for step in range(steps):
                                if stop_clients.is_set():
                                    return
                                # The first three operations contend on one key;
                                # the rest mix independent and conflicting keys.
                                key = keys[0] if step < 3 else rng.choice(keys)
                                method = ("PUT", "GET", "DELETE")[step] if step < 3 else rng.choice(
                                    ("PUT", "GET", "DELETE"))
                                value = None
                                if method == "PUT":
                                    value = "" if (worker + step) % 4 == 0 else f"{seed}:{worker}:{step}\x00\n值"
                                started = time.monotonic_ns()
                                status, body = self.request("POST" if method == "PUT" else method, key, value)
                                finished = time.monotonic_ns()
                                operation = HistoryOperation(worker * steps + step, method, key, value,
                                                             started, finished, status, body)
                                with lock:
                                    history.append(operation)
                        except Exception as error:
                            with lock:
                                errors.append((worker, repr(error)))
                            stop_clients.set()
                            start.abort()

                    threads = [threading.Thread(target=client, args=(worker,)) for worker in range(workers)]
                    try:
                        for thread in threads:
                            thread.start()
                        deadline = time.monotonic() + 20
                        for thread in threads:
                            thread.join(timeout=max(0, deadline - time.monotonic()))
                    finally:
                        stop_clients.set()
                        start.abort()
                        for thread in threads:
                            if thread.ident is not None:
                                thread.join(timeout=5)
                    self.assertFalse(any(thread.is_alive() for thread in threads), "history client did not finish")
                    self.assertEqual(errors, [], f"client errors; completed history: {history!r}")
                    self.assertEqual(len(history), workers * steps)
                    history.sort(key=lambda operation: operation.id)
                    self.assertTrue(any(left.key == right.key and left.started < right.finished and
                                        right.started < left.finished
                                        for index, left in enumerate(history) for right in history[index + 1:]),
                                    f"no overlapping calls on the same key; history={history!r}")

                    def verify():
                        try:
                            witness = check_history(history)
                        except Exception as error:
                            self.fail(f"history check failed: {error}; snapshot_ms={snapshot_ms}, "
                                      f"seed={seed}; history={history!r}")
                        self.assertEqual(set(witness), set(keys))
                        self.assertEqual(sorted(identifier for order in witness.values() for identifier in order),
                                         list(range(len(history))))

                    # Check the online observations before adding recovery reads,
                    # then solve again with the latter constrained after every
                    # acknowledged operation. A different valid witness is fine.
                    verify()
                    if snapshot_ms:
                        self.wait_stats(lambda stats: stats["engine"]["snapshot_successes_total"] >
                                        before["snapshot_successes_total"] and
                                        stats["engine"]["snapshot_sequence"] > 0,
                                        "automatic snapshot did not checkpoint any workload operations")
                    process = self.engine
                    self.assertIsNone(process.poll())
                    self.stop("engine", kill=True)
                    self.assertEqual(process.returncode, -signal.SIGKILL)
                    self.stop("gateway")
                    self.start_engine()
                    self.start_gateway()
                    for key in keys:
                        started = time.monotonic_ns()
                        status, body = self.request("GET", key)
                        history.append(HistoryOperation(len(history), "GET", key, None, started,
                                                        time.monotonic_ns(), status, body))
                    verify()

    def test_bounded_queue_reports_overload(self):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_WORKERS": "1", "MINIKV_REQUEST_QUEUE_SIZE": "1",
                                "MINIKV_WAL_FLUSH_MS": "300", "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        self.start_engine()
        with contextlib.ExitStack() as stack:
            clients = [stack.enter_context(self.rpc_socket()) for _ in range(12)]
            for index, sock in enumerate(clients):
                sock.sendall(frame(1, f"load-{index}".encode(), b"value"))
            statuses = [read_response(sock)[0] for sock in clients]
            self.assertIn(5, statuses, "overload was not reported")
            self.assertTrue(all(status in (0, 5) for status in statuses), statuses)

    def test_shutdown_delivers_admitted_responses(self):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_WORKERS": "1", "MINIKV_REQUEST_QUEUE_SIZE": "1",
                                "MINIKV_WAL_FLUSH_MS": "1000", "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        self.start_engine()
        with contextlib.ExitStack() as stack:
            clients = [stack.enter_context(self.rpc_socket()) for _ in range(8)]
            for index, sock in enumerate(clients):
                sock.sendall(frame(1, f"shutdown-{index}".encode(), b"value"))
            readable, _, _ = select.select(clients, [], [], 2)
            self.assertTrue(readable, "no admission/overload response")
            # Exhausted request capacity proves a request was admitted. Its
            # reliable-mode acknowledgement is still waiting on the flush.
            gate = readable[0]
            self.assertEqual(read_response(gate)[0], 5)
            self.engine.terminate()
            acknowledged = 0
            for sock in clients:
                if sock is gate:
                    continue
                try:
                    status, _ = read_response(sock)
                    acknowledged += status == 0
                except (EOFError, ConnectionResetError):
                    pass  # A request that was not admitted may be disconnected.
            self.assertGreater(acknowledged, 0, "shutdown discarded all admitted responses")
        self.stop("engine")

    def test_shutdown_drains_pipelined_input_before_closing_slow_response(self):
        for half_close, queued_reply in ((False, False), (True, False), (False, True)):
            with self.subTest(half_close=half_close, queued_reply=queued_reply):
                self.check_shutdown_pipelined_response(half_close, queued_reply)

    def check_shutdown_pipelined_response(self, half_close, queued_reply):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_WAL_MODE": "throughput", "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        self.start_engine()
        self.start_gateway()
        value = b"v" * (1024 * 1024)
        self.assertEqual(self.request("POST", "shutdown-large", value.decode()), (200, b"OK\n"))
        with socket.socket() as sock:
            sock.settimeout(3)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
            sock.connect(("127.0.0.1", self.engine_port))
            if queued_reply:
                sock.sendall(frame(2, b"shutdown-large"))
                stats = self.wait_stats(lambda s: s["server"]["requests_started_total"] == 2 and
                                        s["server"]["requests_inflight"] == 0,
                                        "single reply did not reach the socket output buffer")
                # Freeze an idle reactor before putting more input in the kernel,
                # so shutdown must preserve a reply already handed to send().
                self.engine.send_signal(signal.SIGSTOP)
                try:
                    deadline = time.monotonic() + 2
                    while "State:\tT" not in Path(f"/proc/{self.engine.pid}/status").read_text():
                        self.assertLess(time.monotonic(), deadline, "reactor did not stop")
                        time.sleep(0.005)
                    sock.sendall(frame(2, b"shutdown-large") * 700 + frame(1, b"shutdown-unadmitted", b"bad"))
                    self.engine.terminate()
                finally:
                    self.engine.send_signal(signal.SIGCONT)
                admitted_reads = 1
                applied_sequence = stats["engine"]["applied_sequence"]
            else:
                admitted_reads, applied_sequence = self.stop_with_pipelined_response(sock, half_close)
            time.sleep(0.05)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
            for _ in range(admitted_reads):
                self.assertEqual(read_response(sock), (1, value))
            self.assertEqual(sock.recv(1), b"", "shutdown submitted another pipelined request")
            # A pooled RPC connection need not close its write side after reading
            # a reply. Acknowledged output must allow prompt server shutdown.
            self.engine.wait(timeout=2)
        self.stop("engine")
        self.start_engine()
        with self.rpc_socket() as recovered:
            recovered.sendall(frame(2, b"shutdown-unadmitted") + frame(4, b""))
            self.assertEqual(read_response(recovered), (2, b""))
            self.assertEqual(json.loads(read_response(recovered)[1])["engine"]["applied_sequence"], applied_sequence)

    def stop_with_pipelined_response(self, sock, half_close):
        # Leave unadmitted requests in both user and kernel receive buffers.
        # The terminal PUT must never execute during shutdown.
        sock.sendall(frame(2, b"shutdown-large") * 700 + frame(1, b"shutdown-unadmitted", b"bad"))
        if half_close:
            sock.shutdown(socket.SHUT_WR)
        stats = self.wait_stats(lambda s: s["server"]["requests_started_total"] >= 3,
                                "pipelined reads did not start")
        deadline = time.monotonic() + 2
        while True:
            started = stats["server"]["requests_started_total"]
            self.assertLess(started, 701, "responses did not exercise send backpressure")
            time.sleep(0.05)
            code, stats = self.runtime_stats()
            self.assertEqual(code, 200)
            if stats["server"]["requests_started_total"] == started:
                break
            self.assertLess(time.monotonic(), deadline, "reply output never became blocked")
        admitted_reads = started - 1  # The first request populated the value.
        applied_sequence = stats["engine"]["applied_sequence"]
        self.engine.terminate()
        return admitted_reads, applied_sequence

    def test_shutdown_drains_async_replies_after_network_grace_expires(self):
        self.stop("gateway")
        self.stop("engine")
        self.engine_env.update({"MINIKV_WAL_FLUSH_MS": "6000", "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        self.start_engine()
        self.start_gateway()
        with self.rpc_socket() as sock:
            sock.sendall(frame(1, b"late-shutdown", b"recoverable"))
            self.wait_stats(lambda s: s["engine"]["wal_durable_waiters"] == 1, "write was not admitted")
            self.stop("gateway")
            self.engine.terminate()
            sock.settimeout(8)
            # The network grace ends before the six-second WAL timer. The
            # completion target must remain alive after the socket is gone.
            with self.assertRaises((EOFError, ConnectionResetError)):
                read_response(sock)
            self.engine.wait(timeout=5)
        self.stop("engine")
        self.start_engine()
        self.start_gateway()
        self.assertEqual(self.request("GET", "late-shutdown"), (200, b"VALUE recoverable\n"))

    def test_mixed_benchmark_has_no_system_failures(self):
        result = subprocess.run([
            str(BENCH), "-url", f"http://127.0.0.1:{self.http_port}/kv",
            "-workers=8", "-requests=200", "-op=mixed", "-keyspace=64",
            "-preload-count=64", "-seed=1", "-value-size=128",
        ], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("100.00%", result.stdout)
        self.assertIn("P99.9", result.stdout)
        self.assertIn("成功吞吐量", result.stdout)

    def test_json_benchmark_matches_storage_operations(self):
        code, before = self.runtime_stats()
        self.assertEqual(code, 200)
        result = subprocess.run([
            str(BENCH), "-url", f"http://127.0.0.1:{self.http_port}/kv",
            "-workers=8", "-requests=257", "-op=mixed", "-keyspace=23",
            "-preload-count=0", "-write-ratio=35", "-delete-ratio=15",
            "-seed=-19", "-value-size=128", "-format=json",
        ], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["schema_version"], 1)
        self.assertTrue(report["complete"])
        self.assertEqual(report["load_model"], "closed_loop")
        self.assertEqual(report["workload_generator"], "indexed-pcg-v1")
        self.assertEqual(report["config"]["preload_count"], 0)
        self.assertEqual(report["preload"]["target_keys"], 23)
        self.assertEqual(report["preload"]["completed_keys"], 23)
        self.assertIn("预热", result.stderr)
        outcomes = report["outcomes"]
        self.assertEqual(outcomes["requests"], 257)
        self.assertEqual(outcomes["failures"], 0)
        self.assertEqual(outcomes["successes"] + outcomes["logical_misses"], 257)
        self.assertEqual(sum(report["operations"].values()), 257)
        self.assertEqual(sum(report["http_statuses"].values()), 257)
        self.assertEqual(report["latency_ns"]["samples"], 257)
        code, after = self.runtime_stats()
        self.assertEqual(code, 200)
        # Independently reconcile the client report with actual engine sequence
        # advancement: every PUT/DELETE (including misses) adds exactly one LSN.
        writes = report["operations"]["put"] + report["operations"]["delete"]
        self.assertEqual(after["engine"]["applied_sequence"] - before["engine"]["applied_sequence"], 23 + writes)
        self.assertEqual(after["engine"]["durable_sequence"], after["engine"]["applied_sequence"])
        self.assertEqual(after["gateway"]["rpc"]["calls_total"] - before["gateway"]["rpc"]["calls_total"], 23 + 257)


class MiniKVStartup(unittest.TestCase):
    @staticmethod
    def environment(directory):
        env = {key: value for key, value in os.environ.items() if not key.startswith("MINIKV_")}
        env.update({"MINIKV_DATA_DIR": str(directory), "MINIKV_SNAPSHOT_INTERVAL_MS": "0"})
        return env

    @staticmethod
    def legacy_binary_files(directory):
        directory.mkdir()
        header = struct.pack("!8sQQ", b"MKVSNP01", 0, 0)
        record = struct.pack("!4sBQII", b"MKL1", 1, 1, 4, 5) + b"keptvalue"
        (directory / "snapshot.v1").write_bytes(header + struct.pack("!I", zlib.crc32(header)))
        (directory / "wal.v1").write_bytes(record + struct.pack("!I", zlib.crc32(record)))

    def test_invalid_http_admission_configuration_fails_before_listening(self):
        for value in ("0", "-1", "65537", "invalid"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as temporary:
                env = self.environment(Path(temporary) / "data")
                env.update({"MINIKV_HTTP_MAX_INFLIGHT": value, "MINIKV_HTTP_ADDR": "127.0.0.1:0"})
                result = subprocess.run([str(GATEWAY)], env=env, capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("invalid MINIKV_HTTP_MAX_INFLIGHT", result.stderr)
                self.assertNotIn("listening", result.stdout + result.stderr)

    def test_data_capacity_configuration_validation(self):
        invalid = ("", "-1", "+1", " 1", "1 ", "1.0", "18446744073709551616", "invalid")
        for value in invalid:
            for legacy in (False, True):
                with self.subTest(value=value, legacy=legacy), tempfile.TemporaryDirectory() as temporary:
                    directory = Path(temporary) / "data"
                    if legacy:
                        self.legacy_binary_files(directory)
                    before = {path.name: path.read_bytes() for path in directory.glob("*")}
                    env = self.environment(directory)
                    env["MINIKV_MAX_DATA_BYTES"] = value
                    for args in ([], ["--import-legacy"]):
                        result = subprocess.run([str(ENGINE)] + args, env=env,
                                                capture_output=True, text=True, timeout=5)
                        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                        self.assertIn("MINIKV_MAX_DATA_BYTES", result.stderr)
                        self.assertEqual(directory.exists(), legacy)
                        self.assertEqual({path.name: path.read_bytes() for path in directory.glob("*")}, before)
        for value in ("0", "1", "18446744073709551615"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as temporary:
                env = self.environment(Path(temporary) / "data")
                env["MINIKV_MAX_DATA_BYTES"] = value
                result = subprocess.run([str(ENGINE), "--import-legacy"], env=env,
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_invalid_server_configuration_does_not_open_storage(self):
        invalid = {
            "MINIKV_WORKERS": "0",
            "MINIKV_REQUEST_QUEUE_SIZE": "0",
            "MINIKV_MAX_CONNECTIONS": "0",
            "MINIKV_CLIENT_IDLE_MS": "0",
            "MINIKV_ENGINE_PORT": "0",
            "MINIKV_ENGINE_HOST": "localhost",
        }
        for name, value in invalid.items():
            for legacy in (False, True):
                with self.subTest(variable=name, legacy=legacy), tempfile.TemporaryDirectory() as temporary:
                    directory = Path(temporary) / "data"
                    if legacy:
                        self.legacy_binary_files(directory)
                    before = {path.name: path.read_bytes() for path in directory.glob("*")}
                    env = self.environment(directory)
                    env[name] = value
                    result = subprocess.run([str(ENGINE)], env=env, capture_output=True, text=True, timeout=5)
                    self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                    self.assertIn(name, result.stderr)
                    self.assertEqual(directory.exists(), legacy, "invalid configuration created storage")
                    self.assertEqual({path.name: path.read_bytes() for path in directory.glob("*")}, before,
                                     "invalid configuration opened or upgraded storage")

    def test_import_ignores_server_only_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "data"
            directory.mkdir()
            originals = {"data.db": b"kept:value\n", "wal.log": b"PUT added imported\n"}
            for name, value in originals.items():
                (directory / name).write_bytes(value)
            env = self.environment(directory)
            for name in ("WORKERS", "REQUEST_QUEUE_SIZE", "MAX_CONNECTIONS", "CLIENT_IDLE_MS",
                         "ENGINE_PORT", "ENGINE_HOST"):
                env["MINIKV_" + name] = "invalid"
            result = subprocess.run([str(ENGINE), "--import-legacy"], env=env,
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Legacy import complete", result.stdout)
            self.assertNotIn("listening", result.stdout)
            self.assertEqual({name: (directory / name).read_bytes() for name in originals}, originals)
            self.assertEqual((directory / "wal.v1").read_bytes()[:8], b"MKVWAL02")
            snapshot = (directory / "snapshot.v1").read_bytes()
            self.assertEqual(snapshot[:8], b"MKVSNP01")
            self.assertEqual(struct.unpack("!QQ", snapshot[8:24]), (1, 2))


if __name__ == "__main__":
    unittest.main()
