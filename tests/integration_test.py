#!/usr/bin/env python3
"""Exercise the built C++ engine and Go gateway against temporary local data."""

import contextlib
import http.client
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from urllib.parse import urlencode


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
        })
        self.start_engine()
        self.start_gateway()

    def launch(self, binary, env, port, name):
        log_path = Path(self.directory.name) / f"{name}.log"
        log = log_path.open("ab")
        self.log_files.append(log)
        process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log, cwd=ROOT)
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

    def start_engine(self):
        self.launch(ENGINE, self.engine_env, self.engine_port, "engine")

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

    def test_runtime_stats_and_recovery(self):
        self.assertEqual(self.request("POST", "private-key", "private-value"), (200, b"OK\n"))
        self.assertEqual(self.request("GET", "private-key"), (200, b"VALUE private-value\n"))
        stats = self.wait_stats(lambda s: s["server"]["requests_inflight"] == 0 and
                               s["engine"]["async_requests_inflight"] == 0, "completed requests did not release capacity")
        self.assertEqual(stats["schema_version"], 1)
        self.assertEqual(stats["engine"]["keys"], 1)
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
        self.assertFalse(stats["engine"]["io_failed"])
        self.assertEqual(stats["server"]["workers_capacity"], 2)
        self.assertEqual(stats["server"]["requests_inflight"], 0)
        self.assertEqual(stats["server"]["requests_capacity"], 130)
        self.assertEqual(stats["server"]["requests_started_total"], 2)
        self.assertIsInstance(stats["server"]["request_queue_wait_duration_ns_total"], int)
        self.assertEqual(stats["gateway"]["rpc"]["calls_total"], 2)
        self.assertEqual(stats["gateway"]["rpc"]["errors_total"], 0)
        self.assertEqual(stats["gateway"]["rpc"]["pool_capacity"], 16)
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
        self.start_engine()
        code, recovered = self.runtime_stats()
        self.assertEqual(code, 200)
        self.assertEqual(recovered["engine"]["keys"], 1)
        self.assertEqual(recovered["engine"]["durable_sequence"], 1)
        self.assertEqual(recovered["engine"]["wal_commits_total"], 0)
        for field in ("wal_capacity_waiters", "wal_capacity_waits_total", "wal_capacity_wait_duration_ns_total",
                      "wal_durable_waiters", "wal_durable_waits_total", "wal_durable_wait_duration_ns_total"):
            self.assertEqual(recovered["engine"][field], 0)
        self.assertEqual(recovered["server"]["requests_started_total"], 0)
        self.assertEqual(recovered["server"]["request_queue_wait_duration_ns_total"], 0)
        self.assertEqual(recovered["gateway"]["rpc"]["calls_total"], 2)

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
        while wal.stat().st_size and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(wal.stat().st_size, 0, "periodic checkpoint did not run")
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


if __name__ == "__main__":
    unittest.main()
