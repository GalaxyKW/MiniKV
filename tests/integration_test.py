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
            # A full queue proves at least one request was admitted. Its
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


if __name__ == "__main__":
    unittest.main()
