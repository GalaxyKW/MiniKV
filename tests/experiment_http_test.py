#!/usr/bin/env python3
"""Check bounded experiment status reads against real loopback HTTP responses."""

import contextlib
import http.server
import json
from pathlib import Path
import sys
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benmark"))
import experiment


STATS = {
    "schema_version": 1,
    "engine": {"io_failed": False, "stopping": False, "wal_mode": "reliable",
               "keys": 0, "applied_sequence": 0, "durable_sequence": 0,
               "wal_pending_bytes": 0, "snapshot_successes_total": 1, "snapshot_failures_total": 0},
    "server": {"workers_capacity": 4}, "gateway": {"rpc": {"pool_capacity": 32}},
}
BODY = json.dumps(STATS).encode()
MAX_RESPONSE = 1024 * 1024


@contextlib.contextmanager
def stats_server(kind="normal", body=BODY):
    stopping = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            # End every fixture connection after one request. A failed status
            # query must not leave a test thread waiting for another request.
            self.close_connection = True
            try:
                self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                if kind == "slow_header":
                    self.wfile.write(b"X-Slow: ")
                    for _ in range(40):
                        if stopping.is_set():
                            return
                        self.wfile.write(b"x")
                        self.wfile.flush()
                        if stopping.wait(.01):
                            return
                    self.wfile.write(b"\r\n")
                length = len(body) + (10 if kind == "truncated" else 0)
                self.wfile.write(f"Content-Length: {length}\r\nConnection: close\r\n\r\n".encode())
                if kind == "slow_body":
                    for position in range(0, len(body), 8):
                        if stopping.is_set():
                            return
                        self.wfile.write(body[position:position + 8])
                        self.wfile.flush()
                        if stopping.wait(.01):
                            return
                else:
                    self.wfile.write(body)
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                # A deadline/size failure intentionally closes its own socket.
                pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .005})
    thread.start()
    try:
        yield server.server_port
    finally:
        stopping.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)
        if thread.is_alive():
            raise RuntimeError("status fixture server did not stop")


class StatusHTTPTests(unittest.TestCase):
    def test_fast_complete_response(self):
        with stats_server() as port:
            sample = experiment.fetch_stats(port, timeout=.5)
        self.assertIsNone(sample["error"], sample)
        self.assertEqual(sample["http_status"], 200)
        self.assertEqual(sample["body"], STATS)
        self.assertGreaterEqual(sample["monotonic_end_ns"], sample["monotonic_start_ns"])
        self.assertTrue(sample["started_at"])
        self.assertTrue(sample["finished_at"])

    def check_trickle_deadline(self, kind, expected_status):
        with stats_server(kind) as port:
            started = time.monotonic()
            sample = experiment.fetch_stats(port, timeout=.08)
            elapsed = time.monotonic() - started
        self.assertIsNotNone(sample["error"], "continued socket progress escaped the overall deadline")
        self.assertEqual(sample["http_status"], expected_status)
        self.assertIsNone(sample["body"])
        # Allow substantial scheduling slack; each complete fixture response
        # requires at least 400 ms when only individual reads are bounded.
        self.assertLess(elapsed, .35, sample)
        self.assertGreater(sample["monotonic_end_ns"], sample["monotonic_start_ns"])

    def test_slow_header_progress_does_not_reset_deadline(self):
        self.check_trickle_deadline("slow_header", None)

    def test_slow_body_progress_does_not_reset_deadline(self):
        self.check_trickle_deadline("slow_body", 200)

    def test_exact_size_limit_accepts_complete_json(self):
        body = BODY + b" " * (MAX_RESPONSE - len(BODY))
        with stats_server(body=body) as port:
            sample = experiment.fetch_stats(port, timeout=.5)
        self.assertIsNone(sample["error"], sample)
        self.assertEqual(sample["body"], STATS)

    def test_oversize_response_is_rejected(self):
        with stats_server(body=b"x" * (MAX_RESPONSE + 1)) as port:
            sample = experiment.fetch_stats(port, timeout=.5)
        self.assertEqual(sample["http_status"], 200)
        self.assertIsNone(sample["body"])
        self.assertIn("exceeds", sample["error"] or "")

    def test_truncated_content_length_is_not_valid_stats(self):
        # The transmitted prefix is itself valid JSON, so JSON decoding alone
        # cannot detect that the declared HTTP body was not completely received.
        with stats_server("truncated") as port:
            sample = experiment.fetch_stats(port, timeout=.5)
        self.assertEqual(sample["http_status"], 200)
        self.assertIsNotNone(sample["error"], sample)
        self.assertIsNone(sample["body"])


if __name__ == "__main__":
    unittest.main()
