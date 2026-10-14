#!/usr/bin/env python3
"""Check existing storage through the offline CLI without changing any file."""

import fcntl
import json
import os
from pathlib import Path
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import zlib


ROOT = Path(__file__).resolve().parents[1]
ENGINE = Path(os.environ.get("MINIKV_TEST_ENGINE", ROOT / "build" / "engine")).resolve()
PRIVATE_KEY = b"private-key-marker"
PRIVATE_VALUE = b"private-value-marker\x00binary"


def with_crc(data):
    return data + struct.pack("!I", zlib.crc32(data))


def record(sequence, key, value=b"", operation=1, version=2):
    # Independently encode the documented format instead of calling the codec
    # under test or constructing Engine, which would repair or upgrade fixtures.
    header = struct.pack("!4sBQII", b"MKL2" if version == 2 else b"MKL1",
                         operation, sequence, len(key), len(value))
    if version == 2:
        header = with_crc(header)
    return with_crc(header + key + value)


def snapshot(sequence, entries=()):
    header = with_crc(struct.pack("!8sQQ", b"MKVSNP01", sequence, len(entries)))
    return header + b"".join(record(sequence, key, value, version=1) for key, value in entries)


def wal_header():
    return with_crc(struct.pack("!8sIQ", b"MKVWAL02", 0, 0))


def inventory(directory):
    """Capture replacement, rewrite and cleanup effects, without following links.

    atime is intentionally omitted: read-only access can legitimately update it.
    mtime/ctime and inode catch a rewrite or replacement with identical bytes.
    """
    result = {}

    def visit(path):
        info = path.lstat()
        metadata = (info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
                    info.st_uid, info.st_gid, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        if stat.S_ISREG(info.st_mode):
            contents = path.read_bytes()
        elif stat.S_ISLNK(info.st_mode):
            contents = os.readlink(path)
        else:
            contents = None
        result[str(path.relative_to(directory))] = (metadata, contents)
        if stat.S_ISDIR(info.st_mode):
            for child in sorted(path.iterdir()):
                visit(child)

    if directory.exists():
        visit(directory)
    return result


class StorageCheck(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="minikv-check-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.directory = self.root / "data"

    def environment(self, **overrides):
        environment = {key: value for key, value in os.environ.items() if not key.startswith("MINIKV_")}
        environment["MINIKV_DATA_DIR"] = str(self.directory)
        environment.update(overrides)
        return environment

    def fixture(self, snapshot_bytes=None, wal_bytes=None):
        self.directory.mkdir(exist_ok=True)
        files = {"LOCK": b"existing lock\n",
                 "snapshot.v1": snapshot(0) if snapshot_bytes is None else snapshot_bytes,
                 "wal.v1": wal_header() if wal_bytes is None else wal_bytes,
                 "snapshot.v1.tmp": b"unfinished snapshot must remain\n",
                 "wal.v1.tmp": b"unfinished WAL must remain\n"}
        for name, data in files.items():
            (self.directory / name).write_bytes(data)

    def check(self, expected_code=0, *, environment=None, args=("--check",), **process_options):
        before = inventory(self.root)
        try:
            result = subprocess.run([str(ENGINE), *args],
                                    env=self.environment() if environment is None else environment,
                                    cwd=ROOT, capture_output=True, text=True, timeout=5,
                                    **process_options)
        finally:
            self.assertEqual(inventory(self.root), before, "offline check changed storage or directory entries")
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, expected_code, output)
        for secret in (PRIVATE_KEY.decode(), "private-value-marker"):
            self.assertNotIn(secret, output, "offline report exposed user data")
        if expected_code == 1:
            self.assertEqual(result.stdout, "", "failed check emitted a success report")
            self.assertTrue(result.stderr, "failed check had no diagnostic")
            return result
        self.assertEqual(result.stderr, "", result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(set(report), {
            "schema_version", "status", "keys", "data_bytes", "data_capacity_bytes",
            "snapshot_sequence", "applied_sequence", "wal_format", "wal_valid_bytes", "wal_trailing_bytes",
        })
        for name in set(report) - {"status", "wal_format"}:
            self.assertIs(type(report[name]), int, name)
            self.assertGreaterEqual(report[name], 0, name)
        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(report["status"], "valid" if expected_code == 0 else "recoverable_tail")
        return report

    def assert_report(self, report, *, image, sequence, checkpoint=0, version=2,
                      valid_bytes, trailing_bytes=0, capacity=0):
        self.assertEqual(report, {
            "schema_version": 1,
            "status": "recoverable_tail" if trailing_bytes else "valid",
            "keys": len(image),
            "data_bytes": sum(len(key) + len(value) for key, value in image.items()),
            "data_capacity_bytes": capacity,
            "snapshot_sequence": checkpoint,
            "applied_sequence": sequence,
            "wal_format": "v2" if version == 2 else "v1",
            "wal_valid_bytes": valid_bytes,
            "wal_trailing_bytes": trailing_bytes,
        })

    def test_current_and_legacy_wal_replay_without_mutation(self):
        for version in (2, 1):
            with self.subTest(version=version):
                entries = [(PRIVATE_KEY, b"before"), (b"deleted", b"before")]
                wal = wal_header() if version == 2 else b""
                wal += record(1, b"covered", b"old", version=version)
                wal += record(2, b"covered", operation=3, version=version)
                wal += record(3, b"added", b"a\x00b", version=version)
                wal += record(4, b"deleted", operation=3, version=version)
                wal += record(5, PRIVATE_KEY, PRIVATE_VALUE, version=version)
                self.fixture(snapshot(2, entries), wal)
                self.assert_report(self.check(), image={PRIVATE_KEY: PRIVATE_VALUE, b"added": b"a\x00b"},
                                   sequence=5, checkpoint=2, version=version, valid_bytes=len(wal))

    def test_empty_and_checkpointed_wal_remain_unchanged(self):
        for version in (2, 1):
            for covered in (False, True):
                with self.subTest(version=version, covered=covered):
                    wal = wal_header() if version == 2 else b""
                    if covered:
                        wal += record(1, b"obsolete", b"ignored", version=version)
                    self.fixture(snapshot(2, [(PRIVATE_KEY, PRIVATE_VALUE)]), wal)
                    self.assert_report(self.check(), image={PRIVATE_KEY: PRIVATE_VALUE}, sequence=2,
                                       checkpoint=2, version=version, valid_bytes=len(wal))

    def test_torn_v2_record_reports_every_boundary_without_repair(self):
        first = record(1, PRIVATE_KEY, PRIVATE_VALUE)
        following = record(2, b"unfinished", b"not applied")
        for has_prefix in (False, True):
            torn = following if has_prefix else first
            prefix = wal_header() + (first if has_prefix else b"")
            # Include every header, payload and final checksum boundary. The
            # zero-byte boundary is healthy, not a reported incomplete record.
            for cut in range(1, len(torn)):
                with self.subTest(has_prefix=has_prefix, cut=cut):
                    self.fixture(wal_bytes=prefix + torn[:cut])
                    self.assert_report(self.check(2), image={PRIVATE_KEY: PRIVATE_VALUE} if has_prefix else {},
                                       sequence=int(has_prefix), valid_bytes=len(prefix), trailing_bytes=cut)
        # Ordinary recovery compacts even valid records covered by the snapshot.
        # Inspection must retain the input boundary and the unfinished suffix.
        prefix = wal_header() + first
        self.fixture(snapshot(1, [(PRIVATE_KEY, PRIVATE_VALUE)]), prefix + following[:-1])
        self.assert_report(self.check(2), image={PRIVATE_KEY: PRIVATE_VALUE}, sequence=1, checkpoint=1,
                           valid_bytes=len(prefix), trailing_bytes=len(following) - 1)

    def test_corruption_is_rejected_instead_of_reported_as_tail(self):
        good_record = record(1, PRIVATE_KEY, PRIVATE_VALUE)
        good_wal = wal_header() + good_record
        cases = {}
        for name, offset in (("file header CRC", 20), ("record magic", 24),
                             ("protected record length", 24 + 17), ("record body CRC", len(good_wal) - 1)):
            corrupt = bytearray(good_wal)
            corrupt[offset] ^= 1
            cases[name] = (snapshot(0), bytes(corrupt))
        for cut in (1, 8, 23):
            cases[f"short WAL file header {cut}"] = (snapshot(0), wal_header()[:cut])
        corrupt_snapshot = bytearray(snapshot(0))
        corrupt_snapshot[24] ^= 1
        cases["snapshot header CRC"] = (bytes(corrupt_snapshot), wal_header())
        cases["snapshot duplicate key"] = (snapshot(1, [(PRIVATE_KEY, b"first"), (PRIVATE_KEY, b"last")]), wal_header())
        cases["snapshot missing entry"] = (snapshot(1, [(PRIVATE_KEY, PRIVATE_VALUE)])[:-1], wal_header())
        cases["snapshot trailing bytes"] = (snapshot(0) + b"extra", wal_header())
        cases["snapshot impossible count"] = (with_crc(struct.pack("!8sQQ", b"MKVSNP01", 0, 1)), wal_header())
        cases["snapshot wrong record sequence"] = (with_crc(struct.pack("!8sQQ", b"MKVSNP01", 2, 1)) +
                                                     record(1, PRIVATE_KEY, PRIVATE_VALUE, version=1), wal_header())
        old = record(1, PRIVATE_KEY, PRIVATE_VALUE, version=1)
        for cut in (1, 21, len(old) - 1):
            cases[f"ambiguous legacy tail {cut}"] = (snapshot(0), old[:cut])
        for name, (snapshot_bytes, wal_bytes) in cases.items():
            with self.subTest(name=name):
                self.fixture(snapshot_bytes, wal_bytes)
                self.check(1)

    def test_checksum_valid_sequence_gaps_are_rejected(self):
        for version in (2, 1):
            for checkpoint, sequences in ((0, (2,)), (0, (1, 3)), (0, (1, 1)), (2, (1, 4)), (0, (0,))):
                with self.subTest(version=version, checkpoint=checkpoint, sequences=sequences):
                    wal = wal_header() if version == 2 else b""
                    wal += b"".join(record(sequence, PRIVATE_KEY, PRIVATE_VALUE, version=version)
                                    for sequence in sequences)
                    self.fixture(snapshot(checkpoint), wal)
                    self.check(1)

    def test_missing_files_and_text_import_do_not_create_storage(self):
        self.check(1)  # Missing directory must not be created.
        self.directory.mkdir()
        self.check(1)
        (self.directory / "LOCK").write_bytes(b"existing lock")
        self.check(1)
        for missing in ("LOCK", "snapshot.v1", "wal.v1"):
            with self.subTest(missing=missing):
                self.fixture()
                (self.directory / missing).unlink()
                self.check(1)
        for path in self.directory.iterdir():
            path.unlink()
        (self.directory / "data.db").write_bytes(PRIVATE_KEY + b":" + PRIVATE_VALUE + b"\n")
        (self.directory / "wal.log").write_bytes(b"PUT other value\n")
        (self.directory / "LOCK").write_bytes(b"existing lock")
        self.check(1)

    def test_unsafe_file_types_and_hard_links_are_rejected(self):
        for name in ("LOCK", "snapshot.v1", "wal.v1"):
            for kind in ("symlink", "dangling", "fifo", "directory", "hardlink"):
                with self.subTest(name=name, kind=kind):
                    self.fixture()
                    entry = self.directory / name
                    target = self.root / "target"
                    entry.rename(target)
                    if kind == "symlink":
                        entry.symlink_to(target)
                    elif kind == "dangling":
                        entry.symlink_to(self.root / "absent")
                    elif kind == "fifo":
                        os.mkfifo(entry)
                    elif kind == "directory":
                        entry.mkdir()
                    else:
                        os.link(target, entry)
                    self.check(0 if name == "snapshot.v1" and kind == "hardlink" else 1)
                    if kind == "directory":
                        entry.rmdir()
                    else:
                        entry.unlink()
                    target.unlink()

    def test_shared_lock_is_compatible_and_exclusive_lock_is_rejected(self):
        self.fixture()
        with (self.directory / "LOCK").open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.check(1)
            fcntl.flock(lock, fcntl.LOCK_UN)
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
            self.check()
            fcntl.flock(lock, fcntl.LOCK_UN)
        self.check()

    def test_active_engine_is_rejected_then_check_succeeds(self):
        self.fixture(wal_bytes=wal_header() + record(1, PRIVATE_KEY, PRIVATE_VALUE))
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        environment = self.environment(MINIKV_ENGINE_HOST="127.0.0.1", MINIKV_ENGINE_PORT=str(port),
                                       MINIKV_WAL_MODE="reliable", MINIKV_WAL_FLUSH_MS="1",
                                       MINIKV_SNAPSHOT_INTERVAL_MS="0", MINIKV_WORKERS="1")
        process = subprocess.Popen([str(ENGINE)], cwd=ROOT, env=environment,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    self.fail("engine exited during startup: " + "".join(process.communicate()))
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2) as connection:
                        connection.settimeout(1)
                        connection.sendall(struct.pack("!4sB3xII", b"MKV1", 2, len(PRIVATE_KEY), 0) + PRIVATE_KEY)
                        reply = b""
                        while len(reply) < 12 + len(PRIVATE_VALUE):
                            data = connection.recv(12 + len(PRIVATE_VALUE) - len(reply))
                            if not data:
                                self.fail("engine closed readiness response")
                            reply += data
                        self.assertEqual(reply[:12], struct.pack("!4sB3xI", b"MKR1", 1, len(PRIVATE_VALUE)))
                        self.assertEqual(reply[12:], PRIVATE_VALUE)
                    break
                except ConnectionRefusedError:
                    time.sleep(0.01)
            else:
                self.fail("engine did not become ready")
            self.check(1)
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                self.fail("engine did not stop: " + stdout + stderr)
        self.assertEqual(process.returncode, 0, stdout + stderr)
        self.check()

    def test_read_only_permissions_need_no_writable_lock_or_storage(self):
        self.fixture(wal_bytes=wal_header() + record(1, PRIVATE_KEY, PRIVATE_VALUE))
        # chmod alone is not evidence under root. Drop all root identities and
        # supplementary groups, then prove that actual writes fail before check.
        def drop_privileges():
            os.setgroups([])
            os.setgid(65534)
            os.setuid(65534)

        # Python 3.8 has no Popen user/group options. This test runner is single
        # threaded, so a minimal preexec_fn can safely lower the child identity.
        identity = {"preexec_fn": drop_privileges} if os.geteuid() == 0 else {}
        self.root.chmod(0o755)
        for path in self.directory.iterdir():
            path.chmod(0o444)
        self.directory.chmod(0o555)
        try:
            probe = subprocess.run([
                sys.executable, "-c",
                "import os, pathlib, sys\n"
                "root = pathlib.Path(sys.argv[1])\n"
                "for target, flags in ((root / 'LOCK', os.O_WRONLY), (root / 'probe', os.O_WRONLY | os.O_CREAT)):\n"
                "    try: descriptor = os.open(target, flags, 0o600)\n"
                "    except PermissionError: continue\n"
                "    else: os.close(descriptor); raise SystemExit('write unexpectedly permitted')\n",
                str(self.directory),
            ], capture_output=True, text=True, timeout=5, **identity)
            self.assertEqual(probe.returncode, 0, probe.stdout + probe.stderr)
            self.assert_report(self.check(**identity), image={PRIVATE_KEY: PRIVATE_VALUE}, sequence=1,
                               valid_bytes=len(wal_header() + record(1, PRIVATE_KEY, PRIVATE_VALUE)))
        finally:
            self.directory.chmod(0o755)
            for path in self.directory.iterdir():
                path.chmod(0o644)

    def test_capacity_uses_final_replayed_image_and_rejects_excess(self):
        for version in (2, 1):
            with self.subTest(version=version):
                wal = wal_header() if version == 2 else b""
                wal += record(2, b"large", b"much larger intermediate value", version=version)
                wal += record(3, b"large", operation=3, version=version)
                wal += record(4, b"k", b"v", version=version)
                self.fixture(snapshot(1, [(b"large", b"large snapshot value")]), wal)
                report = self.check(environment=self.environment(MINIKV_MAX_DATA_BYTES="2"))
                self.assert_report(report, image={b"k": b"v"}, sequence=4, checkpoint=1,
                                   version=version, valid_bytes=len(wal), capacity=2)
                self.check(1, environment=self.environment(MINIKV_MAX_DATA_BYTES="1"))
        for value in ("", "-1", "+1", " 1", "18446744073709551616", "invalid"):
            with self.subTest(capacity=value):
                self.check(1, environment=self.environment(MINIKV_MAX_DATA_BYTES=value))

    def test_uint64_sequences_and_capacity_are_reported_without_truncation(self):
        maximum = (1 << 64) - 1
        for version in (2, 1):
            with self.subTest(version=version):
                wal = (wal_header() if version == 2 else b"") + record(
                    maximum, PRIVATE_KEY, PRIVATE_VALUE, version=version)
                self.fixture(snapshot(maximum - 1), wal)
                report = self.check(environment=self.environment(MINIKV_MAX_DATA_BYTES=str(maximum)))
                self.assert_report(report, image={PRIVATE_KEY: PRIVATE_VALUE}, sequence=maximum,
                                   checkpoint=maximum - 1, version=version, valid_bytes=len(wal), capacity=maximum)

    def test_cli_rejects_combinations_and_ignores_runtime_configuration(self):
        self.fixture()
        for args in (("--check", "--import-legacy"), ("--import-legacy", "--check"),
                     ("--check", "--check"), ("--check", "unknown")):
            with self.subTest(args=args):
                self.check(1, args=args)
        environment = self.environment()
        for suffix in ("WORKERS", "REQUEST_QUEUE_SIZE", "MAX_CONNECTIONS", "CLIENT_IDLE_MS",
                       "ENGINE_PORT", "ENGINE_HOST", "WAL_MODE", "WAL_BATCH_SIZE", "WAL_QUEUE_BYTES",
                       "WAL_FLUSH_MS", "SNAPSHOT_INTERVAL_MS"):
            environment["MINIKV_" + suffix] = "invalid"
        self.assert_report(self.check(environment=environment), image={}, sequence=0, valid_bytes=24)

    def test_output_failures_report_error_without_mutating_storage(self):
        self.fixture(wal_bytes=record(1, PRIVATE_KEY, PRIVATE_VALUE, version=1))
        for destination in ("full device", "closed pipe"):
            with self.subTest(destination=destination):
                if destination == "full device":
                    output = os.open("/dev/full", os.O_WRONLY)
                else:
                    read_end, output = os.pipe()
                    os.close(read_end)
                before = inventory(self.root)
                try:
                    result = subprocess.run([str(ENGINE), "--check"], env=self.environment(), cwd=ROOT,
                                            stdout=output, stderr=subprocess.PIPE, text=True, timeout=5)
                finally:
                    os.close(output)
                    self.assertEqual(inventory(self.root), before, "output failure changed storage")
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertTrue(result.stderr, "failed report write had no diagnostic")
                self.assertNotIn(PRIVATE_KEY.decode(), result.stderr)
                self.assertNotIn("private-value-marker", result.stderr)


if __name__ == "__main__":
    unittest.main()
