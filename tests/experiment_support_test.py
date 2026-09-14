#!/usr/bin/env python3
"""Validate experiment provenance and process sampling without running services."""

import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("experiment_support", ROOT / "benmark" / "experiment_support.py")
support = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(support)


def stat_line(pid=321, comm="worker ) (copy job)", user=17, system=23, start=123456):
    fields = ["0"] * 20
    fields[0] = "S"
    fields[11], fields[12], fields[19] = str(user), str(system), str(start)
    fields[13], fields[14] = "901", "902"  # Child CPU must not be added to own CPU.
    return f"{pid} ({comm}) " + " ".join(fields) + "\n"


class ProcessSampleTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="minikv-proc-fixture-")
        self.addCleanup(self.directory.cleanup)
        self.proc = Path(self.directory.name)
        self.process = self.proc / "321"
        self.process.mkdir()
        (self.process / "stat").write_text(stat_line())
        (self.process / "status").write_text("Name:\tworker\nVmPeak:\t999 kB\nVmHWM:\t55 kB\nVmRSS:\t12 kB\n")
        (self.process / "io").write_text("rchar: 98765\nread_bytes: 4096\nwrite_bytes: 0\ncancelled_write_bytes: 512\n")

    def sample(self, pid=321):
        return support.sample_process(pid, proc_root=self.proc)

    def test_parens_spaces_units_and_process_counters(self):
        result = self.sample()
        self.assertTrue(result["available"], result["errors"])
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["rss_bytes"], 12 * 1024)
        self.assertEqual(result["hwm_bytes"], 55 * 1024)
        self.assertEqual(result["cpu_user_ticks"], 17)
        self.assertEqual(result["cpu_system_ticks"], 23)
        self.assertEqual(result["starttime_ticks"], 123456)
        self.assertEqual(result["read_bytes"], 4096)
        self.assertEqual(result["write_bytes"], 0)  # A measured zero remains zero.
        self.assertEqual(result["cancelled_write_bytes"], 512)
        self.assertTrue(result["sampled_at_utc"].endswith("Z"))
        self.assertGreater(result["monotonic_ns"], 0)

    def test_missing_process_and_invalid_pid_do_not_invent_zeroes(self):
        for pid in (999, 0, -1, True):
            with self.subTest(pid=pid):
                result = self.sample(pid)
                self.assertFalse(result["available"])
                self.assertTrue(result["errors"])
                self.assertTrue(all(result[field] is None for field in support._PROCESS_FIELDS))

    def test_missing_memory_field_keeps_other_measured_fields(self):
        (self.process / "status").write_text("VmHWM: 55 kB\n")
        result = self.sample()
        self.assertFalse(result["available"])
        self.assertIsNone(result["rss_bytes"])
        self.assertEqual(result["hwm_bytes"], 55 * 1024)
        self.assertEqual(result["read_bytes"], 4096)
        self.assertIn("status.VmRSS: missing field", result["errors"])

    def test_permission_error_preserves_partial_data_and_explicit_error(self):
        original = Path.read_text

        def read(path, *args, **kwargs):
            if path == self.process / "io":
                raise PermissionError(errno.EACCES, "private diagnostic must not be copied")
            return original(path, *args, **kwargs)

        with mock.patch.object(Path, "read_text", read):
            result = self.sample()
        self.assertFalse(result["available"])
        self.assertEqual(result["rss_bytes"], 12 * 1024)
        self.assertIsNone(result["read_bytes"])
        self.assertIsNone(result["write_bytes"])
        self.assertIn("io: PermissionError (errno=13)", result["errors"])
        self.assertNotIn("private diagnostic", json.dumps(result))

    def test_invalid_counter_units_duplicates_and_negative_numbers(self):
        for text in ("VmRSS: 12 MB\nVmHWM: 55 kB\n", "VmRSS: -1 kB\nVmHWM: 55 kB\n",
                     "VmRSS: 12 kB\nVmRSS: 13 kB\nVmHWM: 55 kB\n"):
            with self.subTest(status=text):
                (self.process / "status").write_text(text)
                result = self.sample()
                self.assertFalse(result["available"])
                self.assertIsNone(result["rss_bytes"])
                self.assertEqual(result["hwm_bytes"], 55 * 1024)
        (self.process / "io").write_text("read_bytes: invalid\nwrite_bytes: -1\ncancelled_write_bytes: 0\n")
        result = self.sample()
        self.assertIsNone(result["read_bytes"])
        self.assertIsNone(result["write_bytes"])
        self.assertEqual(result["cancelled_write_bytes"], 0)

    def test_truncated_bad_identity_and_bad_cpu_stat(self):
        for text in ("321 (worker) S 0\n", stat_line(pid=322), stat_line(user=-1), "no process fields"):
            with self.subTest(stat=text):
                (self.process / "stat").write_text(text)
                result = self.sample()
                self.assertFalse(result["available"])
                self.assertTrue(result["errors"])
                self.assertTrue(all(result[field] is None for field in support._PROCESS_FIELDS))

    def test_exit_and_pid_reuse_during_sample_discard_mixed_data(self):
        original = Path.read_text
        for exited in (False, True):
            with self.subTest(exited=exited):
                reads = 0

                def read(path, *args, **kwargs):
                    nonlocal reads
                    if path == self.process / "stat":
                        reads += 1
                        if reads == 2:
                            if exited:
                                raise FileNotFoundError(errno.ENOENT, "process exited")
                            return stat_line(start=123457)
                    return original(path, *args, **kwargs)

                with mock.patch.object(Path, "read_text", read):
                    result = self.sample()
                self.assertFalse(result["available"])
                self.assertTrue(all(result[field] is None for field in support._PROCESS_FIELDS))
                self.assertIn("stat_after: process disappeared or identity changed", result["errors"])


class MetadataTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="minikv-metadata-fixture-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.proc = self.root / "proc"
        self.proc.mkdir()
        (self.proc / "cpuinfo").write_text("processor : 0\nmodel name : Fixture CPU\n\nprocessor : 1\nmodel name : Fixture CPU\n")
        (self.proc / "meminfo").write_text("MemTotal: 1000 kB\nMemAvailable: 250 kB\nMemFree: 100 kB\n")
        (self.root / "build").mkdir()
        self.cache = self.root / "build" / "CMakeCache.txt"
        self.cache.write_text("# comment\nCMAKE_BUILD_TYPE:STRING=RelWithDebInfo\n"
                              "CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/c++\n"
                              "CMAKE_CXX_FLAGS:STRING=-Wall -DNUMBER=2\n"
                              "MINIKV_SANITIZERS:BOOL=OFF\n"
                              "PRIVATE_TOKEN:STRING=must-not-be-collected\n")

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), *args], stderr=subprocess.DEVNULL)

    def initialize_git(self):
        if not shutil.which("git"):
            self.skipTest("git is not installed")
        self.git("init", "--quiet")
        self.git("config", "user.name", "MiniKV metadata test")
        self.git("config", "user.email", "minikv-test@example.invalid")
        (self.root / "tracked name").write_text("original\n")
        self.git("add", "tracked name")
        self.git("commit", "--quiet", "-m", "fixture")
        # Fixture /proc and build files do not describe user untracked changes.
        (self.root / ".git" / "info" / "exclude").write_text("/proc/\n/build/\n")

    def metadata(self, binaries=None):
        with mock.patch.object(os, "cpu_count", return_value=8), \
             mock.patch.object(os, "sched_getaffinity", return_value={2, 4}), \
             mock.patch.object(os, "sysconf", return_value=100):
            return support.collect_metadata(self.root, binaries or {}, proc_root=self.proc)

    def test_cpu_memory_cmake_and_no_environment_dump(self):
        with mock.patch.dict(os.environ, {"PRIVATE_TOKEN": "environment-must-not-be-collected"}):
            result = self.metadata()
        self.assertEqual(result["schema_version"], 1)
        self.assertTrue(result["collected_at_utc"].endswith("Z"))
        self.assertEqual(result["cpu"]["model_name"], "Fixture CPU")
        self.assertEqual(result["cpu"]["logical_cpus"], 8)
        self.assertEqual(result["cpu"]["affinity_cpus"], [2, 4])
        self.assertEqual(result["cpu"]["clock_ticks_per_second"], 100)
        self.assertTrue(result["cpu"]["available"])
        self.assertEqual(result["memory"]["total_bytes"], 1000 * 1024)
        self.assertEqual(result["memory"]["available_bytes"], 250 * 1024)
        cache = result["cmake_caches"][str(self.cache)]
        self.assertTrue(cache["available"])
        self.assertEqual(cache["fields"]["CMAKE_CXX_FLAGS"], "-Wall -DNUMBER=2")
        self.assertNotIn("PRIVATE_TOKEN", json.dumps(result))
        self.assertNotIn("must-not-be-collected", json.dumps(result))
        self.assertFalse(result["git"]["available"])
        self.assertIsNone(result["git"]["head"])

    def test_missing_memory_is_not_reported_as_empty_host(self):
        (self.proc / "meminfo").unlink()
        result = self.metadata()["memory"]
        self.assertFalse(result["available"])
        self.assertIsNone(result["total_bytes"])
        self.assertIsNone(result["available_bytes"])
        self.assertTrue(result["errors"])

    def test_binary_hash_size_missing_file_and_neighboring_cache(self):
        binary = self.root / "custom-build" / "engine"
        binary.parent.mkdir()
        data = b"MiniKV binary fixture\0" * 100000
        binary.write_bytes(data)
        extra_cache = binary.parent / "CMakeCache.txt"
        extra_cache.write_text("CMAKE_BUILD_TYPE:STRING=Debug\n")
        result = self.metadata({"engine": binary.relative_to(self.root), "missing": self.root / "missing"})
        observed = result["binaries"]["engine"]
        self.assertTrue(observed["available"], observed["errors"])
        self.assertEqual(observed["path"], str(binary))
        self.assertEqual(observed["size_bytes"], len(data))
        self.assertEqual(observed["sha256"], hashlib.sha256(data).hexdigest())
        self.assertIn(str(extra_cache), result["cmake_caches"])
        missing = result["binaries"]["missing"]
        self.assertFalse(missing["available"])
        self.assertIsNone(missing["size_bytes"])
        self.assertIsNone(missing["sha256"])

    def test_binary_replacement_during_hash_is_detected(self):
        binary = self.root / "build" / "engine"
        binary.write_bytes(b"before")
        original = Path.stat
        reads = 0

        def metadata(path, *args, **kwargs):
            nonlocal reads
            if path == binary:
                reads += 1
                if reads == 2:
                    replacement = binary.with_suffix(".new")
                    replacement.write_bytes(b"after")
                    replacement.replace(binary)
            return original(path, *args, **kwargs)

        with mock.patch.object(Path, "stat", metadata):
            result = support._binary_metadata(binary)
        self.assertFalse(result["available"])
        self.assertIsNone(result["sha256"])
        self.assertIn("binary: changed during hashing", result["errors"])

    def test_git_clean_dirty_diff_hash_and_untracked_names_without_contents(self):
        self.initialize_git()
        clean = self.metadata()["git"]
        self.assertTrue(clean["available"], clean["errors"])
        self.assertFalse(clean["dirty"])
        self.assertEqual(clean["head"], self.git("rev-parse", "HEAD").decode().strip())
        self.assertEqual(clean["tracked_diff_sha256"], hashlib.sha256(b"").hexdigest())
        (self.root / "tracked name").write_text("tracked-private-content\n")
        untracked = self.root / "untracked file\nname"
        untracked.write_text("untracked-private-content")
        dirty = self.metadata()["git"]
        self.assertTrue(dirty["dirty"])
        self.assertTrue(dirty["tracked_dirty"])
        self.assertEqual(dirty["untracked_files"], [untracked.name])
        diff = self.git("diff", "--binary", "--no-ext-diff", "--no-textconv", "HEAD", "--")
        self.assertEqual(dirty["tracked_diff_sha256"], hashlib.sha256(diff).hexdigest())
        self.assertNotIn("private-content", json.dumps(dirty))
        untracked.write_text("different private contents")
        self.assertEqual(self.metadata()["git"]["tracked_diff_sha256"], dirty["tracked_diff_sha256"])

    def test_git_rename_and_untracked_newlines_are_separate_records(self):
        self.initialize_git()
        self.git("mv", "tracked name", "renamed\nfile")
        (self.root / "untracked\nfile").write_text("private")
        result = self.metadata()["git"]
        self.assertTrue(result["available"], result["errors"])
        self.assertTrue(result["tracked_dirty"])
        self.assertEqual(result["untracked_files"], ["untracked\nfile"])

    def test_git_metadata_ignores_inherited_repository_and_config_overrides(self):
        self.initialize_git()
        (self.root / "tracked name").write_text("local changes\n")
        (self.root / "local untracked").write_text("local contents")
        expected = self.metadata()["git"]

        with tempfile.TemporaryDirectory(prefix="minikv-other-repository-") as directory:
            other = Path(directory)
            subprocess.run(["git", "init", "--quiet", str(other)], check=True)
            subprocess.run(["git", "-C", str(other), "-c", "user.name=Fixture",
                            "-c", "user.email=fixture@example.invalid", "commit", "--quiet",
                            "--allow-empty", "-m", "other repository"], check=True)
            (other / "foreign untracked").write_text("other contents")
            overrides = {
                "GIT_DIR": str(other / ".git"), "GIT_WORK_TREE": str(other),
                "GIT_INDEX_FILE": str(other / ".git" / "index"),
                "GIT_COMMON_DIR": str(other / ".git"),
                "GIT_OBJECT_DIRECTORY": str(other / ".git" / "objects"),
                "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(other / ".git" / "objects"),
                "GIT_CONFIG_COUNT": "2", "GIT_CONFIG_KEY_0": "core.worktree", "GIT_CONFIG_VALUE_0": str(other),
                "GIT_CONFIG_KEY_1": "status.showUntrackedFiles", "GIT_CONFIG_VALUE_1": "no",
            }
            with mock.patch.dict(os.environ, overrides):
                result = self.metadata()["git"]
                # Collection must not change the caller's environment either.
                self.assertEqual(os.environ["GIT_DIR"], str(other / ".git"))
            self.assertEqual(result, expected)
            self.assertTrue(result["available"], result["errors"])
            self.assertEqual(result["untracked_files"], ["local untracked"])


if __name__ == "__main__":
    unittest.main()
