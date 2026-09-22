#!/usr/bin/env python3
"""Bounded process-guard checks without starting experiment services."""

import ctypes
import errno
import io
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
GUARD = ROOT / "benmark" / "process_guard.py"
sys.path.insert(0, str(ROOT / "benmark"))
import process_guard


def wait_child(pid, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found, status = os.waitpid(pid, os.WNOHANG)
        if found:
            return status
        time.sleep(.01)
    raise AssertionError("child did not exit before deadline: %d" % pid)


def read_ready(fd):
    if not select.select([fd], [], [], 5)[0]:
        raise AssertionError("child did not reach its pipe gate")
    value = os.read(fd, 128)
    if not value:
        raise AssertionError("child closed its readiness pipe")
    return value


def reap_driver_children():
    # This isolated, single-threaded subreaper owns only this test's descendants.
    # A direct child cannot have its PID reused until this driver reaps it.
    deadline = time.monotonic() + 5
    children_path = Path("/proc/self/task", str(os.getpid()), "children")
    while time.monotonic() < deadline:
        try:
            while os.waitpid(-1, os.WNOHANG)[0]:
                pass
        except ChildProcessError:
            return
        for raw in children_path.read_text().split():
            try:
                os.kill(int(raw), signal.SIGKILL)
            except ProcessLookupError:
                pass
        time.sleep(.01)
    raise AssertionError("test driver could not reap its descendants")


def orphan_driver(marker):
    """Exercise the pre-installation race under a disposable subreaper."""
    def terminate(signum, frame):
        raise RuntimeError("test driver terminated before completion")
    signal.signal(signal.SIGTERM, terminate)
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
    prctl.restype = ctypes.c_int
    if prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "cannot install test subreaper")
    pid_read, pid_write = os.pipe()
    ready_read, ready_write = os.pipe()
    gate_read, gate_write = os.pipe()
    result = {}
    try:
        parent = os.fork()
        if parent == 0:
            try:
                os.close(pid_read)
                os.close(ready_read)
                os.close(gate_write)
                expected_parent = os.getpid()
                child = os.fork()
                if child == 0:
                    os.close(pid_write)
                    os.write(ready_write, b"ready")
                    os.close(ready_write)
                    if os.read(gate_read, 1) != b"g":
                        os._exit(124)
                    os.close(gate_read)
                    code = "from pathlib import Path; Path(__import__('sys').argv[1]).touch()"
                    os.execv(sys.executable, [sys.executable, "-I", "-S", str(GUARD), str(expected_parent),
                                             sys.executable, "-c", code, str(marker)])
                os.close(ready_write)
                os.close(gate_read)
                os.write(pid_write, str(child).encode())
                os.close(pid_write)
                while True:
                    signal.pause()
            except BaseException:
                os._exit(124)
        os.close(pid_write)
        os.close(ready_write)
        os.close(gate_read)
        child = int(read_ready(pid_read))
        assert read_ready(ready_read) == b"ready"
        os.kill(parent, signal.SIGKILL)
        parent_status = wait_child(parent)
        assert os.WIFSIGNALED(parent_status) and os.WTERMSIG(parent_status) == signal.SIGKILL
        # getppid() will be this live subreaper, not PID 1. Merely checking for
        # PID 1 therefore cannot satisfy the expected-parent contract.
        status = Path("/proc", str(child), "status").read_text()
        adopted_by = int(next(line.split()[1] for line in status.splitlines() if line.startswith("PPid:")))
        assert adopted_by == os.getpid(), "gated child was not adopted by the test driver"
        os.write(gate_write, b"g")
        child_status = wait_child(child)
        result = {"adopted_by_driver": True, "child_exited": os.WIFEXITED(child_status),
                  "child_exit": os.WEXITSTATUS(child_status) if os.WIFEXITED(child_status) else None,
                  "target_started": marker.exists()}
        assert result["child_exited"] and result["child_exit"] == 125, result
        assert not result["target_started"], "guard executed target after its original parent disappeared"
    finally:
        # Assertions and wait statuses above precede emergency cleanup; cleanup
        # cannot turn a leaked running target into a passing result.
        reap_driver_children()
        for fd in (pid_read, ready_read, gate_write):
            os.close(fd)
    print(json.dumps(result), flush=True)


@unittest.skipUnless(sys.platform == "linux", "process guard requires Linux")
class ProcessGuardTests(unittest.TestCase):
    def test_prctl_failure_refuses_to_execute_target(self):
        prctl = mock.Mock(return_value=-1)
        libc = mock.Mock(prctl=prctl)
        error = io.StringIO()
        with mock.patch.object(process_guard.ctypes, "CDLL", return_value=libc), \
                mock.patch.object(process_guard.ctypes, "get_errno", return_value=errno.EPERM), \
                mock.patch.object(process_guard.os, "getppid") as parent, \
                mock.patch.object(process_guard.os, "execv") as execute, \
                mock.patch.object(process_guard.sys, "stderr", error):
            result = process_guard.main([str(os.getpid()), sys.executable, "-c", "raise SystemExit(0)"])
        self.assertEqual(result, 125)
        prctl.assert_called_once_with(1, signal.SIGKILL, 0, 0, 0)
        parent.assert_not_called()
        execute.assert_not_called()
        self.assertIn("experiment child guard:", error.getvalue())

    def test_exec_preserves_pid_arguments_stdout_and_exit_status(self):
        arguments = ["space in argument", "", "Unicode-\u4e2d\u6587", "line\nbreak", "--literal-option"]
        code = ("import json, os, signal, sys; "
                "print(json.dumps({'pid':os.getpid(),'parent':os.getppid(),'argv':sys.argv[2:]}),flush=True); "
                "mode=int(sys.argv[1]); "
                "os.kill(os.getpid(),signal.SIGTERM) if mode < 0 else sys.exit(mode)")
        for exit_code in (0, 7, -signal.SIGTERM):
            with self.subTest(exit_code=exit_code):
                process = subprocess.Popen([sys.executable, "-I", "-S", str(GUARD), str(os.getpid()), sys.executable,
                                            "-c", code, str(exit_code), *arguments],
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    stdout, stderr = process.communicate(timeout=5)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
                self.assertEqual(process.returncode, exit_code, stderr)
                observed = json.loads(stdout)
                self.assertEqual(observed, {"pid": process.pid, "parent": os.getpid(), "argv": arguments})

    def test_parent_dead_before_guard_installation_never_executes_target(self):
        with tempfile.TemporaryDirectory(prefix="minikv-guard-race-") as directory:
            marker = Path(directory) / "target-started"
            process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--orphan-driver", str(marker)],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                stdout, stderr = process.communicate(timeout=25)
            finally:
                if process.poll() is None:
                    # Let the driver's finally reap its children before using
                    # SIGKILL as the last resort for a broken test harness.
                    process.terminate()
                    try:
                        process.communicate(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stdout + stderr)
            self.assertEqual(json.loads(stdout), {"adopted_by_driver": True, "child_exited": True,
                                                "child_exit": 125, "target_started": False})
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--orphan-driver":
        orphan_driver(Path(sys.argv[2]))
    else:
        unittest.main()
