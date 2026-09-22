#!/usr/bin/env python3
"""Exec a direct Linux experiment child with a parent-death kill fallback.

The runner supplies its own PID before Popen. Arming first and then checking
that PID covers a parent that dies before the prctl call. Ordinary exec keeps
the setting and PID; MiniKV's copied programs do not change credentials or
fork descendants. This is a hard-stop fallback, not a graceful shutdown path.
"""

import ctypes
import os
import signal
import sys


def arm_parent_death(expected_parent):
    if sys.platform != "linux":
        raise OSError("parent-death protection requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
    prctl.restype = ctypes.c_int
    # PR_SET_PDEATHSIG = 1. The uncatchable signal also stops children that
    # ignore SIGTERM. Normal shutdown remains under the live runner's control.
    if prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    if os.getppid() != expected_parent:
        raise OSError("experiment parent exited before child protection was armed")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    try:
        if len(argv) < 2:
            raise ValueError("expected parent PID and an absolute executable path")
        expected_parent = int(argv[0])
        command = argv[1:]
        if expected_parent <= 0 or not os.path.isabs(command[0]):
            raise ValueError("expected positive parent PID and an absolute executable path")
        arm_parent_death(expected_parent)
        os.execv(command[0], command)
    except (OSError, ValueError, AttributeError) as error:
        print("experiment child guard: " + str(error), file=sys.stderr, flush=True)
        return 125


if __name__ == "__main__":
    sys.exit(main())
