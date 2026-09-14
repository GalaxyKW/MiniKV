"""Bounded, local metadata and Linux process sampling for reproducible runs.

An ``available`` flag is true only when that group has no collection errors.
Partial values remain usable when another field is unavailable; missing values
are None, never synthetic zeroes. Resource counters are cumulative for one PID
and starttime pair, and memory values come from the visible /proc filesystem,
not from cgroup limits. No environment dump or network lookup is performed.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import platform
import stat
import subprocess
import time


_CMAKE_FIELDS = {
    "BUILD_TESTING", "MINIKV_SANITIZERS", "CMAKE_BUILD_TYPE",
    "CMAKE_CXX_COMPILER", "CMAKE_CXX_COMPILER_LAUNCHER", "CMAKE_CXX_STANDARD",
    "CMAKE_CXX_FLAGS", "CMAKE_EXE_LINKER_FLAGS", "CMAKE_GENERATOR", "CMAKE_MAKE_PROGRAM",
}
for _kind in ("DEBUG", "RELEASE", "RELWITHDEBINFO", "MINSIZEREL"):
    _CMAKE_FIELDS.add("CMAKE_CXX_FLAGS_" + _kind)
    _CMAKE_FIELDS.add("CMAKE_EXE_LINKER_FLAGS_" + _kind)

_PROCESS_FIELDS = (
    "rss_bytes", "hwm_bytes", "cpu_user_ticks", "cpu_system_ticks", "starttime_ticks",
    "read_bytes", "write_bytes", "cancelled_write_bytes",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _error(source: str, error: Exception) -> str:
    # Raw command stderr and exception strings may contain arbitrary file data.
    number = getattr(error, "errno", None)
    suffix = f" (errno={number})" if number is not None else ""
    return f"{source}: {type(error).__name__}{suffix}"


def _read(path: Path, source: str, errors: list[str]) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        errors.append(_error(source, error))
        return None


def _numbers(text: str, names: dict[str, str], *, kib: bool, source: str,
             errors: list[str]) -> dict:
    result = {field: None for field in names.values()}
    seen = set()
    for line in text.splitlines():
        name, separator, raw = line.partition(":")
        if not separator or name not in names:
            continue
        field = names[name]
        if name in seen:
            errors.append(f"{source}.{name}: duplicate field")
            result[field] = None
            continue
        seen.add(name)
        parts = raw.split()
        try:
            if (kib and (len(parts) != 2 or parts[1] != "kB")) or (not kib and len(parts) != 1):
                raise ValueError
            number = int(parts[0])
            if number < 0:
                raise ValueError
            result[field] = number * (1024 if kib else 1)
        except (ValueError, IndexError):
            errors.append(f"{source}.{name}: invalid number or unit")
    for name in names:
        if name not in seen:
            errors.append(f"{source}.{name}: missing field")
    return result


def _parse_process_stat(text: str, pid: int) -> dict:
    # comm may itself contain spaces, '(' and ')', so splitting the whole line
    # shifts every field. All fields after its final ')' have fixed positions.
    opening, closing = text.find("("), text.rfind(")")
    if opening < 1 or closing <= opening or int(text[:opening].strip()) != pid:
        raise ValueError("invalid process stat identity")
    fields = text[closing + 1:].split()
    if len(fields) < 20 or len(fields[0]) != 1:
        raise ValueError("truncated process stat")
    values = {
        "cpu_user_ticks": int(fields[11]),
        "cpu_system_ticks": int(fields[12]),
        "starttime_ticks": int(fields[19]),
    }
    if any(value < 0 for value in values.values()):
        raise ValueError("negative process counter")
    return values


def sample_process(pid: int, *, proc_root: Path = Path("/proc")) -> dict:
    """Read one process without assuming unavailable kernel counters are zero.

    CPU counters use SC_CLK_TCK ticks and include this process's threads, not
    waited-for children. read_bytes/write_bytes are /proc I/O storage counters,
    not syscall payload bytes. VmHWM is the kernel's process-lifetime RSS peak.
    Reading stat before and after detects PID reuse; the files are otherwise
    separate observations, not an atomic process snapshot.
    """
    errors: list[str] = []
    result = {
        "pid": pid, "sampled_at_utc": _utc_now(), "monotonic_ns": time.monotonic_ns(),
        "available": False, "errors": errors,
        **{field: None for field in _PROCESS_FIELDS},
    }
    if not isinstance(pid, int) or isinstance(pid, bool) or pid < 1:
        errors.append("pid: must be a positive integer")
        return result
    directory = Path(proc_root) / str(pid)
    first = _read(directory / "stat", "stat", errors)
    if first is None:
        return result
    try:
        identity = _parse_process_stat(first, pid)
    except ValueError as error:
        errors.append(_error("stat", error))
        return result
    result.update(identity)
    status = _read(directory / "status", "status", errors)
    if status is not None:
        result.update(_numbers(status, {"VmRSS": "rss_bytes", "VmHWM": "hwm_bytes"},
                               kib=True, source="status", errors=errors))
    io = _read(directory / "io", "io", errors)
    if io is not None:
        result.update(_numbers(io, {"read_bytes": "read_bytes", "write_bytes": "write_bytes",
                                   "cancelled_write_bytes": "cancelled_write_bytes"},
                               kib=False, source="io", errors=errors))
    final = _read(directory / "stat", "stat_after", errors)
    try:
        final_identity = _parse_process_stat(final, pid) if final is not None else None
        if final_identity is None or final_identity["starttime_ticks"] != identity["starttime_ticks"]:
            errors.append("stat_after: process disappeared or identity changed")
            result.update({field: None for field in _PROCESS_FIELDS})
        else:
            result.update(final_identity)
    except ValueError as error:
        errors.append(_error("stat_after", error))
        result.update({field: None for field in _PROCESS_FIELDS})
    result["available"] = not errors
    return result


def _run_git(root: Path, args: list[str], source: str, errors: list[str]) -> bytes | None:
    try:
        # Repository-selection variables override `git -C`, and injected Git
        # configuration can redirect the worktree or suppress real changes.
        # Metadata must describe root, independently of the invoking shell.
        environment = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
        command = subprocess.run(["git", "--no-optional-locks", "-C", str(root), *args],
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 timeout=10, check=False, env=environment)
    except (OSError, subprocess.TimeoutExpired) as error:
        errors.append(_error(source, error))
        return None
    if command.returncode:
        errors.append(f"{source}: command exited {command.returncode}")
        return None
    return command.stdout


def _git_metadata(root: Path) -> dict:
    errors: list[str] = []
    result = {"available": False, "errors": errors, "head": None, "tracked_diff_sha256": None,
              "dirty": None, "tracked_dirty": None, "untracked_files": []}
    head = _run_git(root, ["rev-parse", "--verify", "HEAD"], "git.head", errors)
    if head is not None:
        result["head"] = head.decode("ascii").strip()
        diff = _run_git(root, ["diff", "--binary", "--no-ext-diff", "--no-textconv", "HEAD", "--"],
                        "git.diff", errors)
        if diff is not None:
            result["tracked_diff_sha256"] = hashlib.sha256(diff).hexdigest()
    status = _run_git(root, ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                      "git.status", errors)
    if status is not None:
        entries = iter(status.split(b"\0"))
        tracked_dirty = False
        for entry in entries:
            if not entry:
                continue
            if len(entry) < 4 or entry[2:3] != b" ":
                errors.append("git.status: invalid porcelain record")
                break
            code, filename = entry[:2], entry[3:]
            if code == b"??":
                # Escaped invalid UTF-8 bytes remain safe in a UTF-8 JSON file.
                result["untracked_files"].append(filename.decode("utf-8", errors="backslashreplace"))
            else:
                tracked_dirty = True
                if b"R" in code or b"C" in code:
                    if not next(entries, None):
                        errors.append("git.status: missing rename source")
                        break
        result["untracked_files"].sort()
        result["tracked_dirty"] = tracked_dirty
        result["dirty"] = tracked_dirty or bool(result["untracked_files"])
    result["available"] = not errors
    return result


def _binary_metadata(path: Path) -> dict:
    errors: list[str] = []
    result = {"path": str(path), "size_bytes": None, "sha256": None, "available": False, "errors": errors}
    try:
        before = path.stat()
        if not stat.S_ISREG(before.st_mode):
            errors.append("binary: not a regular file")
            return result
        digest = hashlib.sha256()
        with path.open("rb") as binary:
            opened = os.fstat(binary.fileno())
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
            finished = os.fstat(binary.fileno())
        after = path.stat()
        identity = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns)
        if not identity(before) == identity(opened) == identity(finished) == identity(after):
            errors.append("binary: changed during hashing")
        else:
            result["size_bytes"] = finished.st_size
            result["sha256"] = digest.hexdigest()
    except OSError as error:
        errors.append(_error("binary", error))
    result["available"] = not errors
    return result


def _cmake_metadata(path: Path) -> dict:
    errors: list[str] = []
    result = {"available": False, "errors": errors, "fields": {}}
    text = _read(path, "cmake_cache", errors)
    if text is not None:
        for line in text.splitlines():
            name, separator, rest = line.partition(":")
            if not separator or name not in _CMAKE_FIELDS:
                continue
            _, separator, value = rest.partition("=")
            if separator:
                result["fields"][name] = value
            else:
                errors.append(f"cmake_cache.{name}: invalid field")
    result["available"] = not errors
    return result


def collect_metadata(root: Path, binaries: dict[str, Path], *, proc_root: Path = Path("/proc")) -> dict:
    """Collect provenance, not proof a binary was built from this checkout.

    tracked_diff_sha256 hashes `git diff --binary HEAD` (index and working-tree
    changes combined), and never records patch contents. Untracked names are
    recorded without reading their contents. CMake caches are available build
    records, not an attestation linking flags/source to a selected binary.
    """
    root = Path(root).absolute()
    proc_root = Path(proc_root)
    cpu_errors: list[str] = []
    cpu = {"model_name": None, "logical_cpus": os.cpu_count(), "affinity_cpus": None,
           "clock_ticks_per_second": None, "available": False, "errors": cpu_errors}
    info = _read(proc_root / "cpuinfo", "cpuinfo", cpu_errors)
    if info is not None:
        for line in info.splitlines():
            name, separator, value = line.partition(":")
            if separator and name.strip() in ("model name", "Hardware", "Processor") and value.strip():
                cpu["model_name"] = value.strip()
                break
        if cpu["model_name"] is None:
            cpu_errors.append("cpuinfo: model unavailable")
    if cpu["logical_cpus"] is None:
        cpu_errors.append("logical_cpus: unavailable")
    try:
        cpu["affinity_cpus"] = sorted(os.sched_getaffinity(0))
    except (AttributeError, OSError) as error:
        cpu_errors.append(_error("affinity", error))
    try:
        ticks = os.sysconf("SC_CLK_TCK")
        if ticks <= 0:
            raise ValueError
        cpu["clock_ticks_per_second"] = ticks
    except (AttributeError, OSError, ValueError) as error:
        cpu_errors.append(_error("clock_ticks", error))
    cpu["available"] = not cpu_errors

    memory_errors: list[str] = []
    memory = {"total_bytes": None, "available_bytes": None, "available": False, "errors": memory_errors}
    meminfo = _read(proc_root / "meminfo", "meminfo", memory_errors)
    if meminfo is not None:
        memory.update(_numbers(meminfo, {"MemTotal": "total_bytes", "MemAvailable": "available_bytes"},
                               kib=True, source="meminfo", errors=memory_errors))
    memory["available"] = not memory_errors

    binary_paths = {name: (path if path.is_absolute() else root / path).absolute()
                    for name, path in ((name, Path(value)) for name, value in binaries.items())}
    cache_paths = {root / "build" / "CMakeCache.txt"}
    for path in binary_paths.values():
        candidate = path.parent / "CMakeCache.txt"
        try:
            if candidate.is_file():
                cache_paths.add(candidate)
        except OSError:
            # Let the normal reader report a permission error for this cache.
            cache_paths.add(candidate)
    return {
        "schema_version": 1, "collected_at_utc": _utc_now(),
        "platform": {"system": platform.system(), "release": platform.release(), "machine": platform.machine(),
                     "python_version": platform.python_version(), "python_implementation": platform.python_implementation()},
        "cpu": cpu, "memory": memory, "git": _git_metadata(root),
        "binaries": {name: _binary_metadata(path) for name, path in binary_paths.items()},
        "cmake_caches": {str(path): _cmake_metadata(path) for path in sorted(cache_paths)},
    }
