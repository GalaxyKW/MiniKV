#!/usr/bin/env python3
"""Independently verify a moved capacity archive; never run benchmark services.

The bundled auditor runs under a Python audit hook that rejects reads beneath
the original experiment root and rejects subprocess/network use. The collector
is extracted separately from its frozen Git revision before that audit phase.
Only this check's JSON result and a fresh temporary tree are written.
"""
import argparse
from collections import Counter
import contextlib
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import runpy
import subprocess
import sys
import tarfile
import tempfile


REVISION = "bc7e031a962ff44367a160a5fc5adcda83ac1893"
ARCHIVE_SHA = "e05918769798088f1eafc214b0c77b51cdd642314b3819cddc9f115dee80827b"
FORBIDDEN = Path("/mnt/nvme/minikv-review/capacity-stagewise-iobxnqbx")
MODULES = ("benmark/experiment.py", "benmark/summarize.py",
           "benmark/experiment_support.py", "benmark/snapshot_report.py",
           "benmark/process_guard.py")
BINARY_WARNING = "Executable copies are absent; provenance uses recorded hashes only (recorded_hashes_only)."


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def sha_bytes(value):
    return hashlib.sha256(value).hexdigest()


def sha(path):
    return sha_bytes(path.read_bytes())


def load(path):
    return json.loads(path.read_text())


def safe_name(name):
    parts = PurePosixPath(name).parts
    require(name and not name.startswith("/") and "\\" not in name
            and ".." not in parts and "." not in parts
            and str(PurePosixPath(name)) == name, "unsafe member path: " + repr(name))
    return parts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--git-repository", type=Path, required=True)
    parser.add_argument("--temporary-parent", type=Path, default=Path("/tmp"))
    args = parser.parse_args()
    artifact = args.artifact.resolve()
    work = Path(tempfile.mkdtemp(prefix="capacity-isolated-relocation-", dir=args.temporary_parent))
    require(FORBIDDEN != work and FORBIDDEN not in work.parents, "temporary tree is under original root")
    forbidden_opens, opened_paths, disallowed_actions = [], set(), []
    audit_phase = False

    def audit_hook(event, arguments):
        if event == "open" and isinstance(arguments[0], (str, bytes, os.PathLike)):
            path = os.path.realpath(os.fsdecode(arguments[0]))
            if path == str(FORBIDDEN) or path.startswith(str(FORBIDDEN) + os.sep):
                forbidden_opens.append(path)
                raise PermissionError("original experiment root reads are forbidden: " + path)
            if audit_phase:
                opened_paths.add(path)
        if audit_phase and (event == "subprocess.Popen" or event == "os.system"
                            or event in ("socket.connect", "socket.bind")):
            disallowed_actions.append(event)
            raise PermissionError("offline auditor action forbidden: " + event)

    sys.addaudithook(audit_hook)
    manifest = load(artifact / "MANIFEST.json")
    archive = artifact / manifest["archive"]["file"]
    require(sha(archive) == ARCHIVE_SHA == manifest["archive"]["sha256"], "archive digest mismatch")
    require(archive.stat().st_size == manifest["archive"]["size_bytes"], "archive size mismatch")
    recorded = manifest["members"]
    names = [item["path"] for item in recorded]
    require(len(names) == len(set(names)), "duplicate manifest member")
    expected = {item["path"]: item for item in recorded}
    require(len(expected) == manifest["archive"]["files"] == 619, "member count differs")
    unpacked = work / "unpacked"
    raw_bytes = 0
    with tarfile.open(archive, "r:gz") as stream:
        members = stream.getmembers()
        require(len(members) == len(expected), "tar member count differs")
        require(len({member.name for member in members}) == len(members), "duplicate tar member")
        require({member.name for member in members} == set(expected), "tar/manifest member set differs")
        for member in members:
            parts = safe_name(member.name)
            require(parts[0] == "data-capacity-stagewise" and len(parts) > 1, "unexpected archive root")
            require(member.isfile() and not member.issym() and not member.islnk(), "nonregular archive member")
            info = expected[member.name]
            content = stream.extractfile(member).read()
            require(member.size == len(content) == info["size_bytes"], "member size mismatch: " + member.name)
            require(sha_bytes(content) == info["sha256"], "member digest mismatch: " + member.name)
            destination = unpacked.joinpath(*parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(content)
            raw_bytes += len(content)
    require(raw_bytes == manifest["archive"]["raw_bytes"] == 35865235, "raw byte total mismatch")
    moved_parent = work / "relocated"
    moved_parent.mkdir()
    moved = moved_parent / "data-capacity-stagewise"
    (unpacked / "data-capacity-stagewise").rename(moved)
    require(not (unpacked / "data-capacity-stagewise").exists(), "archive tree did not move")
    checked_attachments = []
    for attachment in manifest["attachments"]:
        parts = safe_name(attachment["file"])
        require(len(parts) == 1, "attachment is not a sibling file")
        require(sha(artifact / parts[0]) == attachment["sha256"], "attachment digest mismatch: " + parts[0])
        checked_attachments.append(parts[0])

    collector = work / "collector"
    git_command = ["git", "--no-optional-locks", "-C", str(args.git_repository.resolve()),
                   "archive", "--format=tar", REVISION, *MODULES]
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    git_result = subprocess.run(git_command, env=environment, check=True, capture_output=True, timeout=30)
    with tarfile.open(fileobj=io.BytesIO(git_result.stdout), mode="r:") as stream:
        extracted = []
        for member in stream.getmembers():
            parts = safe_name(member.name.rstrip("/"))
            if member.isdir():
                continue
            require(member.isfile() and member.name in MODULES, "unexpected collector member")
            destination = collector.joinpath(*parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(stream.extractfile(member).read())
            extracted.append(member.name)
    require(set(extracted) == set(MODULES), "collector extraction incomplete")
    plan = load(moved / "plan.json")
    collector_hashes = {name: sha(collector / name) for name in MODULES}
    require(collector_hashes == plan["collector_files_sha256"], "collector bytes differ from frozen plan")
    require(plan["collector_revision"] == REVISION, "collector revision differs")
    require(not any((moved / "frozen").glob("*")), "frozen binaries unexpectedly retained")
    for stage in plan["stages"]:
        stage_root = moved / stage["directory"]
        require(not (stage_root / "binaries").exists(), "stage binary copies unexpectedly retained")
        require(not any(path.name == "data" for path in stage_root.rglob("*")), "data directory unexpectedly retained")
        require(sha(stage_root / "process_guard.py") == collector_hashes["benmark/process_guard.py"], "helper copy differs")

    old_argv = sys.argv
    auditor_command = [sys.executable, str(artifact / "audit_capacity.py"), str(moved), "--repository", str(collector)]
    sys.argv = auditor_command[1:]
    sys.dont_write_bytecode = True
    audit_stdout, audit_stderr = io.StringIO(), io.StringIO()
    audit_phase = True
    try:
        with contextlib.redirect_stdout(audit_stdout), contextlib.redirect_stderr(audit_stderr):
            try:
                runpy.run_path(str(artifact / "audit_capacity.py"), run_name="__main__")
            except SystemExit as error:
                exit_code = 0 if error.code is None else error.code
            else:
                exit_code = 0
    finally:
        audit_phase = False
        sys.argv = old_argv
    require(exit_code == 0, "relocated auditor returned nonzero: " + str(exit_code))
    require(not forbidden_opens and not disallowed_actions, "auditor attempted forbidden I/O")
    original, relocated = load(artifact / "analysis.json"), load(moved / "analysis.json")
    require(relocated["counts"] == original["counts"] and relocated["counts"]["eligible_rounds"] == 48
            and relocated["counts"]["official_status"]["valid"] == 48, "round count mismatch")
    require(len(relocated["rounds"]) == 48 and len(relocated["pairs"]) == 32
            and len(relocated["comparisons"]) == 8, "matrix sizes differ")
    require(all(pair["eligible"] for pair in relocated["pairs"]), "ineligible pair")
    require(all(group["eligible_pairs"] == 4 and group["classification"] == "not_triggered"
                and group["engine_rss_diagnostic"] == "not_triggered" for group in relocated["comparisons"]),
            "comparison result differs")
    require(relocated["rounds"] == original["rounds"], "round fields differ")
    require(relocated["pairs"] == original["pairs"], "pair fields differ")
    require(relocated["comparisons"] == original["comparisons"], "comparison fields differ")
    allowed_differences = []
    require(original["warnings"] == [], "unexpected original top-level warnings")
    frozen_warnings = ["frozen/" + name + " absent; recorded hashes only" for name in plan["binaries"]]
    require(Counter(relocated["warnings"]) == Counter(frozen_warnings), "unexpected relocated top-level warnings")
    normalized = json.loads(json.dumps(relocated))
    normalized["warnings"] = original["warnings"]
    allowed_differences.append({"path": "warnings", "before": [], "after": relocated["warnings"]})
    require(len(relocated["stages"]) == len(original["stages"]) == 12, "stage count differs")
    for number, (before, after) in enumerate(zip(original["stages"], relocated["stages"])):
        require(before["process_guard_verification"] == after["process_guard_verification"] == "copy_verified", "guard level differs")
        require(before["binary_verification"] == "copies_verified" and after["binary_verification"] == "recorded_hashes_only", "binary level differs")
        require(before["warnings"] == [] and after["warnings"] == [BINARY_WARNING], "unexpected stage warnings")
        for key in ("binary_verification", "warnings"):
            allowed_differences.append({"path": "stages[" + str(number) + "]." + key, "before": before[key], "after": after[key]})
            normalized["stages"][number][key] = before[key]
    require(normalized == original, "analysis differs outside explicitly allowed fields")
    for name, info in expected.items():
        path = moved_parent.joinpath(*PurePosixPath(name).parts)
        require(path.stat().st_size == info["size_bytes"] and sha(path) == info["sha256"], "raw member changed during audit: " + name)
    require(not forbidden_opens, "original root was read")
    result = {
        "schema_version": 1, "status": "passed", "recorded_at": datetime.now(timezone.utc).isoformat(),
        "purpose": "Actual unpack/move/offline audit of formal evidence; no services or measurements executed.",
        "command": [sys.executable, "-I", "-B", str(Path(__file__).resolve()), *old_argv[1:]],
        "script_sha256": sha(Path(__file__)), "temporary_directory": str(work), "relocated_root": str(moved),
        "forbidden_original_root": str(FORBIDDEN), "original_root_open_attempts": forbidden_opens,
        "offline_disallowed_actions": disallowed_actions,
        "collector": {"revision": REVISION, "git_archive_command": git_command,
                      "git_archive_exit_code": git_result.returncode, "files_sha256": collector_hashes},
        "archive": {"file": archive.name, "sha256": sha(archive), "size_bytes": archive.stat().st_size,
                    "members_verified": len(expected), "raw_bytes_verified": raw_bytes,
                    "all_members_regular_unique_safe_paths": True, "all_members_unchanged_after_audit": True},
        "attachments_verified": checked_attachments,
        "auditor": {"file": "audit_capacity.py", "sha256": sha(artifact / "audit_capacity.py"),
                    "logical_command": auditor_command, "execution": "runpy under same-process Python audit hook",
                    "exit_code": exit_code, "stdout": audit_stdout.getvalue(), "stderr": audit_stderr.getvalue(),
                    "distinct_files_opened": len(opened_paths)},
        "counts": relocated["counts"], "pairs": len(relocated["pairs"]), "comparisons": len(relocated["comparisons"]),
        "eligible_pairs_each_comparison": [group["eligible_pairs"] for group in relocated["comparisons"]],
        "classifications": dict(Counter(group["classification"] for group in relocated["comparisons"])),
        "rss_diagnostics": dict(Counter(group["engine_rss_diagnostic"] for group in relocated["comparisons"])),
        "binary_verification": dict(Counter(stage["binary_verification"] for stage in relocated["stages"])),
        "process_guard_verification": dict(Counter(stage["process_guard_verification"] for stage in relocated["stages"])),
        "comparison": {"rounds_exact_equal": True, "pairs_exact_equal": True, "comparisons_exact_equal": True,
                       "all_analysis_equal_after_explicit_allowances": True,
                       "allowed_differences": allowed_differences},
        "raw_package_and_supplied_analysis_modified": False,
        "scope_limit": "Manifest contents were used for member/attachment checks; the manifest itself is not self-hashed in this result. Validation fixture scripts were retained but not executed."
    }
    (artifact / "ISOLATED_RELOCATION_CHECK.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"status": result["status"], "temporary_directory": str(work),
                      "members_verified": len(expected), "counts": result["counts"],
                      "pairs": result["pairs"], "comparisons": result["comparisons"],
                      "original_root_open_attempts": len(forbidden_opens)}))


if __name__ == "__main__":
    main()
