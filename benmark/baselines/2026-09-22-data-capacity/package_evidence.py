#!/usr/bin/env python3
"""Package a finished or observed-interrupted experiment without altering raw state."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tarfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    root, destination = args.root.resolve(), args.destination.resolve()
    plan = json.loads((root / 'plan.json').read_text())
    execution = json.loads((root / 'execution.json').read_text())
    assert execution['plan_sha256'] == sha(root / 'plan.json')
    assert len(execution['stages']) <= len(plan['stages']) == 12
    if execution['status'] != 'complete':
        interruption = json.loads((root / 'interruption-observation.json').read_text())
        assert interruption['replacement_or_resume'] is False
        assert all(row['state_after'] in ('absent', 'Z') for row in interruption['cleanup'])
    assert not destination.exists(), 'Use a new evidence directory'
    files = []
    roots = ['pilot-A', 'smoke-C'] + [row['directory'] for row in plan['stages']]
    for name in roots:
        directory = root / name
        if not directory.exists():
            continue  # Missing stages remain explicit in plan and execution.
        for path in sorted(directory.rglob('*')):
            relative = path.relative_to(directory)
            if relative.parts[0] in ('binaries', 'data') or 'data' in relative.parts[:-1]:
                continue
            assert not path.is_symlink(), 'Do not follow archive symlinks'
            if path.is_file():
                files.append(path)
    names = ['plan.json', 'execution.json', 'run_plan.py', 'driver.log',
             'binary-provenance.json', 'cpp-build-provenance.json',
             'calibration-plan.json', 'calibration-result.json', 'pilot.log',
             'smoke-C.log', 'smoke-C-summary.json', 'full-regression.log',
             'post-run-binary-checks.json', 'interruption-observation.json']
    for stage in plan['stages']:
        names.extend([stage['directory'] + '.log', stage['directory'] + '-host-before.json'])
    for name in names:
        path = root / name
        if path.is_file():
            files.append(path)
    files = sorted(set(files))
    destination.mkdir(parents=True)
    archive = destination / 'data-capacity.tar.gz'
    with tarfile.open(archive, 'w:gz') as output:
        for path in files:
            output.add(path, arcname='data-capacity/' + str(path.relative_to(root)), recursive=False)
    for name in ('audit_capacity.py', 'analysis.json', 'analysis.csv', 'package_evidence.py'):
        shutil.copyfile(root / name, destination / name)
    manifest = {
        'schema_version': 1,
        'description': 'All produced artifacts from the 48-round formal plan, including its incomplete execution state and separate interruption observation; four A-only calibration and four C smoke rounds. Pilot/smoke excluded from comparisons. Raw artifacts omit data and executable files; derived analysis and scripts are separate attachments.',
        'raw_execution_status': execution['status'],
        'planned_stages': len(plan['stages']),
        'started_stages': len(execution['stages']),
        'collector_revision': plan['collector_revision'],
        'plan_sha256': sha(root / 'plan.json'),
        'driver_sha256': plan['driver_sha256'],
        'binary_validation_after_relocation': 'recorded_hashes_only',
        'archive': {'file': archive.name, 'sha256': sha(archive),
                    'size_bytes': archive.stat().st_size,
                    'raw_bytes': sum(path.stat().st_size for path in files),
                    'files': len(files)},
        'members': [{'path': 'data-capacity/' + str(path.relative_to(root)),
                     'size_bytes': path.stat().st_size, 'sha256': sha(path)} for path in files],
        'attachments': [{'file': name, 'sha256': sha(destination / name)}
                        for name in ('audit_capacity.py', 'analysis.json', 'analysis.csv', 'package_evidence.py')],
    }
    (destination / 'MANIFEST.json').write_text(json.dumps(manifest, indent=2) + '\n')
    (destination / 'SHA256SUMS').write_text(''.join(sha(path) + '  ' + path.name + '\n'
        for path in sorted(destination.iterdir()) if path.is_file() and path.name != 'SHA256SUMS'))
    print(json.dumps(manifest['archive'], indent=2))


if __name__ == '__main__':
    main()
