#!/usr/bin/env python3
"""Recheck frozen identities, process exits and retained MiniKV databases.

No benchmark is launched and no database file is modified. Reports establish
checks performed at the recorded location/time only. Database directories are
omitted from repository archives; those archives alone cannot repeat --check.
Use --root for a relocated complete experiment and --source-root to relocate the
repository files named in the frozen plan (their hashes must still match).
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import subprocess
import sys


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def metadata(info):
    return {name: getattr(info, 'st_' + name) for name in
            ('dev', 'ino', 'mode', 'size', 'mtime_ns', 'ctime_ns')}


def data_image(directory):
    """Compare contents and metadata while allowing normal read-access updates."""
    image = {'directory': metadata(directory.stat()), 'files': {}}
    for path in sorted(directory.iterdir()):
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError('unexpected nonregular data entry: ' + str(path))
        entry = metadata(info)
        entry['sha256'] = digest(path)
        if metadata(path.lstat()) != metadata(info):
            raise RuntimeError('data entry changed while hashing: ' + str(path))
        image['files'][path.name] = entry
    if metadata(directory.stat()) != image['directory']:
        raise RuntimeError('data directory changed while hashing: ' + str(directory))
    return image


def current_process(pid):
    try:
        raw = Path('/proc/{}/stat'.format(pid)).read_text()
    except FileNotFoundError:
        return None
    # comm may contain spaces or parentheses. Fields after its final ')' begin
    # at field 3; Linux starttime is field 22.
    fields = raw[raw.rfind(')') + 2:].split()
    return {'state': fields[0], 'starttime_ticks': int(fields[19])}


def inspect_processes(case_dir, result):
    identities = {name: set() for name in ('engine', 'gateway', 'bench')}
    with (case_dir / 'resources.jsonl').open() as stream:
        for line in stream:
            for name, sample in json.loads(line)['processes'].items():
                if name in identities and sample.get('available'):
                    identities[name].add((sample['pid'], sample['starttime_ticks']))
    output = {}
    for name, samples in identities.items():
        recorded = result['processes'][name]
        pid = result['pids'][name]
        if len(samples) != 1 or next(iter(samples))[0] != pid or recorded['pid'] != pid:
            raise RuntimeError('{} process identity is missing or inconsistent: {!r}'.format(name, samples))
        _, starttime = next(iter(samples))
        observed = current_process(pid)
        output[name] = {
            'pid': pid, 'starttime_ticks': starttime,
            'forced': recorded['forced'], 'returncode': recorded['returncode'],
            'normal_exit': recorded['forced'] is False and recorded['returncode'] == 0,
            'current_proc_stat': observed,
            'original_process_absent': observed is None or observed['starttime_ticks'] != starttime,
        }
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--source-root', type=Path,
                        help='repository containing the frozen benmark source files')
    parser.add_argument('--output', type=Path, help='default: ROOT/analysis/post-run-checks.json')
    parser.add_argument('--timeout', type=float, default=60,
                        help='per --check subprocess timeout in seconds (default: 60)')
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error('--timeout must be finite and positive')
    root = args.root.resolve()
    output = args.output.resolve() if args.output else root / 'analysis/post-run-checks.json'
    report = {
        'schema_version': 1, 'status': 'running', 'started_at': now(),
        'execution_root': str(root), 'script_sha256': digest(__file__),
        'scope': ('Checks establish observations at the recorded location/time only. '
                  'Retained database directories are omitted from repository archives; '
                  'archive-only checkouts cannot rerun these storage checks. '
                  'Data contents/inode/mode/mtime/ctime are compared; read-access times are excluded. '
                  'Process absence is a current PID/starttime observation, not a historical boot identity proof.'),
        'plan': {}, 'frozen_files': [], 'stage_binary_copies': [], 'stages': [], 'cases': [], 'errors': [],
    }

    def require(condition, message, case=None):
        if not condition:
            report['errors'].append(message)
            if case is not None:
                case['errors'].append(message)

    def verify_file(path, expected, recorded_path=None):
        entry = {'path': str(path), 'recorded_path': recorded_path or str(path), 'expected_sha256': expected}
        try:
            entry['sha256'] = digest(path)
            entry['matches'] = entry['sha256'] == expected
            require(entry['matches'], 'SHA256 mismatch: ' + str(path))
        except Exception as error:
            entry.update(matches=False, error=repr(error))
            require(False, 'cannot hash {}: {!r}'.format(path, error))
        return entry

    try:
        plan = read_json(root / 'plan.json')
        execution = read_json(root / 'execution.json')
        original_root = Path(plan['engines']['A']).parent.parent
        original_source = Path(plan['collector']).parent.parent

        def relocate(recorded):
            path = Path(recorded)
            try:
                return root / path.relative_to(original_root)
            except ValueError:
                if args.source_root:
                    return args.source_root.resolve() / path.relative_to(original_source)
                return path

        plan_hash = digest(root / 'plan.json')
        declared_hash, declared_name = (root / 'PLAN_SHA256').read_text().split()
        report['plan'] = {'sha256': plan_hash, 'declared_sha256': declared_hash,
                          'execution_sha256': execution['plan_sha256'],
                          'execution_status': execution['status'],
                          'execution_json_sha256': digest(root / 'execution.json')}
        require(declared_name == 'plan.json' and plan_hash == declared_hash == execution['plan_sha256'],
                'plan hashes disagree')
        require(execution['status'] == 'complete', 'matrix execution did not complete')
        require(execution['controller_sha256'] == digest(root / 'run_plan.py'), 'controller hash mismatch')
        require(len(plan['stages']) == len(execution['stages']) == 8, 'expected exactly eight stages')
        require(len(plan['frozen_files']) == 10, 'expected ten frozen identities')
        frozen = {item['path']: item['sha256'] for item in plan['frozen_files']}
        require(len(frozen) == 10, 'duplicate frozen identity')
        for item in plan['frozen_files']:
            report['frozen_files'].append(verify_file(relocate(item['path']), item['sha256'], item['path']))
        # Never execute a binary whose frozen identity has failed verification.
        if report['errors']:
            raise RuntimeError('frozen plan preflight failed; storage checks were not run')

        for position, stage in enumerate(plan['stages']):
            stage_dir = root / stage['name']
            actual = execution['stages'][position]
            require(all(actual.get(key) == value for key, value in stage.items()),
                    stage['name'] + ': execution stage differs from plan')
            require(actual['status'] == 'complete' and actual['exit_code'] == 0,
                    stage['name'] + ': collector did not exit normally')
            manifest = read_json(stage_dir / 'manifest.json')
            index = read_json(stage_dir / 'index.json')
            names = ['r01-throughput-snapshot-off', 'r01-throughput-snapshot-on']
            require(index['status'] == 'complete' and index['successful_runs'] == 2 and index['failed_runs'] == 0,
                    stage['name'] + ': stage index is incomplete')
            require([item['name'] for item in index['runs']] == names,
                    stage['name'] + ': stage does not contain exactly OFF then ON')
            source_paths = {'engine': plan['engines'][stage['version']],
                            'gateway': plan['gateway'], 'bench': plan['bench']}
            stage_record = {'name': stage['name'], 'version': stage['version'],
                            'scenario': stage['scenario'], 'collector_exit_code': actual['exit_code'],
                            'manifest_sha256': digest(stage_dir / 'manifest.json'),
                            'index_sha256': digest(stage_dir / 'index.json')}
            report['stages'].append(stage_record)
            for role, source in source_paths.items():
                copied = manifest['executables'][role]
                expected_path = original_root / stage['name'] / 'binaries' / role
                require(Path(copied['path']) == expected_path and copied['sha256'] == frozen[source],
                        stage['name'] + ': recorded executable mismatch: ' + role)
                require(manifest['arguments'][role] == source and
                        manifest['metadata']['binaries'][role]['sha256'] == frozen[source],
                        stage['name'] + ': source executable provenance mismatch: ' + role)
                identity = verify_file(stage_dir / 'binaries' / role, frozen[source], copied['path'])
                identity.update(stage=stage['name'], role=role)
                report['stage_binary_copies'].append(identity)

            for name in names:
                case_dir = stage_dir / name
                case_name = stage['name'] + '/' + name
                case = {'name': case_name, 'status': 'running', 'errors': [], 'checks': []}
                report['cases'].append(case)
                try:
                    result = read_json(case_dir / 'result.json')
                    benchmark = read_json(case_dir / 'report.json')
                    settled = read_json(case_dir / 'stats-settled.json')['engine']
                    commands = read_json(case_dir / 'commands.json')
                    case['input_sha256'] = {filename: digest(case_dir / filename) for filename in
                                             ('result.json', 'report.json', 'stats-settled.json',
                                              'commands.json', 'resources.jsonl')}
                    require(result == next(item for item in index['runs'] if item['name'] == name),
                            case_name + ': result differs from index', case)
                    require(result['status'] == 'ok' and result['errors'] == [], case_name + ': failed run', case)
                    require(benchmark['complete'] is True and benchmark['outcomes']['failures'] == 0,
                            case_name + ': incomplete or failed benchmark', case)
                    expected_sequence = (benchmark['preload']['completed_keys'] +
                                         benchmark['operations']['put'] + benchmark['operations']['delete'])
                    expected = {'keys': 100000,
                                'data_bytes': {'primary': 102988890, 'stress': 13388890}[stage['scenario']],
                                'applied_sequence': expected_sequence}
                    case['expected'] = expected
                    case['sequence_inputs'] = {'preload_completed_keys': benchmark['preload']['completed_keys'],
                                                'put': benchmark['operations']['put'],
                                                'delete': benchmark['operations']['delete']}
                    require(benchmark['preload']['completed_keys'] == 100000 and benchmark['operations']['delete'] == 0,
                            case_name + ': workload no longer matches the frozen size model', case)
                    fields = tuple(expected) + ('durable_sequence', 'wal_pending_bytes', 'wal_inflight_bytes',
                                                'wal_queued_records', 'io_failed')
                    case['settled'] = {field: settled[field] for field in fields}
                    require(all(settled[field] == value for field, value in expected.items()) and
                            settled['durable_sequence'] == expected_sequence and
                            all(settled[field] == 0 for field in ('wal_pending_bytes', 'wal_inflight_bytes',
                                                               'wal_queued_records')) and settled['io_failed'] is False,
                            case_name + ': settled state differs from expected durable state', case)
                    case['processes'] = inspect_processes(case_dir, result)
                    for role, observation in case['processes'].items():
                        require(observation['normal_exit'], case_name + ': abnormal ' + role + ' exit', case)
                        require(observation['original_process_absent'], case_name + ': original ' + role + ' still exists', case)
                        require(commands[role]['argv'][0] == manifest['executables'][role]['path'],
                                case_name + ': command did not use frozen stage copy: ' + role, case)
                    data_dir = case_dir / 'data'
                    require(Path(commands['engine']['environment']['MINIKV_DATA_DIR']) == original_root / case_name / 'data',
                            case_name + ': data path differs from recorded command', case)
                    case['data_before'] = data_image(data_dir)
                    for version in ('A', 'B'):
                        command = [str(relocate(plan['engines'][version])), '--check']
                        environment = {'PATH': '/bin:/usr/bin', 'LANG': 'C', 'LC_ALL': 'C', 'TZ': 'UTC',
                                       'MINIKV_DATA_DIR': str(data_dir), 'MINIKV_MAX_DATA_BYTES': '0'}
                        check = {'version': version, 'command': command, 'environment': environment,
                                 'started_at': now(), 'timeout_seconds': args.timeout}
                        case['checks'].append(check)
                        try:
                            completed = subprocess.run(command, env=environment, cwd=root,
                                                       stdin=subprocess.DEVNULL, capture_output=True,
                                                       text=True, timeout=args.timeout)
                            check.update(exit_code=completed.returncode, stdout=completed.stdout,
                                         stderr=completed.stderr, finished_at=now())
                            parsed = json.loads(completed.stdout)
                            check['parsed_json'] = parsed
                            check['valid'] = (completed.returncode == 0 and parsed['status'] == 'valid' and
                                              parsed['schema_version'] == 1 and parsed['wal_trailing_bytes'] == 0 and
                                              all(parsed[field] == value for field, value in expected.items()))
                            require(check['valid'], case_name + ': frozen ' + version + ' storage check failed', case)
                        except Exception as error:
                            check.update(valid=False, error=repr(error), finished_at=now())
                            if isinstance(error, subprocess.TimeoutExpired):
                                check['stdout'] = (error.stdout or b'').decode(errors='replace')
                                check['stderr'] = (error.stderr or b'').decode(errors='replace')
                            require(False, case_name + ': frozen ' + version + ' check error: ' + repr(error), case)
                    case['data_after'] = data_image(data_dir)
                    case['data_unchanged'] = case['data_before'] == case['data_after']
                    require(case['data_unchanged'], case_name + ': database changed during checks', case)
                    require(case['checks'][0].get('parsed_json') == case['checks'][1].get('parsed_json'),
                            case_name + ': A and B disagree about stored data', case)
                except Exception as error:
                    require(False, case_name + ': ' + repr(error), case)
                case['status'] = 'fail' if case['errors'] else 'pass'
                print('{} {}'.format(case['status'].upper(), case_name), flush=True)

        for identity in report['frozen_files'] + report['stage_binary_copies']:
            identity['sha256_after_checks'] = digest(identity['path'])
            require(identity['sha256_after_checks'] == identity['expected_sha256'],
                    'executable/source identity changed during post-check: ' + identity['path'])
        require(digest(root / 'plan.json') == plan_hash, 'plan changed during post-check')
    except Exception as error:
        report['errors'].append(repr(error))

    checks = [check for case in report['cases'] for check in case['checks']]
    processes = [item for case in report['cases'] for item in case.get('processes', {}).values()]
    report['counts'] = {
        'stages': len(report['stages']), 'cases': len(report['cases']),
        'frozen_files': len(report['frozen_files']), 'stage_binary_copies': len(report['stage_binary_copies']),
        'storage_checks': len(checks), 'valid_storage_checks': sum(check.get('valid', False) for check in checks),
        'normal_process_exits': sum(item['normal_exit'] for item in processes),
        'original_processes_absent': sum(item['original_process_absent'] for item in processes),
        'unchanged_databases': sum(case.get('data_unchanged', False) for case in report['cases']),
    }
    for field, expected in {'stages': 8, 'cases': 16, 'frozen_files': 10, 'stage_binary_copies': 24,
                            'storage_checks': 32, 'valid_storage_checks': 32, 'normal_process_exits': 48,
                            'original_processes_absent': 48, 'unchanged_databases': 16}.items():
        require(report['counts'][field] == expected, '{}: expected {}, observed {}'.format(
            field, expected, report['counts'][field]))
    report['status'] = 'fail' if report['errors'] else 'pass'
    report['finished_at'] = now()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    print(json.dumps({'status': report['status'], 'counts': report['counts'], 'output': str(output)}), flush=True)
    return 0 if report['status'] == 'pass' else 1


if __name__ == '__main__':
    sys.exit(main())
