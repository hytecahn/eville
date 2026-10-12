"""Separate publisher: run only after weekly.py succeeds, with GitHub App credentials."""
import argparse
import datetime as dt
from pathlib import Path
import subprocess
import tempfile


def run(args, cwd=None):
    return subprocess.run(args, cwd=cwd, check=True, text=True, capture_output=True, timeout=120).stdout.strip()


def main():
    p = argparse.ArgumentParser()
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument('--output', type=Path)
    source.add_argument('--output-from', type=Path)
    p.add_argument('--repository', default='hytecahn/eville')
    a = p.parse_args()
    out = (a.output or Path(a.output_from.read_text().strip())).resolve()
    if not (out / 'validated.patch').exists():
        print('No validated change; no PR or version bump.')
        return
    # Always apply to the exact analyzed commit. Never write master.
    with tempfile.TemporaryDirectory() as directory:
        run(['gh', 'repo', 'clone', a.repository, directory, '--', '--quiet'])
        base = (out / 'base-sha.txt').read_text().strip()
        latest = run(['git', 'rev-parse', 'origin/master'], directory)
        if latest != base:
            raise ValueError('master moved: rerun analysis and tests against the new head')
        branch = 'bot/ezville-weekly-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S')
        run(['git', 'checkout', '-b', branch, base], directory)
        run(['git', 'apply', '--check', str(out / 'validated.patch')], directory)
        run(['git', 'apply', str(out / 'validated.patch')], directory)
        run(['git', 'add', 'simple_mqtt_ezville_control'], directory)
        run(['git', '-c', 'user.name=EzVille maintenance bot', '-c', 'user.email=ezville-bot@users.noreply.github.com',
             'commit', '-m', 'fix: telemetry-backed weekly maintenance'], directory)
        run(['git', 'push', 'origin', branch], directory)
        print(run(['gh', 'pr', 'create', '--repo', a.repository, '--base', 'master', '--head', branch,
                   '--draft', '--title', 'EzVille weekly maintenance', '--body-file', str(out / 'pr-body.md')], directory))


if __name__ == '__main__':
    main()
