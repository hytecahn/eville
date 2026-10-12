#!/bin/sh
set -eu
# This script is installed from a reviewed revision outside the disposable checkout.
exec 9>/var/lib/ezville/weekly.lock
flock -n 9 || exit 0
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
git clone --quiet https://github.com/hytecahn/eville.git "$work/repo"
# Output is per-run so an old validated patch can never be published accidentally.
result="/var/lib/ezville/results/$(date -u +%Y%m%dT%H%M%S)"
python3 /opt/eville/monitoring/weekly.py --database /var/lib/ezville/telemetry.sqlite --repo "$work/repo" --output "$result"
# A separate publisher service/user consumes successful result directories.
printf '%s\n' "$result" > /var/lib/ezville/last-successful-result
