# EzVille Monitor 0.5.0

HAOS-local log collection and Monday 08:00 KST maintenance analysis.

- Original logs remain in HA `/share`; redacted archive and analysis remain in persistent `/data`.
- HA-authenticated ingress dashboard plus optional, independently authenticated read-only API for a HTTPS tunnel.
- Optional 22:00 KST daily EW11 report in HA notifications and an optional Companion App push.
- Supervisor default role and read-only `/share`; notifications use the separate HA Core API proxy.
- Optional OpenAI API analysis and GitHub draft PR publication, initially disabled.
- No master writes, automatic merge, HA update or physical device commands.
- Signed `/v1/daily` reads expose daily scheduling/delivery state and passive packet discovery evidence.
- External AI review runs separately; every repair requires owner review and explicit approval before merge/version publication.

See [setup and limitations](../monitoring/README.md). OpenAI and GitHub credentials are entered
by the account owner in masked HA configuration fields. OTP stays enabled.


## Elevator labels, car-entry investigation and storage (Monitor 0.7.0)

The operator confirmed `33:01:43:01` and `33:01:C3:03` as elevator-call-related
traffic. Reports identify them as elevator call/arrival-related signal and call
response, with `classification_basis=operator_confirmed_elevator_call`.
These structure signatures do not establish the payload's exact role, button
press count, or physical arrival. The existing exact-frame arrival detector
remains unchanged. Packet occurrence counts remain distinct from HA commands.

The signed read-only `POST /v1/packets` accepts only Unix-second `start` and `end`
for a past interval of at most one hour, within 90 days (actual archive retention
may be shorter). Query 21:30–22:00 and compare the 21:40–21:50 profiles with the
adjacent control intervals for a reported 21:45 car-entry notification. Returned
profiles contain full bucket counts, first/last times, bounded fingerprints,
classification and change counts. They never contain raw frames or log text.
Partially overlapping buckets are flagged and cannot give exact interval counts.
Queries are bounded to 2000 archive batches, 1024 profiles and a response budget;
`truncated`, missing-source, malformed-record and coverage fields must be checked.
An empty result is not proof that a car notification never occurred or that it
used this monitored bus. Add `/v1/packets` to a path-specific Cloudflare API
exception if that configuration otherwise excludes this endpoint.

`monitoring/read_remote.py packets --origin <HTTPS-origin> --start <epoch> --end
<epoch>` uses the same read key and response signature checks as other reads.
No read key grants actuation rights.

EW11 0.0.16 also accepts `car_entry` on its existing `<HA_TOPIC>/packet/mark`
MQTT topic (default `ezville/packet/mark`). Publish this observation label when
an entry notification is seen; it invokes only the bounded existing capture,
not a vehicle or elevator command. Its short 5-second-before/15-second-after
trace and 300-frame cap may miss traffic in a busy bus; later marking cannot
reconstruct an earlier event. A report of an approximate time is not an
HA-recorded marker.

The HA daily job measures storage at report generation, saves `summary.storage`
in its successful daily report, includes it in the HA notification, and exports
it via authenticated `/v1/daily`. Measurements cover all files matching the
configured source-log prefix (including rotations and stack diagnostics), the
monitor SQLite database/WAL/SHM, logical and reusable database pages, and source
and monitor filesystem total/used/free/available capacity. File sizes are bytes;
HA display uses MiB. Measurements export no filesystem paths or contents.
The previous successful day's snapshot supplies retained-size changes; the
first snapshot has no change baseline. Growth after rotation or cleanup is net
retained growth, not total log production. Filesystem values include other apps
and may describe the same filesystem: do not add their capacities together.
The archive retention budget is distinct from disk capacity and DB physical
size. Missing/inaccessible measurements must be reported as unavailable.
Existing saved reports are not retroactively given fabricated storage values.
