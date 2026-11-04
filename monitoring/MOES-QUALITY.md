# MOES / MG24 quality collector

Version 0.7.4 implementation, not yet deployed to the user's NUC. Tests use explicit
fixtures; they are not new radio measurements. Existing production version and
automation remain unchanged.

## Operation

- Runs inside the existing EzVille Monitor HAOS add-on on the NUC, opt-in via
  `moes_quality_enabled: true` (default false).
- Uses Supervisor MQTT service discovery and its approved broker credentials.
  Credentials stay in memory and are never exported or logged. TLS-configured
  brokers fail closed until a verified TLS configuration is supplied.
- KST 00/06/12/18 slots, 15-minute startup grace; later startup records a missed
  window. Restart cannot repeat a claimed slot. Interrupted runs retain attempts.
- Exactly two hardcoded IEEE targets; resolves their network addresses from
  current `bridge/devices` metadata before every request.
- Requires `raw` in bridge definitions; endpoint 1 / cluster 0 / read attr 0 only.
  No IR, rejoin, channel, firmware, pairing, HA restart or arbitrary commands.
- 20 reads per target, alternating, two seconds after each completed attempt.
  ZCL timeout 8000 ms, outer MQTT deadline 10 s to receive bridge timeout errors.
- Verifies unique MQTT transaction, network address, endpoint, cluster, command,
  ZCL TSN, attribute success, uint8 type and value. Unknown response schemas fail
  closed. The deployment must validate its first real raw response against the
  installed Zigbee2MQTT 2.14.3 before claiming production readiness.
- Dedicated clean MQTT subscriber is disconnected after each run. Other clients
  and the existing parent-recovery extension are untouched.
- Parent/LQI samples come from the existing retained parent-recovery status;
  no status query starts a measurement. Both aggregate and per-device checkedAt
  must be at most 600 s old. Stale samples are explicitly unknown.
- Coordinator linkquality is not used. MAC/APS retries, continuous join/leave
  events and physical IR success are explicitly unmeasured.

## Persistence and retrieval

`/data/telemetry.sqlite` contains append-only `moes_runs` and `moes_attempts`.
Each attempt commits independently. No telemetry deletion or overwrite is added.
The existing add-on archive maintenance still owns overall archive management;
back up the add-on data and monitor disk usage for long-term retention.

Existing HMAC key, request nonce/time checks and response authentication are reused:

- Existing signed POST `/v1/report` includes `moes_quality`, preserving the
  already-used external path (no dependency on `/v1/operations`).
- Signed POST `/v1/moes-quality` with `{}` gives 28 latest run summaries, the
  newest run's raw attempts, and pooled 24h/7d statistics over those runs.
- Payload `{"slot":"2026-11-05T06:00:00+09:00"}` retrieves all original attempts
  of one archived slot, including older slots. It cannot trigger a measurement.
- `monitoring/read_remote.py moes-quality --slot ... --origin ... --key-file ...`
  validates response signatures using the existing secret file mechanism.

Success rate is based on sent/recorded attempts, not planned attempts. Missing
runs have null success percentage. Latency quantiles use successful responses
only; p95 is nearest rank. Run completeness must be checked before comparison.

## Deployment checklist (not executed)

1. Review/build this branch on HAOS with the existing Monitor slug/data volume.
   Do not replace HA Core or Zigbee2MQTT. Image adds pinned paho-mqtt 2.1.0.
2. Enable `moes_quality_enabled` in Monitor configuration, preserving all existing
   options and the existing `remote_secret`. Monitor add-on restart is needed;
   HA Core and Zigbee2MQTT restart are not needed.
3. Verify broker discovery, raw action capability and first scheduled run with
   real responses. Verify each device has 20 persisted attempts and inspect
   fresh parent proof. Confirm no physical commands were emitted.
4. Query the existing signed `/v1/report` and verify server signature/time and
   measured slot. Retrieve original attempts with the new endpoint if accessible.
5. Only then resume the ChatGPT report task as an API reader, without remote
   measurement triggers or dependence on a browser/Mac. Append downloaded runs
   to the existing history CSV keyed by slot + IEEE to avoid duplicates.

Local check: `python3 -m unittest discover -s monitoring/tests -p 'test*.py'`.

For commissioning, set `moes_quality_verification_id` to a unique 1–64 character ASCII alphanumeric/underscore/hyphen ID alongside enabling collection. Startup performs one explicitly labelled `verification:<id>` run, separate from scheduled slots. Reusing that ID cannot rerun it. Clear the option after verification. This remains the same fixed read-only benchmark, not an API trigger.
