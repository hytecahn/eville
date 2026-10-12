# HAOS-local EzVille weekly maintenance

The NUC keeps HAOS. EzVille Monitor reads existing `/share` diagnostic files and stores
redacted batches/cursors in its persistent `/data/telemetry.sqlite`. No external log storage server, Linux host, long-lived HA token or OTP automation is required.
An optional monitor hostname is needed for independent external read access.

The collector snapshots Supervisor Core/add-on info every 60 seconds using the **default**
Supervisor role. The optional daily job uses the separate Home Assistant API proxy to
create notifications; it never sends device commands, restarts apps, or changes HA settings.
The ingress dashboard is authenticated by HA and only accepts the Supervisor ingress peer.
No host port is published. Independent external read access uses a separate internal
listener (8788) routed through a specifically configured HTTPS tunnel hostname.

## Daily EW11 report (optional)

Enable `daily_enabled` in EzVille Monitor to create a Home Assistant persistent notification
at 22:00 Asia/Seoul. It summarizes the **previous 24 hours** (yesterday 22:00 to today
22:00), not the incomplete calendar day. The full report is also shown on the HA-authenticated
monitor dashboard. For phone push, set `daily_notify_service` to the exact
`notify.mobile_app_<device>` action registered by the HA Companion App; leave it empty for
HA-only notifications. No report is sent to an external analytics provider.
Delivery runs about 1–2 minutes after 22:00 so the final packet bucket can be collected.
On first enablement, the monitor waits for the next 22:00 boundary rather than reporting
an earlier period it did not fully observe.

### Passive packet discovery

EzVille 0.0.14 profiles every checksum-valid incoming `F7` frame before device-specific
parsing. Every five minutes it writes exact counts, a structural signature
(`device:address:command:length`), known/unclassified status and up to three raw samples
per signature. A separate `/share/simple_mqtt_ezville_control.log.packet.log` rotates at
8 MiB with three backups (32 MiB maximum). More than 256 signatures in a bucket produces
an explicit overflow count. Other framing, checksum failures and traffic that never
reaches EW11 cannot be classified as valid frames. Ordinary polling is counted rather
than logged line by line. Light and thermostat state changes are observed state changes,
not necessarily physical button presses.

The daily report lists packet counts by device ID, light/thermostat state changes, new
signatures and unclassified signatures. The first day builds a baseline. Raw packet
samples remain on HA in the authenticated monitor dashboard and are excluded from
phone notifications and the public remote recent-logs endpoint. Protect HA backups
containing `/share` or `/data` accordingly.

To study a wallpad button, mark the experiment in HA **Settings > Tools > Actions**:

```yaml
action: mqtt.publish
data:
  topic: ezville/packet/mark
  payload: bell  # or door_open, other
  retain: false
```

Within 15 seconds press the physical button once. Repeat with and without the press.
The add-on retains at most 300 verified frames from five seconds before and fifteen
seconds after each marker, at most one marker per minute. The report lists candidate
signatures more frequent after the marker, with HA-only raw examples. These are
correlations to investigate, not established meanings. The marker never sends an
RS485 command; do not replay candidate frames onto the bus.

Update the EzVille runtime to 0.0.14 and the Monitor to 0.4.0 before relying on call
timings. New runtime lines have a machine-readable epoch timestamp; commands record a
single `elevator_call_sent` event when the first send is accepted locally, and the
existing arrival packet records an `elevator_arrival_signal` event. Retries do not count
as extra calls. The packet contains no request ID, so a signal is paired **only** if
exactly one call remains within 15 minutes. Even then elapsed time is an estimate,
not proof that the requested elevator arrived at the user's floor. Multiple outstanding
calls, missing signals and pre-upgrade lines are unconfirmed or incomplete.
The report separates EW11 reset attempts, completed resets, communication session
restarts and watchdog worker restarts. Error and timeout counts describe log lines,
not independent root causes. Collection gaps and historical logs without reliable event
timestamps mark the report incomplete; zeros then mean only "none observed".

Enabling `homeassistant_api` in the Monitor allows it to create notifications through the
Supervisor Core API proxy, a broader capability than its earlier info-only Supervisor
queries. This code sends only a fixed notification action and optionally one validated
mobile notification action. Daily reports remain on HA in `/data/telemetry.sqlite` and
are not returned by the public read-only API. A failed delivery is retried after an hour;
interrupted `running` reports need manual inspection to avoid duplicate phone pushes.

## Weekly job

Each Monday at **08:00 Asia/Seoul**, the local worker obtains the public master sources,
sends a bounded redacted report and sources to the OpenAI Responses API (`store: false`),
and keeps the Korean analysis/evidence in HA. OpenAI processes the submitted data; this is
not offline AI. Provider retention/billing policies still apply; ChatGPT Plus is not an API key.
The model has **no tools**. Output is parsed as exact source replacements and checked for
allowed paths, unique matches, deletion limits and Python syntax. Generated code is never
executed on HA. Network/API failures have at most three attempts per week, one hour apart.
An interrupted in-progress job requires inspection; it is not blindly re-billed after restart.
A start after Monday 08:00 catches up the current week only. HA power/network outage delays
analysis; this local-only configuration cannot independently detect a completely offline NUC.

When publication is enabled, a complete week of observations, running add-on and matching
installed/master version, causal evidence and no pending automated PR are required. The bot
creates a dedicated branch, source commit, candidate patch-version/changelog update and a
**draft PR**. It never writes master. PR text does not contain private analysis or raw logs.
GitHub tests/builds run on that candidate; syntax validation is not regression validation.
No automatic merge, release, install or rollback is enabled. Release remains the existing
workflow after reviewed merge. An invalid or unproven proposal has no version bump.

## One-time setup

1. Merge this reviewed PR after actual monitor test/build results succeed.
2. Refresh the existing add-on repository, install **EzVille Monitor**, leave protection on.
3. Set `log_path=/share/simple_mqtt_ezville_control.log` and
   `target_slug=8a83e751_simple_mqtt_ezville_control` (verified on the current HA).
4. Start with `weekly_enabled=false`, `publish_pr=false`; open the ingress dashboard and
   verify collection/versions/source presence. Enable boot start and execution watchdog.
5. The account owner enters an OpenAI API key into `openai_api_key` through HA's masked field.
   Set a project budget in the OpenAI account. Default model is `gpt-5.4`; configure an
   accessible Structured Outputs model if needed. API analysis is separately billed.
6. To publish PRs, enter a fine-grained GitHub token scoped **only to hytecahn/eville** with
   Contents read/write and Pull requests read/write (metadata read). No workflow/admin permission.
   For analysis only, leave this token blank; public source reads are unauthenticated.
7. Enable `weekly_enabled`, and optionally `publish_pr`, then restart the monitor to reload
   settings. A catch-up report runs if the current Monday deadline has passed.
   The first seven days are incomplete and cannot generate changes. Check **Open Web UI**
   for the local analysis/status. Credentials must never be pasted in chat or committed.

## Retention and limits

Redacted archive: 30 days, configurable 7–90; logical data budget: 256 MiB, configurable
32–1024. SQLite reuses freed pages; journal/index overhead and peak file size are additional.
Original ezville `/share` files are separate and retain the existing 90-day policy, with no
byte-size cap. Analysis summaries: 180 days. Removing the monitor can remove its `/data`;
include it in HA backups. Rotation/truncation is recorded; stack dump files are excluded.
Original occurrence times may differ from collection timestamps; initial historical catch-up
can inflate counts. A source gap/stale data is observation failure, never proof of health.

Redaction removes common credentials, IPv4/MAC addresses, but cannot guarantee removal of
all private strings. Only 30 bounded error examples are sent to AI; inspect the ingress report
before enabling API analysis. Public proposed code must be reviewed for confidential strings
and behavior as well as CI success. The AI's causality judgment alone is not a safety guarantee.

## Tests

`python -m unittest discover -s monitoring/tests -v`

`docker build -t ezville-monitor-test ezville_monitor`

GitHub monitor CI runs these tests and an actual Docker build. Existing EzVille resilience CI
validates proposed runtime changes. No real OpenAI call, billable analysis, GitHub bot token or
live physical device test is included in local tests.

## Independent remote reads (0.3.0)

This path is independent of the HA login session, OTP, OpenAI API credentials and GitHub
credentials. It is opt-in and **read only**. Original logs/database remain on HA; clients
receive bounded redacted excerpts or reports temporarily for analysis.

- `/v1/health`: collector freshness, stored batch/log-line counts and latest observed versions.
- `/v1/report`: seven-day bounded report, at most 30 redacted error examples.
- `/v1/recent`: at most 30 recent redacted log lines, scans at most the last 100 batches.
- `/v1/daily` (0.5.0): active daily configuration, enabled-since time, latest delivery
  status and the preceding 22:00 KST report. Before delivery, returns an explicitly
  identified preview. A saved report preserves that day's new-pattern classification.
  Includes counts, new/unknown signatures, first/last times and marker before/after
  candidate counts. Raw frames and call IDs remain local; samples become SHA-256
  fingerprints and byte lengths. Returns at most 128 prioritized patterns, the last
  20 markers and 30 calls, with original totals and a truncation flag.
- All data requests require POST with HMAC-SHA256 over method/path/time/nonce/body.
  Clock tolerance is 60 seconds. Nonces are rejected on reuse while this API process runs;
  a server restart resets the nonce cache. Replayed reads cannot perform writes.
- Responses are signed and marked no-store. No cookies, HA/Supervisor token or OTP is used.
  No raw file paths, arbitrary SQL, commands, writes or restart routes are accepted.
- Threads, body sizes and request rate are bounded. Unknown routes/unsigned requests
  expose no telemetry. Disabling `remote_enabled` or removing the tunnel route revokes access.

### One-time administrative installation

1. Update the monitor to 0.3.0. Account owner enters a random 32+ character
   `remote_secret` in the masked option; keep the matching key in an external secret store
   or a private local file (0600). Do not send it in chat, commit it or put it in URLs.
2. Enable `remote_enabled`, save and restart the monitor. It binds 8788 **inside** the
   container; it does not publish a LAN/host port or change Supervisor permissions.
3. Inspect the existing Cloudflared configuration first. For a locally managed tunnel,
   preserve all existing hosts and append:
   ```yaml
   additional_hosts:
     # Preserve all existing entries above this new entry.
     - hostname: ezville-monitor.septimarron.space
       service: http://8a83e751-ezville-monitor:8788
   ```
   The installed monitor container hostname must be verified before applying this value.
   Restart Cloudflared only after monitoring itself is ready; the restart can briefly
   interrupt remote HA access. For a remotely managed tunnel (`tunnel_token`), add this
   hostname/service in Cloudflare's dashboard; app-side additional_hosts are ignored.
   Do not remove the existing HA hostname or weaken HA's OTP protection.
4. The initial installation and tunnel setup require an authenticated administrator once.
   Subsequent **API reads** require only the monitor key, no HA login.
5. Configure a runner/client with the HTTPS origin and the key. This ChatGPT conversation
   does not provide a durable per-project secret vault: the key must be available to the
   execution environment or supplied through an approved external secret store. Installing
   the server alone does not give future conversations possession of that key.

### Acceptance test through the deployed HTTPS route

```sh
python monitoring/read_remote.py health --origin https://ezville-monitor.septimarron.space --key-file /private/monitor.key
python monitoring/read_remote.py recent --origin https://ezville-monitor.septimarron.space --key-file /private/monitor.key
```

The client has no HA auth code or cookie support. Check unsigned requests return no report.
Compare health snapshots over time: growing `batch_count` proves persisted collection
cycles; growing `log_line_count` or a new known source line proves **new log ingestion**.
Heartbeats alone are not new device log lines. Then restart only the monitor and repeat:
stored counts/cursors must survive and continue increasing without duplicated old log lines.

Local HTTP integration tests simulate a real source file append, independent authenticated
reads, heartbeat-only cycles, server/database restart and authentication failures. They are
not a substitute for testing the deployed HTTPS tunnel. Until the admin connection and
external key provisioning complete, mark the remote route **implemented, not operational**.

### Durable scheduled external check

The optional `EzVille independent collection check` GitHub workflow reads the signed health
API hourly (:17), outside the NUC, and returns success/failure without printing reports,
error samples, keys or private metrics. It requires neither OpenAI nor a GitHub write token.
After the deployed HTTPS acceptance test passes, configure repository Actions secrets
`EZVILLE_MONITOR_ORIGIN` and `EZVILLE_REMOTE_SECRET`, and set repository variable
`EZVILLE_REMOTE_CHECK_ENABLED=true`. Until then the workflow is disabled.
Use this secret store for the unattended runner; do not place the key in workflow YAML or
automation prompts. GitHub schedule timing is approximate. Configure your GitHub workflow
failure notification preference if alerts are wanted. This is an external collection
freshness check, not proof of device success or an AI code-repair executor.

## External daily AI review and owner approval

The requested external review is separate from `daily_enabled` and the optional
weekly OpenAI API worker. HA collects continuously and delivers its local daily
report after 22:01:30 KST; the external reviewer reads it at 22:05 KST using the
dedicated read-only key, without HA login or OTP.

Follow [daily review instructions](daily-review.md). Analyze unknown/new packet
signatures including possible doorbell and wallpad door-open events, lighting and
thermostat changes. Evidence is passive; meanings are hypotheses until repeated
marked experiments and control intervals distinguish them. Raw payload decoding
requires separately inspected HA-local samples; hashes cannot establish payload meaning.

Create a draft PR only for a reproducible repair supported by current data. Never
write master, merge, enable auto-merge, create a release, install an update or actuate
devices during the scheduled review. Owner review and explicit approval for the
specific PR are required before merge; a candidate version in the branch is not
a published version. Existing pending PRs must be checked to avoid duplicate repairs.

The unattended runner must have the key in its own approved secret store. A private
scratch file working today does not prove future scheduled executions can access it.
Report unavailable credentials/data as a blocked analysis, not as a healthy system.
