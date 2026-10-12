# HAOS-local EzVille weekly maintenance

The NUC keeps HAOS. EzVille Monitor reads existing `/share` diagnostic files and stores
redacted batches/cursors in its persistent `/data/telemetry.sqlite`. No external log server,
Linux host, remote monitor hostname, long-lived HA token, or OTP automation is required.

The collector snapshots Supervisor Core/add-on info every 60 seconds using the **default**
info-only role. It never sends device commands, restarts apps, or changes HA settings.
The ingress dashboard is authenticated by HA and only accepts the Supervisor ingress peer.
No host port is published; Cloudflare configuration is unchanged.

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
