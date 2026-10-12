# EzVille weekly monitoring: first implementation

## Implemented

- Separate HA collector add-on, read-only shared diagnostic files + Core/add-on info.
- SQLite file cursors/outbox, rotation handling, HTTPS/HMAC transport, duplicate acknowledgement.
- Local receiver behind a TLS proxy, 30-day retention and independent freshness check.
- Monday 08:00 Asia/Seoul systemd timer, missed-start catchup and one-job locking.
- Weekly observation completeness, grouped errors/watchdog/timeouts and bounded samples.
- Noninteractive read-only Codex analysis + constrained unified patch output.
- Existing source-only patch allowlist, regression container checks and patch version increment.
- Separate credentialed publisher creates a commit and **draft PR** against master.

## Not activated or implemented in this first PR

No service is installed on the user's HA or external host. There is no automatic merge,
HA update, rollback, notification destination, or claim of physical device success.
The existing release workflow can publish after a reviewed PR is merged; this PR does not
change its behavior. Final release/deployment gating must be added before auto-merge is enabled.
A generated patch can pass existing tests and still be wrong; draft review remains required.

This is a deployable starting pipeline, not a claim that the full self-healing system is live.

## Host and credentials

Use an always-on **external Linux host**, not the HA host, with Python 3.12, Docker, Git,
Codex CLI (supporting `exec`, `--output-schema`, read-only sandbox), gh CLI, systemd and a TLS proxy.
The analysis user must run in a disposable dedicated VM/host that has no HA credentials,
production-network access, SSH keys or GitHub write credentials. Docker access is a host-level
privilege; a sandbox alone does not protect an otherwise sensitive shared host.

AI auth should use a dedicated API credential/budget. Supply `CODEX_API_KEY` only to the Codex
invocation (or use the official secure API proxy setup); do not expose it to tests or publication.
Do not copy a personal auth.json into this public repository. Current run-weekly.sh assumes CLI
auth is provisioned externally for the analysis user. The Docker test command forwards no env secrets.
The publisher uses a separate `ezville-publisher` user/job and short-lived GitHub App installation credentials for
contents and PR writes, and no AI or HA secrets. Configure gh's git credential helper for HTTPS push.

The stored data is private. Never commit telemetry, environment files or generated AI output.
Regex redaction is defense in depth, not a guarantee of removing all sensitive text. Check a
local sample before enabling export. Stack dump files are excluded; watchdog logs are included.

## Deploy receiver

1. Install reviewed source at `/opt/eville`; keep it separate from disposable analysis checkouts.
2. Create service users `ezville-receiver` and `ezville-analysis` and `/var/lib/ezville`.
   Grant receiver write access and analysis read access to telemetry.sqlite and its SQLite WAL
   sidecar files. Use a shared group/default ACL; do not make the directory world-readable.
   Grant analysis write access to `/var/lib/ezville/results`, `weekly.lock`, and
   `last-successful-result`; keep telemetry owned by receiver.
3. `/etc/ezville/receiver.env` (root owned, 0600):
   ```ini
   EZVILLE_DB=/var/lib/ezville/telemetry.sqlite
   EZVILLE_SHARED_SECRET=<random value of at least 32 characters>
   PORT=8787
   ```
4. Route HTTPS `/ingest` to `http://127.0.0.1:8787/ingest` using your TLS proxy.
   Limit request size to 256 KiB, bound concurrent connections and apply rate limits.
   The receiver has no read/control endpoint and binds loopback only.
5. Install `ezville-receiver.service` in `/etc/systemd/system`, then start it:
   ```sh
   sudo systemctl daemon-reload
   sudo systemctl enable --now ezville-receiver.service
   ```

## Install HA collector (one-time user setup)

Refresh the existing `https://github.com/hytecahn/eville` add-on repository after this PR merges,
then install EzVille Monitor. Configure:

| Option | Value |
|---|---|
| log_path | Actual configured diagnostic_log_file of the ezville add-on |
| node_id | `home` |
| target_slug | Full installed ezville add-on slug |
| receiver_url | Your external HTTPS `/ingest` URL |
| shared_secret | Same random value as the receiver |
| poll_seconds | `60` |

Start the collector and confirm batches arrive. OTP remains enabled. No interactive login
is needed afterwards. Core/add-on snapshots reflect Supervisor-reported state only; MQTT/EW11
health is inferred from existing diagnostic messages. Calls/ACK correlation needs further instrumentation.

## Weekly analysis and publication

For a report without any AI invocation:

```sh
python3 /opt/eville/monitoring/weekly.py --database /var/lib/ezville/telemetry.sqlite --output /var/lib/ezville/results/manual-report
```

Install `ezville-weekly.service` and `.timer`, make `run-weekly.sh` executable,
and provision `/etc/ezville/analysis.env` (may be empty when CLI authentication is separately configured).
Create `/var/lib/ezville/results` and grant only the analysis user write access.
Enable the timer only after a manual invocation succeeds. It starts the analysis at Monday 08:00 KST;
completion depends on AI and build duration. A clean disposable clone and per-run output are mandatory.

```sh
sudo systemctl enable --now ezville-weekly.timer
systemctl list-timers ezville-weekly.timer
```

Install `ezville-publish.service` with a separately provisioned `ezville-publisher` user and
`/etc/ezville/publisher.env`. Grant that user read access only to successful result directories
and the result pointer. The weekly unit uses `OnSuccess` (systemd 249+) to trigger this separate
publisher only after analysis succeeds. To publish under that job's GitHub identity:

```sh
python3 /opt/eville/monitoring/publish.py --output /var/lib/ezville/results/<successful-run>
```

The supplied `OnSuccess` service automates this command once installed and configured.
It is intentionally not executed by the AI process.
No patch means no commit/version bump. Master moving during analysis blocks publication.
Incomplete observation windows (including the first seven days) block code changes, but still
produce an analysis. Receiver restart/data loss or log truncation must be investigated first.

The collector starts from the beginning of retained files. Initial catchup may take time; event
counts currently describe **collected** log lines, not occurrence-time-normalized weekly statistics.
Historical lines are identified by their original text timestamp. Max export throughput is 32 lines
per polling interval; increase collection throughput before enabling verbose packet logs.
Outbox exhaustion pauses collection, never silently discards it; source retention limits recovery.

## Independent stale-collector check

Install/enable `ezville-freshness.service` and `.timer` on the external host. A stale/unknown
stream exits 2 so systemd marks failure; connect `OnFailure` to your chosen alert mechanism.
This PR does not send emails/messages or configure a notification service.

```sh
python3 /opt/eville/monitoring/health.py --database /var/lib/ezville/telemetry.sqlite
```

A receiving collector is not proof that HA, MQTT or an elevator is healthy.

## Validation and later autonomous deployment

```sh
python3 -m unittest discover -s monitoring/tests -v
docker build -t ezville-monitor-test ezville_monitor
```

Before automatic merge/release/install: add a trusted required-check gate against the exact PR head,
independent review of packet/actuation changes, backup/config compatibility checks, deployment
health comparison and a tested rollback runner. Only then enable a specific auto-deployment policy.
