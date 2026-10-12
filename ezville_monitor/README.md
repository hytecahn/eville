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
