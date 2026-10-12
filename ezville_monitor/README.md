# EzVille Monitor 0.2.1

HAOS-local log collection and Monday 08:00 KST maintenance analysis.

- Original logs remain in HA `/share`; redacted archive and analysis remain in persistent `/data`.
- HA-authenticated ingress dashboard; no externally published monitor port.
- Supervisor default info-only role; `/share` mounted read only.
- Optional OpenAI API analysis and GitHub draft PR publication, initially disabled.
- No master writes, automatic merge, HA update or physical device commands.

See [setup and limitations](../monitoring/README.md). OpenAI and GitHub credentials are entered
by the account owner in masked HA configuration fields. OTP stays enabled.
