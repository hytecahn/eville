# EzVille Monitor 0.1.0

Independent collection add-on for `simple_mqtt_ezville_control` v0.0.13 diagnostics.
It reads `/share` without write access, queries Supervisor info with the default role,
and sends signed batches outward over HTTPS. It never sends elevator or wallpad commands.

Setup: see [deployment guide](../monitoring/README.md). The required `target_slug` is the
**installed** add-on slug (including repository prefix), not necessarily the config slug.
Obtain it from the installed add-on's URL or `ha addons list`.

Supervisor endpoint availability must be checked on the actual HA version. Failed reads
are reported as unavailable, never healthy. Do not grant an admin role just to hide a 403.
Use the existing diagnostic file even if Supervisor info access is unavailable.

Set `shared_secret` to at least 32 random characters and use the same value at the receiver.
No HA login password, OTP seed, or long-lived user token is needed. Keep default protection
mode enabled. Default options intentionally cannot start a working external connection.
