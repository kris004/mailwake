# Security policy

## Supported versions

`mailwake` is pre-1.0 and has no supported release line yet. Security fixes are
made against the current `main` branch. Older commits are not maintained.

## Reporting a vulnerability

Do not include vulnerability details, credentials, tokens, private mail data, or
machine-specific configuration in a public issue.

If private vulnerability reporting is enabled in the repository's **Security**
tab, use that flow. Otherwise, open a minimal public issue asking for a private
contact channel without describing the vulnerability. Do not send secrets
through a public issue.

If a report involves a real credential, revoke or rotate it first. The
repository cannot safely receive or store live credentials.

A useful report includes:

- the affected commit or version;
- the source type and relevant non-secret configuration shape;
- the expected and observed behavior;
- minimal reproduction steps using fake values;
- the security impact and any known workaround.

No response-time guarantee is offered before the first stable release, but
reports will be acknowledged and handled as privately as the available channel
allows.

## Security boundary

Configurations are trusted executable input. Configured commands, `state_cmd`,
and credential helpers run with the daemon user's privileges through `sh -c`.
Source payloads are not interpolated into command strings.

Credential-helper output is treated as secret and is never logged. Configured
command output follows its selected output policy and may enter logs. TLS and
certificate verification are enabled by default for IMAP; disabling either is an
explicit unsafe opt-in.
