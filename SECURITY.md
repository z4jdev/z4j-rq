# Security Policy

## Reporting a vulnerability

If you believe you have found a security vulnerability in `z4j-rq`,
**do not open a public GitHub issue**. Email `security@z4j.com` instead.

We acknowledge reports within **48 hours**, provide a preliminary assessment
within **5 business days**, and target fixes within **30 days** (**7 days** for
confirmed critical issues). Reporting timelines, safe harbor, supported-version
policy, and published advisories are maintained in the
[canonical z4j project security policy](https://github.com/z4jdev/z4j/blob/main/SECURITY.md).

## Security-critical surface

This adapter runs inside RQ workers with their Redis access. An authenticated
brain can submit, retry, cancel, purge, and requeue jobs. Callable/argument
validation, avoiding agent-side deserialization of broker-controlled pickles,
project-scoped bulk selection, confirm tokens, and worker event wrapping are
package-specific security surfaces; transport, redaction, and authorization
policy remain owned by `z4j-core` and the brain.
