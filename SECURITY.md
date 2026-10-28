# Security Policy

## Supported Versions

Only the latest released version receives security fixes.

## Reporting a Vulnerability

Please do not open a public issue for security problems. Report them privately through
GitHub's Private Vulnerability Reporting:

1. Open the repository on GitHub.
2. Go to the **Security** tab.
3. Click **Report a vulnerability** and describe the issue.

Include the affected version, steps to reproduce, and the expected and actual behavior. You will
get an acknowledgement, and the report stays private until a fix is released.

## Scope

In scope:

- Bypasses of the idempotency guarantees, such as running a handler twice for one key or
  replaying a response for a different payload.
- Race conditions in `MemoryStore` or in the `Service` lock handling.
- Request-body handling that allows memory exhaustion beyond the configured limit.
- Information leaks in middleware error responses.

Out of scope:

- `MemoryStore` being used across several instances. This is documented as unsupported.
- Store implementations that are not atomic. The contract suite in `storetest` is the check for
  those.
