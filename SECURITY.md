# Security Policy

## Supported Versions
The following versions of the StrategicAI Visibility Loop ETL project receive security updates:

| Version | Supported |
|----------|------------|
| main     | ✅ Active  |
| legacy   | ❌ No longer supported |

## Reporting a Vulnerability
We take security seriously. If you discover a vulnerability or data exposure risk:

1. **Do not open a public GitHub issue.**
2. Instead, email the maintainer directly at **security@strategicaileader.com** with:
   - A detailed description of the issue
   - Steps to reproduce
   - Potential impact and suggested mitigations

You can expect an acknowledgment within **48 hours**, and a resolution timeline within **7 days** depending on severity.

## Data & Access Integrity
This project processes analytics data and public visibility metrics only. It does **not** handle user credentials, PII, or private API keys.

Sensitive configuration files (e.g., API tokens, credentials, or private endpoints) should always be stored in:
- Environment variables (`.env`)
- Or cloud-secret managers (e.g., Fly.io Secrets, GitHub Actions Secrets)

Never commit secrets to version control.

## Responsible Disclosure
We encourage responsible disclosure. Please coordinate privately before publicizing any potential security issue to ensure users and systems remain protected.