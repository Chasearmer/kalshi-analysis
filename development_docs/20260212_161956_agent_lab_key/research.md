# Research: Support AGENT_LAB_ANTHROPIC_API_KEY

Date: 2026-02-12

## Scope

Use `AGENT_LAB_ANTHROPIC_API_KEY` as the credential input variable for containerized runs.

## Findings

1. Claude SDK expects `ANTHROPIC_API_KEY` in process env.
2. Current launcher forwards `ANTHROPIC_API_KEY` only; if only `AGENT_LAB_ANTHROPIC_API_KEY` is set, container startup cannot authenticate.
3. Safe approach is to map alias -> canonical env at launch time without logging secrets.

## Decision

Add runtime env mapping in launcher:

- If `ANTHROPIC_API_KEY` is unset and `AGENT_LAB_ANTHROPIC_API_KEY` is set, inject `ANTHROPIC_API_KEY` from alias for container process env.
- Ensure container receives `ANTHROPIC_API_KEY` in `--env` passthrough names.
- Do not log raw secret values.
