# Research: Run Launch Failures

Date: 2026-02-12

## Reported Failures

1. `uv run kalshi-lab run --name claude_ralph` fails with `Run not found` after scaffolding `002_claude_ralph`.
2. `uv run kalshi-lab run --name 002_claude_ralph` fails in container with:
   - `--dangerously-skip-permissions cannot be used with root/sudo privileges`

## Initial Hypotheses

1. Run-name resolution currently supports full run id/path only, not suffix-by-run-name lookup.
2. Docker worker container runs as root by default; Claude SDK rejects bypass permissions when process is root.

## Desired Fixes

1. Support `--name <run_name_suffix>` lookup when run id is unknown but exactly one `runs/*_<suffix>` exists.
2. Run container worker as non-root user to allow bypass permission mode.
3. Add CLI error handling consistency and smoke tests for the two user-reported paths.
