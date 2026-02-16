# Summary: Fix Run Resolution and Container Launch Failures

Date: 2026-02-12

## Implemented

1. Run name resolution fallback in `harness/manifest.py`:
   - `--name claude_ralph` now resolves to `runs/002_claude_ralph` when unique.
   - Ambiguous suffixes now raise a clear error listing matching run ids.
2. Container worker runs as non-root user in `harness/isolation_launcher.py`:
   - Adds `--user <uid>:<gid>` to avoid Claude `--dangerously-skip-permissions` root restriction.
3. Claude network default adjustment:
   - For Claude provider, legacy manifests with `network: none` are upgraded to `default` unless explicitly overridden by CLI.
4. TTY behavior support:
   - Adds conditional `--tty` when running from an interactive terminal.
5. Cleaner CLI errors in `harness/cli.py`:
   - Run/resume name resolution failures now surface as `Error: ...` instead of full Python tracebacks.

## Tests Added/Updated

- `tests/test_manifest.py`
  - run-name suffix resolution
  - ambiguity handling
- `tests/test_isolation_launcher.py`
  - command includes user/tty
  - network override behavior

## Verification

1. `uv run pytest -q` -> pass
2. `uv run ruff check harness tests` -> pass
3. Runtime checks:
   - `uv run kalshi-lab run --name claude_ralph --max-iterations 0` resolves correctly.
   - root/sudo bypass error no longer appears (container logs show `run_as_user` set).
   - missing run now prints clean `Error: Run not found: ...` message.

## Notes

A separate auth/SDK initialization timeout can still occur without valid Claude credentials; this is handled by the follow-up env-var alias support feature.
