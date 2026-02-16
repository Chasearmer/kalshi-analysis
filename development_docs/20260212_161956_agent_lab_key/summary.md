# Summary: Support AGENT_LAB_ANTHROPIC_API_KEY

Date: 2026-02-12

## Implemented

1. Added env alias support in `harness/isolation_launcher.py`:
   - Accept `AGENT_LAB_ANTHROPIC_API_KEY` as input.
   - Map to canonical `ANTHROPIC_API_KEY` for container launch env.
   - Ensure canonical key is included in container `--env` passthrough list.
2. Kept secret handling safe:
   - Only env var names are logged in events; values are never logged.
3. Updated docs in `README.md` with supported authentication variables.

## Tests Added

- `tests/test_isolation_launcher.py`
  - alias causes canonical passthrough
  - runtime env mapping alias -> canonical
  - canonical key takes precedence when both are present

## Verification

1. `uv run pytest -q` -> pass
2. `uv run ruff check harness tests` -> pass
3. Smoke command with alias only:
   - `env -u ANTHROPIC_API_KEY AGENT_LAB_ANTHROPIC_API_KEY=dummy uv run kalshi-lab resume --name 002_claude_ralph`
   - run logs show `env_passthrough` includes `ANTHROPIC_API_KEY`.

## Result

You can set only `AGENT_LAB_ANTHROPIC_API_KEY` in your shell profile; launcher maps it automatically for containerized runs.
