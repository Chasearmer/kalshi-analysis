# Summary

## What changed
- Reduced SDK hook registration to `PreCompact` only in `harness/ralph_loop.py`.
- Added `_hook_event_names()` helper to make the hook scope explicit.
- Wrapped hook callback logic in defensive `try/except` and added structured `hook.callback_error` logging.
- Preserved pre-compaction checkpoint behavior.
- Kept full tool-call/event history via existing `sdk.message` logging.

## Why this fixes the issue
- Repeated `Error in hook callback ... Stream closed` was caused by high-volume hook callback traffic during stream shutdown.
- By removing non-essential hook registrations (tool/use lifecycle hooks), shutdown no longer fan-outs callback requests.

## Validation
- `uv run pytest -q` -> 24 passed.
- `uv run ruff check harness tests` -> all checks passed.
- Host-mode smoke run:
  - `uv run kalshi-lab run --name 006_claude_ralph --execution-mode host --max-iterations 1 --max-cost-usd 0.01`
  - Completed cleanly with budget stop and no repeated hook-callback stream-closed spam.

## Note
- Container smoke rerun was blocked by a Docker base-image metadata fetch timeout in this environment during rebuild. The behavioral fix itself is validated in host mode and by tests.
