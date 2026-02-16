# Plan

1. Reduce SDK hook registrations in `harness/ralph_loop.py` to `PreCompact` only.
2. Keep pre-compaction checkpoint behavior intact.
3. Add defensive hook callback error handling and structured `hook.callback_error` logging.
4. Add regression test to lock expected hook event list.
5. Run test suite (`pytest`) and lint (`ruff`).
6. Run a smoke execution to verify no repeated hook callback/stream-closed spam in normal operation.
