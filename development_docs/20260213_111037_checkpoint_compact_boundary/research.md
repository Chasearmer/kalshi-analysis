# Research

## User report
- `research/checkpoints/` is empty on `005_claude_ralph`.

## Findings
- `runs/005_claude_ralph/research/checkpoints` exists but has no files.
- Run logs show compaction occurred:
  - `SystemMessage` with `subtype=status` and `status=compacting`
  - `SystemMessage` with `subtype=compact_boundary` and `compact_metadata.trigger=auto`
- Despite that, there were no `checkpoint.precompact` events in `events.jsonl`.

## Conclusion
- This is a real checkpointing gap, not just user expectation mismatch.
- PreCompact hook callbacks are not reliable enough as the sole source for checkpoint creation.

## Fix approach
- Add a fallback checkpoint trigger when SDK emits `SystemMessage(subtype=compact_boundary)`.
- Keep existing hook-based checkpoint logic, but make compact-boundary message handling authoritative fallback.
