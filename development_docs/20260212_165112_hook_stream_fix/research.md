# Research

## Problem observed
- Repeated terminal spam: `Error in hook callback hook_X` and `error: Stream closed`.
- This occurred many times during a single run, especially near stream shutdown.

## Evidence
- `harness/ralph_loop.py` registered one hook callback on six SDK hook events:
  - `UserPromptSubmit`
  - `PreToolUse`
  - `PostToolUse`
  - `PostToolUseFailure`
  - `PreCompact`
  - `Stop`
- Claude debug traces from run logs showed frequent hook execution around tool usage.
- Tool-call history is already fully available from `sdk.message` events, so broad hook registration was redundant for logging.

## Root cause hypothesis
- On stream shutdown (budget stop, interrupt, or other termination), many in-flight hook callbacks still attempt control requests.
- This causes repeated `Stream closed` hook callback errors from the SDK/runtime.
- The issue is amplified by high hook fan-out (multiple hook events per tool action).

## Fix direction
- Keep SDK hooks minimal: only register `PreCompact` (needed for pre-compaction checkpoint snapshots).
- Continue capturing tool calls from `sdk.message` event stream (already implemented).
- Add a defensive try/except in the hook callback to avoid callback failures bubbling up.
