# Plan

1. Add helper to detect and extract compact metadata from SDK system messages.
2. In the main stream loop, write a checkpoint whenever `compact_boundary` is observed.
3. Log this with `checkpoint.precompact` and source `system.compact_boundary`.
4. Add unit tests for compact metadata extraction behavior.
5. Run tests and lint.
