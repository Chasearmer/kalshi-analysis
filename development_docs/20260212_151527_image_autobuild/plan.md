# Plan: Auto Rebuild for Stale Runner Image

Date: 2026-02-12

## Implementation Steps

1. Add fingerprint utilities in `harness/isolation_launcher.py`:
   - deterministic hashing of selected inputs
   - cache-file exclusions (`__pycache__`, `.pyc`, etc.)
2. Add image inspection helper to read labels from existing runtime image.
3. Replace image existence check with `ensure_fresh_image(...)`:
   - if missing: auto build
   - if stale: auto build
   - if fresh: skip build
4. Add structured launcher events:
   - `launcher.container.image.reuse`
   - `launcher.container.image.build_start`
   - `launcher.container.image.build_finish`
5. Add tests for:
   - stale detection path
   - missing image path
   - fresh image path
6. Update README to reflect automatic build behavior.
7. Run tests and lint.

## Acceptance Criteria

- `run`/`resume` no longer require a manual build command in steady-state.
- Image rebuild happens automatically only when inputs changed or image missing.
- Build/reuse decisions are visible in run logs.
