# Plan: Fix Run Resolution and Root Permission Failure

Date: 2026-02-12

## Steps

1. Reproduce both failures locally with current code.
2. Patch run lookup in `harness/manifest.py`:
   - keep current behavior for explicit path/id
   - add suffix-based run-name match fallback
   - fail with clear error on ambiguity
3. Patch container launch in `harness/isolation_launcher.py`:
   - run container as host uid:gid (`--user UID:GID`) to avoid root/sudo restriction
4. Add tests:
   - resolve-by-suffix and ambiguity handling
   - launcher command includes user flag
5. Run `pytest` and lint.
6. Run real smoke test: scaffold + run (`max-iterations=0`) using short name.
7. Write feature `summary.md` with validation outcomes.

## Acceptance Criteria

- `uv run kalshi-lab run --name <suffix>` works for unique suffix.
- Container mode run no longer fails with root/sudo + bypass permission error.
- Regression tests cover both behaviors.
