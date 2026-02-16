# Plan: AGENT_LAB_ANTHROPIC_API_KEY Support

Date: 2026-02-12

## Steps

1. Update `harness/isolation_launcher.py`:
   - Add alias constant.
   - Map alias to canonical env in runtime env builder.
   - Update env passthrough list logic to include canonical key when alias is available.
2. Add tests for env mapping and passthrough behavior.
3. Update README with accepted env var names.
4. Run tests and lint.
5. Run smoke validation with only alias env var set.
6. Write feature summary.

## Acceptance Criteria

- Runs work when only `AGENT_LAB_ANTHROPIC_API_KEY` is set.
- Secret values are not logged.
- Tests cover alias mapping path.
