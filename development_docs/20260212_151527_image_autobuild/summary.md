# Summary: Fingerprint-Based Auto Image Build

Date: 2026-02-12

## Implemented

1. Added runner-image fingerprinting in `harness/isolation_launcher.py`.
   - Hash inputs:
     - `docker/claude-runner.Dockerfile`
     - `pyproject.toml`
     - `README.md`
     - `harness/` sources
   - Excludes cache artifacts (`__pycache__`, `.pyc`, etc.).
2. Added image label inspection and freshness detection.
   - Label key: `com.kalshi_lab.runner_fingerprint`
3. Added automatic build path:
   - If image missing or stale, launcher runs runtime build automatically.
   - Build stamps image with the current fingerprint label.
4. Added launcher telemetry events:
   - `launcher.container.image.reuse`
   - `launcher.container.image.build_start`
   - `launcher.container.image.build_finish`
5. Updated docs (`README.md`) to reflect that manual build is optional.

## Tests Added/Updated

- `tests/test_isolation_launcher.py`
  - fresh image -> reuse path
  - stale image -> build path
  - missing image -> build path

## Verification

1. `uv run pytest -q` -> pass (`16 passed`)
2. `uv run ruff check harness tests` -> pass
3. End-to-end smoke:
   - Scaffolding + `run --max-iterations 0` triggered auto-build event
   - Subsequent `resume` emitted image reuse event
   - Temporary smoke run directory removed after validation

## Result

Manual Docker build management is no longer required for normal usage. The launcher auto-builds only when needed and logs the decision path per run.
