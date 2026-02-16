# Summary: Containerized Run Isolation

Date: 2026-02-12

## Implemented

1. Added container execution config to `architectures/ralph_loop/arch.yaml`:
   - `execution.mode: container`
   - Docker runtime/image defaults
   - `network: none`
   - `use_bypass_permissions: true`
2. Added `harness/isolation_launcher.py`:
   - Resolves execution config from manifest + CLI overrides.
   - Builds strict bind-mount plan:
     - run dir RW at `/workspace/run`
     - data dir RO at `/workspace/data`
     - symlink-compatible RO mount for scaffolded absolute `run/data` symlink target
   - Verifies runtime and image availability.
   - Launches container worker and writes launcher audit events to run logs.
3. Updated `harness/cli.py`:
   - `run`/`resume` now support `--execution-mode`, runtime/network overrides, and bypass override.
   - Added hidden `worker` command for in-container execution to avoid recursion.
4. Updated `harness/ralph_loop.py`:
   - Added `permission_mode_override` in `RalphLoopConfig` and applies it to loaded state.
5. Added worker image definition:
   - `docker/claude-runner.Dockerfile`
6. Updated docs:
   - `README.md` now includes Docker build step and host-mode fallback.
7. Added tests:
   - `tests/test_isolation_launcher.py`
   - Added permission override smoke test in `tests/test_ralph_loop_runner_smoke.py`
8. Updated test config in `pyproject.toml`:
   - `pythonpath = ["."]`
   - `testpaths = ["tests"]`

## Verification

- `uv run pytest -q` -> pass (`13 passed`)
- `uv run ruff check harness tests` -> pass
- Docker integration smoke:
  - `docker build -f docker/claude-runner.Dockerfile -t kalshi-lab-claude-runner:latest .`
  - `uv run kalshi-lab scaffold --arch ralph_loop --problem kalshi --name isolation_smoke2`
  - `uv run kalshi-lab run --name 003_isolation_smoke2 --max-iterations 0`
  - `uv run kalshi-lab resume --name 003_isolation_smoke2`
  - verified `launcher.container.start`, `run.start`, `run.stop`, and `launcher.container.exit` events
  - removed temporary smoke run folders after validation

## Notes

- Docker is the default runtime path.
- Podman is wired as a runtime option (`--container-runtime podman`) but Docker is the documented default to keep setup simple.
