# Plan: Containerized Sandbox Isolation (Selected Approach)

Date: 2026-02-12

## Final Decision

Use an existing open-source container runtime (Docker first, Podman-compatible) as the hard isolation boundary, and build only a thin launcher in this repo.

- Do not build a custom sandbox engine.
- Do not rely on SDK policy controls alone for isolation.
- Keep Anthropic sandbox runtime as optional phase-2 defense-in-depth inside the container.

## Why This Choice

1. Meets strict requirement: run cannot access sibling runs if they are not mounted.
2. Low implementation risk: mature, battle-tested runtime.
3. Compatible with autonomous operation (`bypassPermissions`) once boundary is strict.
4. Resume/checkpoint model already maps cleanly to mounted run directory.

## Security Model

### Boundary 1 (hard): container mounts + network mode

- Mount exactly:
  - `runs/<run_id>` -> `/workspace/run` (rw)
  - dataset path -> `/workspace/data` (ro)
- Do not mount repo root, other `runs/*`, or home directory.
- Default network: disabled (`none`).

### Boundary 2 (soft): SDK controls inside container

- Permission mode:
  - container mode: allow `bypassPermissions` for unattended autonomy.
  - host mode: keep `acceptEdits`.
- Keep hooks/permission rules for extra guardrails and auditing.

### Boundary 3 (optional hardening): Anthropic sandbox runtime in-container

- Add `srt` wrapping for bash/subprocess tools if needed later.
- Treat as additive, not primary boundary.

## Architecture Updates

1. Add `execution` block in `architectures/ralph_loop/arch.yaml`:
   - `execution.mode`: `host | container` (default `container` for this architecture)
   - `execution.container.runtime`: `docker | podman`
   - `execution.container.image`
   - `execution.container.network`: `none | default`
   - `execution.container.use_bypass_permissions`: `true | false`
2. Add host launcher module:
   - resolves mounts
   - validates no disallowed host paths
   - spawns container worker
3. Keep in-container worker logic using existing Ralph loop.
4. Persist full logs/checkpoints in run folder only.

## Logging and Audit Requirements

1. Continue current per-run artifacts:
   - `logs/events.jsonl` (full stream events, including tool calls)
   - `logs/iterations.jsonl`
   - `state.json`
   - `research/checkpoints/*`
2. Add launcher audit events:
   - runtime/image
   - effective mount map
   - network mode
   - permission mode
   - container exit status

## Resume/Extend Behavior

1. Resume uses same `runs/<run_id>` mount in a new container process.
2. Worker loads `state.json`, latest checkpoint summary, and continues.
3. Extending limits (`--max-cost-usd`, time, iterations) updates run state before restart.

## Implementation Sequence

1. Config schema + defaults for execution isolation.
2. Container launcher + command builder (docker/podman).
3. CLI flags:
   - `--execution-mode`
   - `--container-runtime`
   - `--container-network`
   - `--use-bypass-permissions`
4. Worker wiring and environment handoff.
5. Launcher audit logging.
6. Tests:
   - command generation correctness
   - path validation (deny sibling-run mounts)
   - isolation probe (cannot read unmounted sibling run)
   - resume continuity and cumulative counters

## Acceptance Criteria

1. Agent cannot read any path outside mounted run/data paths.
2. Run works unattended with `bypassPermissions` in container mode.
3. Full event/tool-call history is retained in run logs.
4. Resume and extend budgets operate without losing continuity.

## Deferred / Not In Scope

1. gVisor/Firecracker integration.
2. Full domain allowlist orchestration for networked runs.
3. Multi-provider (Claude + Codex) unification in this specific isolation implementation step.
