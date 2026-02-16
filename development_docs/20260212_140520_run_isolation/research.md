# Research: Open-Source Sandbox Options for Run Isolation

Date: 2026-02-12

## Scope

Refine the isolation design for autonomous Ralph-loop execution:

1. Verify what `bypassPermissions` changes versus `acceptEdits`.
2. Compare open-source sandbox options for strict per-run isolation.
3. Decide build-vs-buy for this repo.

## Key Requirements

1. Hard guarantee that a run cannot read sibling `runs/*` folders.
2. Autonomous operation (minimal/no permission prompts).
3. Full run logging retained (events, tool calls, telemetry, checkpoints).
4. Practical local developer setup on macOS + Linux.

## Findings

### 1) Permission mode facts (Claude Agent SDK)

- `acceptEdits` auto-approves file edits/filesystem ops only.
- `bypassPermissions` auto-approves all tools; hooks still run.
- Anthropic docs explicitly say `bypassPermissions` should be used with extreme caution.

Implication:

- If we use `bypassPermissions`, we should only do it inside a strict sandbox boundary.

### 2) Anthropic sandbox runtime (`@anthropic-ai/sandbox-runtime`)

What it is:

- Open-source sandbox runtime from Anthropic.
- Uses OS primitives (`sandbox-exec`/Seatbelt on macOS, `bubblewrap` on Linux) plus proxy-based network controls.

Strengths:

- Purpose-built for agent workflows.
- Useful defense-in-depth layer for Claude tool execution.
- Supports filesystem + network policy controls.

Limitations relevant to our requirement:

- Project is explicitly marked as a beta research preview.
- Filesystem read policy is deny-list based by default (reads allowed everywhere unless denied), which is risky for strict “can only see run folder” guarantees if run directly on host.
- Still policy-driven; strictness depends on correct configuration.

Conclusion:

- Good additional layer, but not the sole boundary for run-level isolation.

### 3) Container runtime option (Docker/Podman)

What it gives:

- Strong practical isolation by mounting only explicit host paths into the container.
- Can run with `--network none`.
- Easy to make run folder RW and data folder RO only.

Relevant documented behaviors:

- Bind mounts are writable by default; must explicitly set `ro` where needed.
- Docker Desktop on macOS runs daemon in a Linux VM, and only shared host paths are mounted.

Conclusion:

- Best fit for strict per-run filesystem isolation with low implementation risk.

### 4) Other open-source options considered

- `gVisor`: strong extra isolation, but Linux-only requirement and higher runtime complexity.
- Firecracker/Kata: stronger VM boundary, but significantly heavier integration/ops.
- `nsjail`/raw `bubblewrap`: powerful, but we'd own the security policy correctness and platform variance ourselves.

Conclusion:

- Valuable future hardening options, not best first implementation for this repo.

## Decision: Build vs Buy

Decision:

- Buy existing runtime isolation (Docker/Podman) and build only a thin in-repo launcher/orchestrator.

Rationale:

1. Meets strict isolation requirement now.
2. Minimal custom security surface area.
3. Cross-platform enough for team workflow.
4. Keeps path open to future hardening (Anthropic sandbox runtime inside container, gVisor on Linux hosts).

## Recommended Stack (v1)

1. Primary boundary: per-run container with explicit mount allowlist.
2. Permission mode inside container: `bypassPermissions` allowed (autonomy) because boundary is external and strict.
3. Defense-in-depth: keep deny hooks/rules and optionally add Anthropic sandbox runtime in-container later.
4. Network default: `none` (opt-in network by config only).

## Source Links

- Claude SDK permissions: https://docs.claude.com/en/docs/claude-code/sdk/sdk-permissions
- Claude sandboxing docs: https://docs.claude.com/en/docs/claude-code/sandboxing
- Anthropic engineering post (sandboxing): https://www.anthropic.com/engineering/claude-code-sandboxing
- Anthropic sandbox runtime repo: https://github.com/anthropics/sandbox-runtime
- Docker bind mounts: https://docs.docker.com/engine/storage/bind-mounts/
- Docker `--network none`: https://docs.docker.com/engine/network/drivers/none/
- Podman run networking: https://docs.podman.io/en/latest/markdown/podman-run.1.html
- bubblewrap repo docs: https://github.com/containers/bubblewrap
- gVisor install/faq: https://gvisor.dev/docs/user_guide/install/ and https://gvisor.dev/docs/user_guide/faq/
- Firecracker docs: https://firecracker-microvm.github.io/
