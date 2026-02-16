# Research: Auto-Build Stale Container Image

Date: 2026-02-12

## Scope

Implement automatic Docker image rebuild when runner image is missing or stale, so manual `docker build` management is not required.

## Findings

1. Current launcher only checks image existence and errors if missing.
2. Staleness can be determined deterministically with a content fingerprint over build-relevant inputs.
3. Most robust source of truth is an image label embedded at build time (runtime-inspectable).
4. Rebuild flow can be runtime-agnostic (`docker` and `podman`) since both support `build`, `image inspect`, and labels.

## Proposed Fingerprint Inputs

- `docker/claude-runner.Dockerfile`
- `pyproject.toml`
- `README.md`
- `harness/**/*.py` and related source files (excluding cache artifacts)

## Decision

Use image-label fingerprinting:

- Compute desired fingerprint locally.
- Inspect image labels for current fingerprint.
- If missing/stale, run build automatically and label image with new fingerprint.
- Log build start/finish and reuse events to run logs.
