"""Run manifest and architecture loading helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from harness.paths import ARCHITECTURES_DIR, RUNS_DIR

RUN_MANIFEST_NAME = "run_manifest.yaml"


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO8601 format."""
    return datetime.now(UTC).isoformat()


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML file as a dictionary."""
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in YAML file: {path}")
    return data


def architecture_file(arch: str) -> Path:
    """Resolve architecture yaml path from an architecture name."""
    arch_dir = ARCHITECTURES_DIR / arch
    if arch_dir.is_dir():
        return arch_dir / "arch.yaml"
    return ARCHITECTURES_DIR / f"{arch}.yaml"


def load_architecture(arch: str) -> tuple[Path, dict[str, Any]]:
    """Load architecture config by name."""
    arch_file = architecture_file(arch)
    if not arch_file.exists():
        raise FileNotFoundError(f"Architecture not found: {arch}")
    return arch_file, load_yaml(arch_file)


def write_run_manifest(
    run_dir: Path,
    *,
    run_id: str,
    run_name: str,
    problem: str,
    architecture_name: str,
    architecture_source: Path,
    architecture_config: dict[str, Any],
) -> Path:
    """Write run manifest with resolved architecture settings."""
    manifest = {
        "schema_version": 1,
        "run": {
            "id": run_id,
            "name": run_name,
            "problem": problem,
            "created_at": utc_now_iso(),
            "run_dir": str(run_dir),
        },
        "architecture": {
            "name": architecture_name,
            "source": str(architecture_source),
            "config": architecture_config,
        },
    }
    path = run_dir / RUN_MANIFEST_NAME
    path.write_text(yaml.safe_dump(manifest, sort_keys=False))
    return path


def load_run_manifest(run_dir: Path) -> dict[str, Any]:
    """Load run manifest from run directory."""
    path = run_dir / RUN_MANIFEST_NAME
    if not path.exists():
        raise FileNotFoundError(f"Run manifest not found: {path}. Scaffold a new run first.")
    return load_yaml(path)


@dataclass(frozen=True)
class RunRef:
    """Resolved run reference."""

    run_dir: Path
    run_id: str


def resolve_run(name: str) -> RunRef:
    """Resolve run by absolute path, relative path, or run id under runs/."""
    candidate = Path(name).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    if candidate.exists() and candidate.is_dir():
        return RunRef(run_dir=candidate.resolve(), run_id=candidate.name)

    run_dir = RUNS_DIR / name
    if run_dir.exists() and run_dir.is_dir():
        return RunRef(run_dir=run_dir.resolve(), run_id=run_dir.name)

    # Fallback: allow resolving by run name suffix, e.g. `claude_ralph` -> `002_claude_ralph`.
    suffix = f"_{name}"
    matches = sorted([d for d in RUNS_DIR.iterdir() if d.is_dir() and d.name.endswith(suffix)])
    if len(matches) == 1:
        match = matches[0].resolve()
        return RunRef(run_dir=match, run_id=match.name)
    if len(matches) > 1:
        options = ", ".join([d.name for d in matches])
        raise FileNotFoundError(f"Run name is ambiguous: {name}. Matching runs: {options}")

    raise FileNotFoundError(f"Run not found: {name}")
