"""Container-based run isolation launcher for Ralph loop execution."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from harness.manifest import load_run_manifest
from harness.paths import LAB_ROOT
from harness.ralph_loop import RalphLoopConfig
from harness.run_logging import RunLogger

WORKER_RUN_DIR = "/workspace/run"
WORKER_DATA_DIR = "/workspace/data"
DEFAULT_CONTAINER_IMAGE = "kalshi-lab-claude-runner:latest"
PERMISSION_BYPASS = "bypassPermissions"
ANTHROPIC_API_KEY_ENV = "ANTHROPIC_API_KEY"
AGENT_LAB_ANTHROPIC_API_KEY_ENV = "AGENT_LAB_ANTHROPIC_API_KEY"
RUNNER_IMAGE_FINGERPRINT_LABEL = "com.kalshi_lab.runner_fingerprint"
RUNNER_DOCKERFILE = LAB_ROOT / "docker" / "claude-runner.Dockerfile"
RUNNER_FINGERPRINT_INPUTS = (
    RUNNER_DOCKERFILE,
    LAB_ROOT / "pyproject.toml",
    LAB_ROOT / "README.md",
    LAB_ROOT / "harness",
)
FINGERPRINT_EXCLUDED_DIR_NAMES = {"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"}
FINGERPRINT_EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


@dataclass(frozen=True)
class ExecutionOverrides:
    """CLI-level execution overrides."""

    mode: str | None = None
    runtime: str | None = None
    network: str | None = None
    use_bypass_permissions: bool | None = None


@dataclass(frozen=True)
class ExecutionConfig:
    """Effective execution configuration."""

    mode: str
    runtime: str
    image: str
    network: str
    use_bypass_permissions: bool


@dataclass(frozen=True)
class BindMount:
    """One bind mount specification."""

    source: Path
    target: str
    read_only: bool


@dataclass(frozen=True)
class ContainerLaunchSpec:
    """Resolved container launch plan for one worker execution."""

    runtime: str
    image: str
    network: str
    workdir: str
    mounts: list[BindMount]
    env_passthrough: list[str]
    env_assignments: dict[str, str]
    worker_args: list[str]
    run_as_user: str | None = None
    allocate_tty: bool = False

    def to_command(self, *, run_id: str) -> list[str]:
        """Build argv for runtime invocation."""
        cmd: list[str] = [self.runtime, "run", "--rm", "--interactive"]
        if self.allocate_tty:
            cmd.append("--tty")
        if self.network == "none":
            cmd.extend(["--network", "none"])
        if self.run_as_user:
            cmd.extend(["--user", self.run_as_user])

        for mount in self.mounts:
            rw_mode = "ro" if mount.read_only else "rw"
            cmd.extend(["--volume", f"{mount.source}:{mount.target}:{rw_mode}"])

        for key in self.env_passthrough:
            cmd.extend(["--env", key])
        for key, value in self.env_assignments.items():
            cmd.extend(["--env", f"{key}={value}"])

        cmd.extend(["--env", f"KALSHI_LAB_RUN_ID={run_id}"])
        cmd.extend(["--env", "KALSHI_LAB_WORKER=1"])
        cmd.extend(["--workdir", self.workdir, self.image])
        cmd.extend(self.worker_args)
        return cmd


def _read_execution_block(manifest: dict) -> dict:
    arch = manifest.get("architecture", {})
    cfg = arch.get("config", {}) if isinstance(arch, dict) else {}
    execution = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
    return execution if isinstance(execution, dict) else {}


def resolve_execution_config(run_dir: Path, overrides: ExecutionOverrides) -> ExecutionConfig:
    """Resolve execution config from manifest + CLI overrides."""
    manifest = load_run_manifest(run_dir)
    execution = _read_execution_block(manifest)
    agent = (
        manifest.get("architecture", {}).get("config", {}).get("agent", {})
        if isinstance(manifest, dict)
        else {}
    )
    provider = str(agent.get("provider", "")).lower() if isinstance(agent, dict) else ""
    container = execution.get("container", {})
    if not isinstance(container, dict):
        container = {}

    mode = str(execution.get("mode", "host"))
    runtime = str(container.get("runtime", "docker"))
    image = str(container.get("image", DEFAULT_CONTAINER_IMAGE))
    network = str(container.get("network", "none"))
    use_bypass = bool(container.get("use_bypass_permissions", False))

    if overrides.mode is not None:
        mode = overrides.mode
    if overrides.runtime is not None:
        runtime = overrides.runtime
    if overrides.network is not None:
        network = overrides.network
    elif provider == "claude" and network == "none":
        # Claude SDK needs outbound network access to initialize and query.
        network = "default"
    if overrides.use_bypass_permissions is not None:
        use_bypass = overrides.use_bypass_permissions

    if mode not in {"host", "container"}:
        raise RuntimeError(f"Invalid execution mode: {mode}")
    if runtime not in {"docker", "podman"}:
        raise RuntimeError(f"Invalid container runtime: {runtime}")
    if network not in {"none", "default"}:
        raise RuntimeError(f"Invalid container network mode: {network}")

    return ExecutionConfig(
        mode=mode,
        runtime=runtime,
        image=image,
        network=network,
        use_bypass_permissions=use_bypass,
    )


def _resolve_data_mounts(run_dir: Path) -> list[BindMount]:
    """Resolve data mounts so run-local `data/` access remains functional."""
    data_path = run_dir / "data"
    if not data_path.exists():
        return []

    source = data_path.resolve()
    mounts = [BindMount(source=source, target=WORKER_DATA_DIR, read_only=True)]

    # Scaffold currently creates an absolute symlink from run/data -> problems/<problem>/data.
    # Mirror that target path in container so existing symlink continues to work.
    if data_path.is_symlink():
        link_target = data_path.resolve()
        if str(link_target) != WORKER_DATA_DIR:
            mounts.append(BindMount(source=source, target=str(link_target), read_only=True))

    return mounts


def _passthrough_env_names(base_env: dict[str, str] | None = None) -> list[str]:
    """Environment variables to pass through to container worker."""
    env = base_env if base_env is not None else os.environ
    keys = {
        ANTHROPIC_API_KEY_ENV,
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_AUTH_TOKEN",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "ALL_PROXY",
    }
    for key in env:
        if key.startswith("ANTHROPIC_"):
            keys.add(key)

    # Accept AGENT_LAB_ANTHROPIC_API_KEY as an alias input variable and expose
    # canonical ANTHROPIC_API_KEY to the containerized worker.
    if AGENT_LAB_ANTHROPIC_API_KEY_ENV in env and ANTHROPIC_API_KEY_ENV not in env:
        keys.add(ANTHROPIC_API_KEY_ENV)

    return sorted([key for key in keys if key in env or key == ANTHROPIC_API_KEY_ENV])


def _runtime_env_for_container_launch(base_env: dict[str, str] | None = None) -> dict[str, str]:
    """Prepare process env used for launching the container command."""
    env = dict(base_env) if base_env is not None else dict(os.environ)
    if ANTHROPIC_API_KEY_ENV not in env and AGENT_LAB_ANTHROPIC_API_KEY_ENV in env:
        env[ANTHROPIC_API_KEY_ENV] = env[AGENT_LAB_ANTHROPIC_API_KEY_ENV]
    return env


def build_container_launch_spec(
    *,
    run_dir: Path,
    run_id: str,
    mode: str,
    loop_cfg: RalphLoopConfig,
    execution: ExecutionConfig,
) -> ContainerLaunchSpec:
    """Build container launch plan for this run mode."""
    if mode not in {"run", "resume"}:
        raise RuntimeError(f"Unsupported worker mode: {mode}")

    mounts: list[BindMount] = [BindMount(source=run_dir.resolve(), target=WORKER_RUN_DIR, read_only=False)]
    mounts.extend(_resolve_data_mounts(run_dir))
    runtime_home = run_dir / ".runtime_home"
    runtime_config = runtime_home / ".config"
    runtime_cache = runtime_home / ".cache"
    runtime_home.mkdir(parents=True, exist_ok=True)
    runtime_config.mkdir(parents=True, exist_ok=True)
    runtime_cache.mkdir(parents=True, exist_ok=True)

    run_as_user: str | None = None
    getuid = getattr(os, "getuid", None)
    getgid = getattr(os, "getgid", None)
    if callable(getuid) and callable(getgid):
        run_as_user = f"{getuid()}:{getgid()}"
    try:
        allocate_tty = sys.stdin.isatty() and sys.stdout.isatty()
    except Exception:  # noqa: BLE001 - conservative fallback to non-tty
        allocate_tty = False

    worker_args = [
        "worker",
        "--run-dir",
        WORKER_RUN_DIR,
        "--run-id",
        run_id,
        "--mode",
        mode,
    ]

    if mode == "run":
        if loop_cfg.max_cost_usd is not None:
            worker_args.extend(["--max-cost-usd", str(loop_cfg.max_cost_usd)])
        if loop_cfg.max_time_minutes is not None:
            worker_args.extend(["--max-time-minutes", str(loop_cfg.max_time_minutes)])
        if loop_cfg.max_tokens_total is not None:
            worker_args.extend(["--max-tokens-total", str(loop_cfg.max_tokens_total)])
        if loop_cfg.max_iterations is not None:
            worker_args.extend(["--max-iterations", str(loop_cfg.max_iterations)])
    else:
        if loop_cfg.extend_cost_usd:
            worker_args.extend(["--extend-cost-usd", str(loop_cfg.extend_cost_usd)])
        if loop_cfg.extend_time_minutes:
            worker_args.extend(["--extend-time-minutes", str(loop_cfg.extend_time_minutes)])
        if loop_cfg.extend_tokens_total:
            worker_args.extend(["--extend-tokens-total", str(loop_cfg.extend_tokens_total)])
        if loop_cfg.extend_iterations:
            worker_args.extend(["--extend-iterations", str(loop_cfg.extend_iterations)])
        if loop_cfg.new_session_from_checkpoint:
            worker_args.append("--new-session-from-checkpoint")

    if execution.use_bypass_permissions:
        worker_args.extend(["--permission-mode-override", PERMISSION_BYPASS])

    return ContainerLaunchSpec(
        runtime=execution.runtime,
        image=execution.image,
        network=execution.network,
        workdir=WORKER_RUN_DIR,
        mounts=mounts,
        env_passthrough=_passthrough_env_names(),
        env_assignments={
            "HOME": f"{WORKER_RUN_DIR}/.runtime_home",
            "XDG_CONFIG_HOME": f"{WORKER_RUN_DIR}/.runtime_home/.config",
            "XDG_CACHE_HOME": f"{WORKER_RUN_DIR}/.runtime_home/.cache",
        },
        worker_args=worker_args,
        run_as_user=run_as_user,
        allocate_tty=allocate_tty,
    )


def _verify_runtime(runtime: str) -> None:
    if shutil.which(runtime):
        return
    raise RuntimeError(
        f"Container runtime `{runtime}` not found. "
        "Install Docker Desktop (recommended) or set --execution-mode host."
    )


def _iter_fingerprint_files(path: Path) -> list[Path]:
    """Yield source files used for runner-image fingerprinting."""
    if path.is_file():
        return [path]

    files: list[Path] = []
    for candidate in sorted(path.rglob("*")):
        if not candidate.is_file():
            continue
        parts = set(candidate.parts)
        if parts.intersection(FINGERPRINT_EXCLUDED_DIR_NAMES):
            continue
        if candidate.suffix in FINGERPRINT_EXCLUDED_SUFFIXES:
            continue
        files.append(candidate)
    return files


def _compute_runner_image_fingerprint() -> str:
    """Compute deterministic fingerprint for the runner image inputs."""
    hasher = hashlib.sha256()
    for root in RUNNER_FINGERPRINT_INPUTS:
        if not root.exists():
            raise RuntimeError(f"Runner image input missing: {root}")
        for file in _iter_fingerprint_files(root):
            rel = file.relative_to(LAB_ROOT)
            hasher.update(str(rel).encode("utf-8"))
            hasher.update(b"\0")
            hasher.update(file.read_bytes())
            hasher.update(b"\0")
    return hasher.hexdigest()


def _inspect_image_labels(runtime: str, image: str) -> tuple[bool, dict[str, str]]:
    """Inspect image labels. Returns (exists, labels)."""
    inspect = subprocess.run(
        [runtime, "image", "inspect", image],
        capture_output=True,
        text=True,
        check=False,
    )
    if inspect.returncode != 0:
        return False, {}
    try:
        payload = json.loads(inspect.stdout)
    except json.JSONDecodeError:
        return True, {}
    if not isinstance(payload, list) or not payload:
        return True, {}
    raw_labels = payload[0].get("Config", {}).get("Labels", {})
    if not isinstance(raw_labels, dict):
        return True, {}
    labels: dict[str, str] = {}
    for key, value in raw_labels.items():
        labels[str(key)] = str(value)
    return True, labels


def _build_runner_image(runtime: str, image: str, fingerprint: str) -> subprocess.CompletedProcess[str]:
    """Build runner image and stamp with fingerprint label."""
    return subprocess.run(
        [
            runtime,
            "build",
            "-f",
            str(RUNNER_DOCKERFILE),
            "-t",
            image,
            "--label",
            f"{RUNNER_IMAGE_FINGERPRINT_LABEL}={fingerprint}",
            str(LAB_ROOT),
        ],
        check=False,
        text=True,
    )


def _ensure_fresh_image(runtime: str, image: str, logger: RunLogger) -> None:
    """Build image automatically when missing or stale."""
    desired_fingerprint = _compute_runner_image_fingerprint()
    image_exists, labels = _inspect_image_labels(runtime, image)
    current_fingerprint = labels.get(RUNNER_IMAGE_FINGERPRINT_LABEL)

    if image_exists and current_fingerprint == desired_fingerprint:
        logger.append_event(
            event_type="launcher.container.image.reuse",
            iteration=None,
            payload={
                "runtime": runtime,
                "image": image,
                "fingerprint": desired_fingerprint,
            },
        )
        return

    reason = "missing" if not image_exists else "stale"
    logger.append_event(
        event_type="launcher.container.image.build_start",
        iteration=None,
        payload={
            "runtime": runtime,
            "image": image,
            "reason": reason,
            "current_fingerprint": current_fingerprint,
            "desired_fingerprint": desired_fingerprint,
        },
    )

    result = _build_runner_image(runtime, image, desired_fingerprint)

    logger.append_event(
        event_type="launcher.container.image.build_finish",
        iteration=None,
        payload={
            "runtime": runtime,
            "image": image,
            "exit_code": result.returncode,
            "reason": reason,
            "desired_fingerprint": desired_fingerprint,
        },
    )
    if result.returncode != 0:
        raise RuntimeError(f"Container image build failed for `{image}` with exit code {result.returncode}")


def launch_container_worker(
    *,
    run_dir: Path,
    run_id: str,
    mode: str,
    loop_cfg: RalphLoopConfig,
    execution: ExecutionConfig,
) -> None:
    """Launch a run worker in an isolated container."""
    logger = RunLogger(run_dir)
    _verify_runtime(execution.runtime)
    _ensure_fresh_image(execution.runtime, execution.image, logger)

    spec = build_container_launch_spec(
        run_dir=run_dir,
        run_id=run_id,
        mode=mode,
        loop_cfg=loop_cfg,
        execution=execution,
    )
    logger.append_event(
        event_type="launcher.container.start",
        iteration=None,
        payload={
            "mode": mode,
            "runtime": spec.runtime,
            "image": spec.image,
            "network": spec.network,
            "mounts": spec.mounts,
            "run_as_user": spec.run_as_user,
            "allocate_tty": spec.allocate_tty,
            "env_passthrough": spec.env_passthrough,
            "env_assignments": spec.env_assignments,
            "worker_args": spec.worker_args,
        },
    )

    cmd = spec.to_command(run_id=run_id)
    runtime_env = _runtime_env_for_container_launch()
    result = subprocess.run(cmd, check=False, env=runtime_env)

    logger.append_event(
        event_type="launcher.container.exit",
        iteration=None,
        payload={"exit_code": result.returncode},
    )
    if result.returncode != 0:
        raise RuntimeError(f"Container worker failed with exit code {result.returncode}")
