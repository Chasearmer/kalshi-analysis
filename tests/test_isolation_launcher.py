"""Tests for container isolation launch planning."""

import json
import subprocess
from pathlib import Path

import yaml

import harness.isolation_launcher as isolation_launcher
from harness.isolation_launcher import (
    AGENT_LAB_ANTHROPIC_API_KEY_ENV,
    ANTHROPIC_API_KEY_ENV,
    PERMISSION_BYPASS,
    RUNNER_IMAGE_FINGERPRINT_LABEL,
    WORKER_DATA_DIR,
    WORKER_RUN_DIR,
    BindMount,
    ContainerLaunchSpec,
    ExecutionConfig,
    ExecutionOverrides,
    _passthrough_env_names,
    _runtime_env_for_container_launch,
    _ensure_fresh_image,
    build_container_launch_spec,
    resolve_execution_config,
)
from harness.ralph_loop import RalphLoopConfig
from harness.run_logging import RunLogger


def _write_manifest(run_dir: Path, *, execution: dict) -> None:
    manifest = {
        "schema_version": 1,
        "run": {
            "id": run_dir.name,
            "name": "test",
            "problem": "kalshi",
            "created_at": "2026-02-12T00:00:00Z",
            "run_dir": str(run_dir),
        },
        "architecture": {
            "name": "ralph_loop",
            "source": "architectures/ralph_loop/arch.yaml",
            "config": {
                "agent": {
                    "provider": "claude",
                    "model": "claude-opus-4-6",
                    "permission_mode": "acceptEdits",
                },
                "execution": execution,
            },
        },
    }
    (run_dir / "run_manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False))


def test_resolve_execution_config_uses_manifest_values(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "001_test"
    run_dir.mkdir(parents=True)
    _write_manifest(
        run_dir,
        execution={
            "mode": "container",
            "container": {
                "runtime": "docker",
                "image": "kalshi-lab-claude-runner:latest",
                "network": "none",
                "use_bypass_permissions": True,
            },
        },
    )

    config = resolve_execution_config(run_dir, ExecutionOverrides())
    assert config.mode == "container"
    assert config.runtime == "docker"
    assert config.network == "default"
    assert config.use_bypass_permissions is True


def test_resolve_execution_config_honors_cli_overrides(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "001_test"
    run_dir.mkdir(parents=True)
    _write_manifest(
        run_dir,
        execution={
            "mode": "container",
            "container": {
                "runtime": "docker",
                "image": "kalshi-lab-claude-runner:latest",
                "network": "none",
                "use_bypass_permissions": True,
            },
        },
    )

    config = resolve_execution_config(
        run_dir,
        ExecutionOverrides(
            mode="host",
            runtime="podman",
            network="default",
            use_bypass_permissions=False,
        ),
    )
    assert config.mode == "host"
    assert config.runtime == "podman"
    assert config.network == "default"
    assert config.use_bypass_permissions is False


def test_resolve_execution_config_allows_explicit_none_network_override(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "001_test"
    run_dir.mkdir(parents=True)
    _write_manifest(
        run_dir,
        execution={
            "mode": "container",
            "container": {
                "runtime": "docker",
                "image": "kalshi-lab-claude-runner:latest",
                "network": "default",
                "use_bypass_permissions": True,
            },
        },
    )

    config = resolve_execution_config(
        run_dir,
        ExecutionOverrides(
            network="none",
        ),
    )
    assert config.network == "none"


def test_build_container_launch_spec_mounts_run_and_data(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "001_test"
    run_dir.mkdir(parents=True)
    data_source = tmp_path / "problems" / "kalshi" / "data"
    data_source.mkdir(parents=True)
    (run_dir / "data").symlink_to(data_source.resolve())

    execution = ExecutionConfig(
        mode="container",
        runtime="docker",
        image="kalshi-lab-claude-runner:latest",
        network="none",
        use_bypass_permissions=True,
    )
    loop_cfg = RalphLoopConfig(max_cost_usd=5.0, max_iterations=2)
    spec = build_container_launch_spec(
        run_dir=run_dir,
        run_id="001_test",
        mode="run",
        loop_cfg=loop_cfg,
        execution=execution,
    )

    assert spec.runtime == "docker"
    assert spec.workdir == WORKER_RUN_DIR
    assert spec.mounts[0].source == run_dir.resolve()
    assert spec.mounts[0].target == WORKER_RUN_DIR
    assert spec.mounts[0].read_only is False
    assert any(m.target == WORKER_DATA_DIR and m.read_only for m in spec.mounts)
    assert any(m.target == str(data_source.resolve()) and m.read_only for m in spec.mounts)
    assert spec.run_as_user is not None
    assert spec.env_assignments["HOME"] == f"{WORKER_RUN_DIR}/.runtime_home"
    assert "--permission-mode-override" in spec.worker_args
    assert PERMISSION_BYPASS in spec.worker_args

    cmd = spec.to_command(run_id="001_test")
    assert cmd[0] == "docker"
    assert "--network" in cmd
    assert "none" in cmd
    assert "--user" in cmd
    assert f"HOME={WORKER_RUN_DIR}/.runtime_home" in cmd
    assert "worker" in cmd
    assert WORKER_RUN_DIR in cmd


def _event_types(run_dir: Path) -> list[str]:
    events_path = run_dir / "logs" / "events.jsonl"
    if not events_path.exists():
        return []
    return [json.loads(line)["event_type"] for line in events_path.read_text().splitlines() if line]


def test_ensure_fresh_image_reuses_when_fingerprint_matches(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    logger = RunLogger(run_dir)

    monkeypatch.setattr(
        isolation_launcher,
        "_compute_runner_image_fingerprint",
        lambda: "fp-current",
    )
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):  # noqa: ANN001
        del kwargs
        calls.append(cmd)
        labels = {"Config": {"Labels": {RUNNER_IMAGE_FINGERPRINT_LABEL: "fp-current"}}}
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout=json.dumps([labels]),
            stderr="",
        )

    monkeypatch.setattr(isolation_launcher.subprocess, "run", fake_run)
    _ensure_fresh_image("docker", "kalshi-lab-claude-runner:latest", logger)

    assert len(calls) == 1
    assert calls[0][:3] == ["docker", "image", "inspect"]
    assert _event_types(run_dir) == ["launcher.container.image.reuse"]


def test_ensure_fresh_image_builds_when_stale(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    logger = RunLogger(run_dir)

    monkeypatch.setattr(
        isolation_launcher,
        "_compute_runner_image_fingerprint",
        lambda: "fp-new",
    )
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):  # noqa: ANN001
        del kwargs
        calls.append(cmd)
        if cmd[:3] == ["docker", "image", "inspect"]:
            labels = {"Config": {"Labels": {RUNNER_IMAGE_FINGERPRINT_LABEL: "fp-old"}}}
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=0,
                stdout=json.dumps([labels]),
                stderr="",
            )
        if cmd[:2] == ["docker", "build"]:
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(isolation_launcher.subprocess, "run", fake_run)
    _ensure_fresh_image("docker", "kalshi-lab-claude-runner:latest", logger)

    assert len(calls) == 2
    build_cmd = calls[1]
    assert build_cmd[:2] == ["docker", "build"]
    assert "--label" in build_cmd
    assert f"{RUNNER_IMAGE_FINGERPRINT_LABEL}=fp-new" in build_cmd
    assert _event_types(run_dir) == [
        "launcher.container.image.build_start",
        "launcher.container.image.build_finish",
    ]


def test_ensure_fresh_image_builds_when_missing(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    logger = RunLogger(run_dir)

    monkeypatch.setattr(
        isolation_launcher,
        "_compute_runner_image_fingerprint",
        lambda: "fp-new",
    )
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):  # noqa: ANN001
        del kwargs
        calls.append(cmd)
        if cmd[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(args=cmd, returncode=1, stdout="", stderr="not found")
        if cmd[:2] == ["docker", "build"]:
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(isolation_launcher.subprocess, "run", fake_run)
    _ensure_fresh_image("docker", "kalshi-lab-claude-runner:latest", logger)

    assert len(calls) == 2
    assert calls[0][:3] == ["docker", "image", "inspect"]
    assert calls[1][:2] == ["docker", "build"]


def test_container_launch_spec_to_command_includes_tty_and_user() -> None:
    spec = ContainerLaunchSpec(
        runtime="docker",
        image="kalshi-lab-claude-runner:latest",
        network="default",
        workdir=WORKER_RUN_DIR,
        mounts=[BindMount(source=Path("/tmp/run"), target=WORKER_RUN_DIR, read_only=False)],
        env_passthrough=[],
        env_assignments={},
        worker_args=["worker", "--run-dir", WORKER_RUN_DIR, "--run-id", "001", "--mode", "run"],
        run_as_user="1000:1000",
        allocate_tty=True,
    )
    cmd = spec.to_command(run_id="001")
    assert "--tty" in cmd
    assert "--user" in cmd
    assert "1000:1000" in cmd


def test_passthrough_env_names_uses_alias_for_canonical_key() -> None:
    env = {
        AGENT_LAB_ANTHROPIC_API_KEY_ENV: "alias-key",
    }
    names = _passthrough_env_names(env)
    assert ANTHROPIC_API_KEY_ENV in names


def test_runtime_env_maps_alias_to_canonical_when_needed() -> None:
    env = {
        AGENT_LAB_ANTHROPIC_API_KEY_ENV: "alias-key",
    }
    runtime = _runtime_env_for_container_launch(env)
    assert runtime[ANTHROPIC_API_KEY_ENV] == "alias-key"


def test_runtime_env_keeps_existing_canonical_precedence() -> None:
    env = {
        AGENT_LAB_ANTHROPIC_API_KEY_ENV: "alias-key",
        ANTHROPIC_API_KEY_ENV: "canonical-key",
    }
    runtime = _runtime_env_for_container_launch(env)
    assert runtime[ANTHROPIC_API_KEY_ENV] == "canonical-key"
