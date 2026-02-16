"""Smoke test for run loop startup/stop without API calls."""

from pathlib import Path

import yaml

from harness.ralph_loop import RalphLoopConfig, run_loop


def test_run_loop_stops_immediately_on_zero_max_iterations(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "001_smoke"
    run_dir.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "run": {
            "id": "001_smoke",
            "name": "smoke",
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
                    "model": "claude-sonnet-4-5-20250929",
                    "permission_mode": "acceptEdits",
                },
                "hyperparameters": {"max_iterations": 100},
                "limits": {"max_cost_usd": 25.0, "max_time_minutes": 240},
            },
        },
    }
    (run_dir / "run_manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False))

    state = run_loop(
        run_dir=run_dir,
        run_id="001_smoke",
        mode="run",
        cfg=RalphLoopConfig(max_iterations=0),
    )

    assert state.status == "stopped"
    assert state.last_stop_reason == "max_iterations_reached"
    assert (run_dir / "state.json").exists()
    assert (run_dir / "logs" / "events.jsonl").exists()


def test_run_loop_applies_permission_mode_override(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "002_smoke"
    run_dir.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "run": {
            "id": "002_smoke",
            "name": "smoke",
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
                    "model": "claude-sonnet-4-5-20250929",
                    "permission_mode": "acceptEdits",
                },
                "hyperparameters": {"max_iterations": 100},
                "limits": {"max_cost_usd": 25.0, "max_time_minutes": 240},
            },
        },
    }
    (run_dir / "run_manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False))

    state = run_loop(
        run_dir=run_dir,
        run_id="002_smoke",
        mode="run",
        cfg=RalphLoopConfig(max_iterations=0, permission_mode_override="bypassPermissions"),
    )

    assert state.status == "stopped"
    assert state.permission_mode == "bypassPermissions"
