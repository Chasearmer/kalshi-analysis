"""Tests for run manifest helpers."""

from pathlib import Path

from harness import manifest


def test_write_and_load_run_manifest(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "001_test"
    run_dir.mkdir(parents=True)
    arch_source = tmp_path / "architectures" / "ralph_loop" / "arch.yaml"
    arch_source.parent.mkdir(parents=True)
    arch_source.write_text("name: ralph_loop\n")

    manifest_path = manifest.write_run_manifest(
        run_dir,
        run_id="001_test",
        run_name="test",
        problem="kalshi",
        architecture_name="ralph_loop",
        architecture_source=arch_source,
        architecture_config={"name": "ralph_loop", "limits": {"max_cost_usd": 25.0}},
    )
    assert manifest_path.exists()

    loaded = manifest.load_run_manifest(run_dir)
    assert loaded["run"]["id"] == "001_test"
    assert loaded["architecture"]["name"] == "ralph_loop"
    assert loaded["architecture"]["config"]["limits"]["max_cost_usd"] == 25.0


def test_resolve_run_prefers_existing_directory(tmp_path: Path) -> None:
    target = tmp_path / "custom_run"
    target.mkdir()
    ref = manifest.resolve_run(str(target))
    assert ref.run_dir == target.resolve()
    assert ref.run_id == "custom_run"


def test_resolve_run_in_runs_dir(tmp_path: Path, monkeypatch) -> None:
    runs_dir = tmp_path / "runs"
    run_dir = runs_dir / "123_sample"
    run_dir.mkdir(parents=True)
    monkeypatch.setattr(manifest, "RUNS_DIR", runs_dir)

    ref = manifest.resolve_run("123_sample")
    assert ref.run_dir == run_dir.resolve()
    assert ref.run_id == "123_sample"


def test_resolve_run_by_name_suffix(tmp_path: Path, monkeypatch) -> None:
    runs_dir = tmp_path / "runs"
    run_dir = runs_dir / "002_claude_ralph"
    run_dir.mkdir(parents=True)
    monkeypatch.setattr(manifest, "RUNS_DIR", runs_dir)

    ref = manifest.resolve_run("claude_ralph")
    assert ref.run_dir == run_dir.resolve()
    assert ref.run_id == "002_claude_ralph"


def test_resolve_run_by_name_suffix_ambiguous(tmp_path: Path, monkeypatch) -> None:
    runs_dir = tmp_path / "runs"
    (runs_dir / "002_claude_ralph").mkdir(parents=True)
    (runs_dir / "003_claude_ralph").mkdir(parents=True)
    monkeypatch.setattr(manifest, "RUNS_DIR", runs_dir)

    try:
        manifest.resolve_run("claude_ralph")
    except FileNotFoundError as e:
        assert "ambiguous" in str(e).lower()
        assert "002_claude_ralph" in str(e)
        assert "003_claude_ralph" in str(e)
    else:
        raise AssertionError("Expected ambiguous run name resolution to fail")
