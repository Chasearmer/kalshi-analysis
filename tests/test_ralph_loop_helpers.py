"""Tests for ralph loop helper functions."""

from pathlib import Path

from harness.ralph_loop import (
    _extract_compact_metadata,
    _extract_token_total,
    _hook_event_names,
    _state_stop_check,
    _write_precompact_checkpoint,
)
from harness.run_state import RunLimits, RunState


def _sample_state() -> RunState:
    return RunState.initialize(
        run_id="001_test",
        architecture_name="ralph_loop",
        provider="claude",
        model="claude-sonnet",
        permission_mode="acceptEdits",
        limits=RunLimits(max_cost_usd=25.0, max_time_minutes=60, max_iterations=10),
    )


def test_extract_token_total_prefers_total_tokens() -> None:
    usage = {"total_tokens": 123, "input_tokens": 70, "output_tokens": 53}
    assert _extract_token_total(usage) == 123


def test_extract_token_total_sums_nested_when_no_total() -> None:
    usage = {"model": {"input_tokens": 20, "output_tokens": 30}, "other": [{"cache_tokens": 5}]}
    assert _extract_token_total(usage) == 55


def test_state_stop_check_by_cost() -> None:
    state = _sample_state()
    state.cumulative_cost_usd = 25.0
    should_stop, reason, _ = _state_stop_check(state)
    assert should_stop is True
    assert reason == "max_cost_reached"


def test_hook_event_names_precompact_only() -> None:
    assert _hook_event_names() == ("PreCompact",)


def test_extract_compact_metadata_from_compact_boundary_message() -> None:
    class _Message:
        subtype = "compact_boundary"
        data = {"compact_metadata": {"trigger": "auto", "pre_tokens": 12345}}

    assert _extract_compact_metadata(_Message()) == {"trigger": "auto", "pre_tokens": 12345}


def test_extract_compact_metadata_returns_none_for_non_compact_message() -> None:
    class _Message:
        subtype = "init"
        data = {}

    assert _extract_compact_metadata(_Message()) is None


def test_write_precompact_checkpoint_creates_latest_and_stamped(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "research" / "current_round").mkdir(parents=True)
    (run_dir / "research").mkdir(exist_ok=True)
    (run_dir / "research" / "findings.md").write_text("# Findings\nalpha\n")
    (run_dir / "research" / "open_questions.md").write_text("# Open\nbeta\n")
    (run_dir / "research" / "strategies.md").write_text("# Strategies\ngamma\n")
    (run_dir / "research" / "current_round" / "summary.md").write_text("# Summary\ndelta\n")

    state = _sample_state()
    checkpoint = _write_precompact_checkpoint(
        run_dir=run_dir,
        state=state,
        iteration=2,
        hook_input={"trigger": "auto"},
    )

    assert checkpoint.exists()
    latest = run_dir / "research" / "checkpoints" / "latest_precompact.md"
    assert latest.exists()
    assert "Iteration: 2" in latest.read_text()
