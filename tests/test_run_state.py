"""Tests for persistent run state helpers."""

from pathlib import Path

from harness.run_state import RunLimits, RunState, load_state, save_state


def test_run_state_round_trip(tmp_path: Path) -> None:
    state = RunState.initialize(
        run_id="001_test",
        architecture_name="ralph_loop",
        provider="claude",
        model="claude-sonnet",
        permission_mode="acceptEdits",
        limits=RunLimits(max_cost_usd=25.0, max_iterations=10),
    )
    state.iteration = 3
    state.session_id = "session-abc"
    state.cumulative_cost_usd = 1.25
    state.cumulative_tokens = 999
    state.add_resume_event("resume", {"extend_cost_usd": 5.0})

    path = tmp_path / "state.json"
    save_state(path, state)
    loaded = load_state(path)

    assert loaded.run_id == "001_test"
    assert loaded.iteration == 3
    assert loaded.session_id == "session-abc"
    assert loaded.cumulative_cost_usd == 1.25
    assert loaded.cumulative_tokens == 999
    assert loaded.limits.max_cost_usd == 25.0
    assert len(loaded.resume_history) == 1
