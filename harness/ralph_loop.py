"""Claude-first Ralph loop runner with resumable state and full event logging."""

from __future__ import annotations

import asyncio
import dataclasses
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from harness.manifest import load_run_manifest, utc_now_iso
from harness.run_logging import RunLogger
from harness.run_state import RunLimits, RunState, default_state_path, load_state, save_state

STATE_STOPPED = "stopped"
STATE_FAILED = "failed"
STATE_RUNNING = "running"


def _now() -> datetime:
    return datetime.now(UTC)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _read_file_tail(path: Path, max_lines: int = 40) -> str:
    if not path.exists():
        return "_missing_"
    lines = path.read_text(encoding="utf-8").splitlines()
    tail = lines[-max_lines:]
    return "\n".join(tail) if tail else "_empty_"


def _extract_token_total(usage: dict[str, Any] | None) -> int:
    """Best-effort token extraction from SDK usage payload."""
    if not usage:
        return 0

    for preferred in ("total_tokens", "totalTokens"):
        value = usage.get(preferred)
        if isinstance(value, (int, float)):
            return int(value)

    total = 0

    def walk(node: Any) -> None:
        nonlocal total
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, (int, float)) and "token" in key.lower():
                    total += int(value)
                else:
                    walk(value)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(usage)
    return total


def _extract_compact_metadata(message: Any) -> dict[str, Any] | None:
    """Extract compaction metadata from a SystemMessage if present."""
    if getattr(message, "subtype", None) != "compact_boundary":
        return None

    data = getattr(message, "data", None)
    if data is None:
        return {"trigger": "unknown"}

    metadata: Any
    if isinstance(data, dict):
        metadata = data.get("compact_metadata")
    else:
        metadata = getattr(data, "compact_metadata", None)

    if metadata is None:
        return {"trigger": "unknown"}
    if isinstance(metadata, dict):
        return metadata
    if dataclasses.is_dataclass(metadata):
        return dataclasses.asdict(metadata)
    if hasattr(metadata, "__dict__"):
        return dict(vars(metadata))
    return {"raw": str(metadata)}


def _state_stop_check(state: RunState) -> tuple[bool, str | None, str | None]:
    """Evaluate stop limits from state."""
    limits = state.limits
    if limits.max_iterations is not None and state.iteration >= limits.max_iterations:
        return True, "max_iterations_reached", f"iteration={state.iteration}"

    if limits.max_cost_usd is not None and state.cumulative_cost_usd >= limits.max_cost_usd:
        return (
            True,
            "max_cost_reached",
            f"cost_usd={state.cumulative_cost_usd:.4f}",
        )

    if limits.max_tokens_total is not None and state.cumulative_tokens >= limits.max_tokens_total:
        return True, "max_tokens_reached", f"tokens={state.cumulative_tokens}"

    minutes = state.cumulative_wall_time_seconds / 60.0
    if limits.max_time_minutes is not None and minutes >= limits.max_time_minutes:
        return True, "max_time_reached", f"minutes={minutes:.2f}"

    return False, None, None


def _build_system_prompt() -> str:
    return (
        "You are an autonomous quantitative research agent for Kalshi strategy discovery. "
        "You must iteratively investigate, implement, run analyses/backtests, and update "
        "persistent research memory files in this repository."
    )


def _build_iteration_prompt(iteration: int) -> str:
    return f"""\
Continue the iterative Kalshi research loop (iteration {iteration}).

Required steps this iteration:
1) Ask one concrete next research question from current evidence.
2) Implement or refine code required to answer it.
3) Run analysis/simulation commands and inspect outputs.
4) Save artifacts (figures/csv/text) in the run workspace.
5) Update persistent memory files:
   - research/findings.md
   - research/open_questions.md
   - research/strategies.md
   - research/current_round/summary.md
6) If a promising strategy is found/refined, update results/strategies.csv.
7) End with concise bullets:
   - Findings
   - Strategy implications
   - Next hypothesis

Constraints:
- Keep methods reproducible and deterministic where possible.
- Use the existing project code patterns and tests.
- Do not skip artifact updates.
"""


def _ensure_research_files(run_dir: Path) -> None:
    research_dir = run_dir / "research"
    current_round_dir = research_dir / "current_round"
    checkpoints_dir = research_dir / "checkpoints"
    for path in (research_dir, current_round_dir, checkpoints_dir, run_dir / "logs"):
        path.mkdir(parents=True, exist_ok=True)

    defaults = {
        research_dir / "findings.md": "# Findings\n\n",
        research_dir / "open_questions.md": "# Open Questions\n\n",
        research_dir / "strategies.md": "# Strategies\n\n",
        current_round_dir / "summary.md": "# Current Round Summary\n\n",
    }
    for path, content in defaults.items():
        if not path.exists():
            path.write_text(content, encoding="utf-8")


def _write_precompact_checkpoint(
    *,
    run_dir: Path,
    state: RunState,
    iteration: int,
    hook_input: dict[str, Any],
) -> Path:
    """Write checkpoint summary immediately before compaction."""
    checkpoint_dir = run_dir / "research" / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    stamp = _now().strftime("%Y%m%d_%H%M%S")
    path = checkpoint_dir / f"precompact_{stamp}.md"
    latest = checkpoint_dir / "latest_precompact.md"

    findings = _read_file_tail(run_dir / "research" / "findings.md")
    questions = _read_file_tail(run_dir / "research" / "open_questions.md")
    strategies = _read_file_tail(run_dir / "research" / "strategies.md")
    summary = _read_file_tail(run_dir / "research" / "current_round" / "summary.md")

    content = f"""\
# Pre-Compaction Checkpoint

- Timestamp: {utc_now_iso()}
- Run: {state.run_id}
- Iteration: {iteration}
- Session ID: {state.session_id or "_none_"}
- Trigger: {hook_input.get("trigger", "unknown")}
- Cumulative cost (USD): {state.cumulative_cost_usd:.6f}
- Cumulative tokens: {state.cumulative_tokens}

## Current Round Summary (tail)

```
{summary}
```

## Findings (tail)

```
{findings}
```

## Open Questions (tail)

```
{questions}
```

## Strategies (tail)

```
{strategies}
```
"""
    path.write_text(content, encoding="utf-8")
    latest.write_text(content, encoding="utf-8")
    return path


def _build_limits(manifest: dict[str, Any]) -> RunLimits:
    arch = manifest.get("architecture", {})
    cfg = arch.get("config", {}) if isinstance(arch, dict) else {}
    raw_limits = cfg.get("limits", {}) if isinstance(cfg, dict) else {}
    raw_hparams = cfg.get("hyperparameters", {}) if isinstance(cfg, dict) else {}
    return RunLimits(
        max_cost_usd=_as_float(raw_limits.get("max_cost_usd")),
        max_time_minutes=_as_int(raw_limits.get("max_time_minutes")),
        max_tokens_total=_as_int(raw_limits.get("max_tokens_total")),
        max_iterations=_as_int(raw_hparams.get("max_iterations")),
        max_turns_per_iteration=_as_int(raw_limits.get("max_turns_per_iteration")),
    )


def _state_from_manifest(run_id: str, manifest: dict[str, Any]) -> RunState:
    arch = manifest.get("architecture", {})
    cfg = arch.get("config", {}) if isinstance(arch, dict) else {}
    agent = cfg.get("agent", {}) if isinstance(cfg, dict) else {}
    return RunState.initialize(
        run_id=run_id,
        architecture_name=str(arch.get("name", "unknown")),
        provider=str(agent.get("provider", "claude")),
        model=agent.get("model"),
        permission_mode=agent.get("permission_mode"),
        limits=_build_limits(manifest),
    )


def _remaining_budget_usd(state: RunState) -> float | None:
    max_cost = state.limits.max_cost_usd
    if max_cost is None:
        return None
    return max(max_cost - state.cumulative_cost_usd, 0.0)


def _compute_cost_delta(result_total: float | None, state: RunState, result_session: str) -> float:
    if result_total is None:
        return 0.0
    if state.session_id and result_session == state.session_id:
        return max(result_total - state.session_total_cost_usd, 0.0)
    return max(result_total, 0.0)


def _result_stop_reason(result_subtype: str, is_error: bool) -> tuple[str | None, str | None]:
    if result_subtype == "error_max_budget_usd":
        return "sdk_max_budget_reached", result_subtype
    if result_subtype == "error_max_turns":
        return "sdk_max_turns_reached", result_subtype
    if is_error:
        return "sdk_error", result_subtype
    return None, None


def _hook_event_names() -> tuple[str, ...]:
    """SDK hook events we register for the run loop.

    We intentionally keep this minimal. Tool-call history is already captured from
    `sdk.message` events, and broad hook registration can cause repeated callback
    failures when a stream shuts down.
    """

    return ("PreCompact",)


@dataclass
class RalphLoopConfig:
    """Runtime options for running/resuming the loop."""

    max_cost_usd: float | None = None
    max_time_minutes: int | None = None
    max_tokens_total: int | None = None
    max_iterations: int | None = None
    new_session_from_checkpoint: bool = False
    extend_cost_usd: float = 0.0
    extend_time_minutes: int = 0
    extend_tokens_total: int = 0
    extend_iterations: int = 0
    permission_mode_override: str | None = None


class RalphLoopRunner:
    """Outer-loop orchestrator."""

    def __init__(self, run_dir: Path, run_id: str, mode: str, cfg: RalphLoopConfig) -> None:
        self.run_dir = run_dir
        self.run_id = run_id
        self.mode = mode
        self.cfg = cfg
        self.logger = RunLogger(run_dir)
        self.state_path = default_state_path(run_dir)
        self.manifest = load_run_manifest(run_dir)
        self.state = self._load_or_initialize_state()
        _ensure_research_files(run_dir)

    def _load_or_initialize_state(self) -> RunState:
        if self.state_path.exists():
            state = load_state(self.state_path)
        else:
            state = _state_from_manifest(self.run_id, self.manifest)

        if self.cfg.permission_mode_override is not None:
            state.permission_mode = self.cfg.permission_mode_override

        if state.provider != "claude":
            raise RuntimeError(
                f"Unsupported provider for this runner: {state.provider}. "
                "Only provider=claude is implemented in this phase."
            )

        if self.mode == "run":
            if state.iteration > 0 or state.session_id:
                raise RuntimeError("Run already has state. Use `resume` command to continue.")
            state.status = STATE_RUNNING
            state.started_at = state.started_at or utc_now_iso()
            self._apply_run_overrides(state)
            state.add_resume_event("run", {"kind": "fresh_start"})
        else:
            state.status = STATE_RUNNING
            state.started_at = state.started_at or utc_now_iso()
            self._apply_resume_overrides(state)

        save_state(self.state_path, state)
        return state

    def _apply_run_overrides(self, state: RunState) -> None:
        if self.cfg.max_cost_usd is not None:
            state.limits.max_cost_usd = self.cfg.max_cost_usd
        if self.cfg.max_time_minutes is not None:
            state.limits.max_time_minutes = self.cfg.max_time_minutes
        if self.cfg.max_tokens_total is not None:
            state.limits.max_tokens_total = self.cfg.max_tokens_total
        if self.cfg.max_iterations is not None:
            state.limits.max_iterations = self.cfg.max_iterations

    def _apply_resume_overrides(self, state: RunState) -> None:
        details: dict[str, Any] = {}
        if self.cfg.extend_cost_usd:
            if state.limits.max_cost_usd is None:
                state.limits.max_cost_usd = self.cfg.extend_cost_usd
            else:
                state.limits.max_cost_usd += self.cfg.extend_cost_usd
            details["extend_cost_usd"] = self.cfg.extend_cost_usd
        if self.cfg.extend_time_minutes:
            if state.limits.max_time_minutes is None:
                state.limits.max_time_minutes = self.cfg.extend_time_minutes
            else:
                state.limits.max_time_minutes += self.cfg.extend_time_minutes
            details["extend_time_minutes"] = self.cfg.extend_time_minutes
        if self.cfg.extend_tokens_total:
            if state.limits.max_tokens_total is None:
                state.limits.max_tokens_total = self.cfg.extend_tokens_total
            else:
                state.limits.max_tokens_total += self.cfg.extend_tokens_total
            details["extend_tokens_total"] = self.cfg.extend_tokens_total
        if self.cfg.extend_iterations:
            if state.limits.max_iterations is None:
                state.limits.max_iterations = state.iteration + self.cfg.extend_iterations
            else:
                state.limits.max_iterations += self.cfg.extend_iterations
            details["extend_iterations"] = self.cfg.extend_iterations

        if self.cfg.new_session_from_checkpoint:
            state.session_id = None
            state.session_total_cost_usd = 0.0
            details["new_session_from_checkpoint"] = True

        state.add_resume_event("resume", details=details)

    async def run(self) -> RunState:
        # Late import so non-run commands do not require SDK installation.
        try:
            from claude_agent_sdk import HookMatcher, query
            from claude_agent_sdk.types import ClaudeAgentOptions, ResultMessage
        except ImportError as e:
            raise RuntimeError(
                "claude-agent-sdk is required for ralph_loop execution. "
                "Install dependencies with `uv sync`."
            ) from e

        self.logger.append_event(
            event_type="run.start",
            iteration=self.state.iteration,
            payload={
                "mode": self.mode,
                "run_id": self.run_id,
                "limits": self.state.limits,
                "session_id": self.state.session_id,
            },
        )

        while True:
            should_stop, reason, detail = _state_stop_check(self.state)
            if should_stop:
                self.state.status = STATE_STOPPED
                self.state.stopped_at = utc_now_iso()
                self.state.last_stop_reason = reason
                self.state.last_stop_detail = detail
                save_state(self.state_path, self.state)
                self.logger.append_event(
                    event_type="run.stop",
                    iteration=self.state.iteration,
                    payload={"reason": reason, "detail": detail},
                )
                return self.state

            iteration = self.state.iteration + 1
            prompt = _build_iteration_prompt(iteration)
            started = _now()
            remaining_budget = _remaining_budget_usd(self.state)
            message_count = 0
            result_msg: ResultMessage | None = None

            async def on_hook(
                input_data: dict[str, Any], tool_use_id: str | None, context: dict[str, Any]
            ) -> dict[str, Any]:
                del context  # Unused hook context payload.
                hook_name = str(input_data.get("hook_event_name", "unknown"))
                try:
                    self.logger.append_event(
                        event_type=f"hook.{hook_name.lower()}",
                        iteration=iteration,
                        payload={
                            "hook_event_name": hook_name,
                            "tool_use_id": tool_use_id,
                            "input": input_data,
                        },
                    )
                    if hook_name == "PreCompact":
                        checkpoint_path = _write_precompact_checkpoint(
                            run_dir=self.run_dir,
                            state=self.state,
                            iteration=iteration,
                            hook_input=input_data,
                        )
                        self.logger.append_event(
                            event_type="checkpoint.precompact",
                            iteration=iteration,
                            payload={"path": str(checkpoint_path)},
                        )
                except Exception as hook_error:  # noqa: BLE001
                    # Hook failures should never tear down the main run loop.
                    self.logger.append_event(
                        event_type="hook.callback_error",
                        iteration=iteration,
                        payload={
                            "hook_event_name": hook_name,
                            "tool_use_id": tool_use_id,
                            "error": str(hook_error),
                        },
                    )
                return {"continue_": True}

            hook_matcher = HookMatcher(hooks=[on_hook])
            hooks = {event_name: [hook_matcher] for event_name in _hook_event_names()}

            options = ClaudeAgentOptions(
                cwd=self.run_dir,
                model=self.state.model,
                permission_mode=self.state.permission_mode,
                continue_conversation=True,
                resume=self.state.session_id,
                max_turns=self.state.limits.max_turns_per_iteration,
                max_budget_usd=remaining_budget,
                include_partial_messages=True,
                hooks=hooks,
                system_prompt=_build_system_prompt(),
            )

            self.logger.append_event(
                event_type="iteration.start",
                iteration=iteration,
                payload={
                    "remaining_budget_usd": remaining_budget,
                    "session_id": self.state.session_id,
                },
            )

            try:
                async for message in query(prompt=prompt, options=options):
                    message_count += 1
                    self.logger.append_event(
                        event_type="sdk.message",
                        iteration=iteration,
                        payload={
                            "message_type": type(message).__name__,
                            "message": message,
                        },
                    )
                    compact_metadata = _extract_compact_metadata(message)
                    if compact_metadata is not None:
                        checkpoint_path = _write_precompact_checkpoint(
                            run_dir=self.run_dir,
                            state=self.state,
                            iteration=iteration,
                            hook_input=compact_metadata,
                        )
                        self.logger.append_event(
                            event_type="checkpoint.precompact",
                            iteration=iteration,
                            payload={
                                "path": str(checkpoint_path),
                                "source": "system.compact_boundary",
                            },
                        )
                    if isinstance(message, ResultMessage):
                        result_msg = message
            except Exception as e:  # noqa: BLE001 - persist state for postmortem.
                self.state.status = STATE_FAILED
                self.state.stopped_at = utc_now_iso()
                self.state.last_stop_reason = "runtime_exception"
                self.state.last_stop_detail = str(e)
                save_state(self.state_path, self.state)
                self.logger.append_event(
                    event_type="run.error",
                    iteration=iteration,
                    payload={"error": str(e)},
                )
                raise

            if result_msg is None:
                self.state.status = STATE_FAILED
                self.state.stopped_at = utc_now_iso()
                self.state.last_stop_reason = "missing_result_message"
                self.state.last_stop_detail = "SDK stream ended without ResultMessage"
                save_state(self.state_path, self.state)
                self.logger.append_event(
                    event_type="run.error",
                    iteration=iteration,
                    payload={"error": self.state.last_stop_detail},
                )
                raise RuntimeError(self.state.last_stop_detail)

            elapsed_seconds = (_now() - started).total_seconds()
            cost_delta = _compute_cost_delta(
                result_msg.total_cost_usd, self.state, result_msg.session_id
            )
            token_delta = _extract_token_total(result_msg.usage)

            self.state.iteration = iteration
            self.state.session_id = result_msg.session_id
            if result_msg.total_cost_usd is not None:
                self.state.session_total_cost_usd = result_msg.total_cost_usd
            self.state.cumulative_cost_usd += cost_delta
            self.state.cumulative_tokens += token_delta
            self.state.cumulative_wall_time_seconds += elapsed_seconds
            self.state.last_result_subtype = result_msg.subtype

            self.logger.append_iteration(
                {
                    "iteration": iteration,
                    "session_id": result_msg.session_id,
                    "result_subtype": result_msg.subtype,
                    "is_error": result_msg.is_error,
                    "num_turns": result_msg.num_turns,
                    "messages_seen": message_count,
                    "duration_ms": result_msg.duration_ms,
                    "duration_api_ms": result_msg.duration_api_ms,
                    "elapsed_seconds_wall": elapsed_seconds,
                    "cost_delta_usd": cost_delta,
                    "cumulative_cost_usd": self.state.cumulative_cost_usd,
                    "token_delta": token_delta,
                    "cumulative_tokens": self.state.cumulative_tokens,
                }
            )
            save_state(self.state_path, self.state)

            stop_reason, stop_detail = _result_stop_reason(result_msg.subtype, result_msg.is_error)
            if stop_reason:
                self.state.status = (
                    STATE_STOPPED if stop_reason.startswith("sdk_max_") else STATE_FAILED
                )
                self.state.stopped_at = utc_now_iso()
                self.state.last_stop_reason = stop_reason
                self.state.last_stop_detail = stop_detail
                save_state(self.state_path, self.state)
                self.logger.append_event(
                    event_type="run.stop",
                    iteration=iteration,
                    payload={"reason": stop_reason, "detail": stop_detail},
                )
                return self.state


def run_loop(run_dir: Path, run_id: str, mode: str, cfg: RalphLoopConfig) -> RunState:
    """Synchronous wrapper for the async runner."""
    runner = RalphLoopRunner(run_dir=run_dir, run_id=run_id, mode=mode, cfg=cfg)
    return asyncio.run(runner.run())
