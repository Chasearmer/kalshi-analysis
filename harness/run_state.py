"""Persistent run state for resumable Ralph loop execution."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STATE_FILE_NAME = "state.json"


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO8601 format."""
    return datetime.now(UTC).isoformat()


@dataclass
class RunLimits:
    """Stop limits for the outer loop."""

    max_cost_usd: float | None = None
    max_time_minutes: int | None = None
    max_tokens_total: int | None = None
    max_iterations: int | None = None
    max_turns_per_iteration: int | None = None


@dataclass
class ResumeEvent:
    """A resume or restart event."""

    timestamp: str
    mode: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunState:
    """Persistent, append-safe state for long-running loop execution."""

    schema_version: int
    run_id: str
    architecture_name: str
    provider: str
    model: str | None
    permission_mode: str | None
    status: str
    created_at: str
    updated_at: str
    started_at: str | None
    stopped_at: str | None
    iteration: int
    session_id: str | None
    session_total_cost_usd: float
    cumulative_cost_usd: float
    cumulative_tokens: int
    cumulative_wall_time_seconds: float
    last_result_subtype: str | None
    last_stop_reason: str | None
    last_stop_detail: str | None
    limits: RunLimits
    resume_history: list[ResumeEvent] = field(default_factory=list)

    @classmethod
    def initialize(
        cls,
        *,
        run_id: str,
        architecture_name: str,
        provider: str,
        model: str | None,
        permission_mode: str | None,
        limits: RunLimits,
    ) -> "RunState":
        """Construct a fresh run state."""
        now = utc_now_iso()
        return cls(
            schema_version=1,
            run_id=run_id,
            architecture_name=architecture_name,
            provider=provider,
            model=model,
            permission_mode=permission_mode,
            status="initialized",
            created_at=now,
            updated_at=now,
            started_at=None,
            stopped_at=None,
            iteration=0,
            session_id=None,
            session_total_cost_usd=0.0,
            cumulative_cost_usd=0.0,
            cumulative_tokens=0,
            cumulative_wall_time_seconds=0.0,
            last_result_subtype=None,
            last_stop_reason=None,
            last_stop_detail=None,
            limits=limits,
        )

    def as_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunState":
        """Deserialize from dictionary."""
        limits_raw = data.get("limits", {})
        resume_raw = data.get("resume_history", [])
        return cls(
            schema_version=int(data.get("schema_version", 1)),
            run_id=str(data["run_id"]),
            architecture_name=str(data["architecture_name"]),
            provider=str(data.get("provider", "claude")),
            model=data.get("model"),
            permission_mode=data.get("permission_mode"),
            status=str(data.get("status", "initialized")),
            created_at=str(data["created_at"]),
            updated_at=str(data.get("updated_at", data["created_at"])),
            started_at=data.get("started_at"),
            stopped_at=data.get("stopped_at"),
            iteration=int(data.get("iteration", 0)),
            session_id=data.get("session_id"),
            session_total_cost_usd=float(data.get("session_total_cost_usd", 0.0)),
            cumulative_cost_usd=float(data.get("cumulative_cost_usd", 0.0)),
            cumulative_tokens=int(data.get("cumulative_tokens", 0)),
            cumulative_wall_time_seconds=float(data.get("cumulative_wall_time_seconds", 0.0)),
            last_result_subtype=data.get("last_result_subtype"),
            last_stop_reason=data.get("last_stop_reason"),
            last_stop_detail=data.get("last_stop_detail"),
            limits=RunLimits(
                max_cost_usd=(
                    float(limits_raw["max_cost_usd"])
                    if limits_raw.get("max_cost_usd") is not None
                    else None
                ),
                max_time_minutes=(
                    int(limits_raw["max_time_minutes"])
                    if limits_raw.get("max_time_minutes") is not None
                    else None
                ),
                max_tokens_total=(
                    int(limits_raw["max_tokens_total"])
                    if limits_raw.get("max_tokens_total") is not None
                    else None
                ),
                max_iterations=(
                    int(limits_raw["max_iterations"])
                    if limits_raw.get("max_iterations") is not None
                    else None
                ),
                max_turns_per_iteration=(
                    int(limits_raw["max_turns_per_iteration"])
                    if limits_raw.get("max_turns_per_iteration") is not None
                    else None
                ),
            ),
            resume_history=[
                ResumeEvent(
                    timestamp=str(item.get("timestamp", utc_now_iso())),
                    mode=str(item.get("mode", "resume")),
                    details=dict(item.get("details", {})),
                )
                for item in resume_raw
            ],
        )

    def mark_updated(self) -> None:
        """Update `updated_at` timestamp."""
        self.updated_at = utc_now_iso()

    def add_resume_event(self, mode: str, details: dict[str, Any]) -> None:
        """Append resume history entry."""
        self.resume_history.append(ResumeEvent(timestamp=utc_now_iso(), mode=mode, details=details))
        self.mark_updated()


def load_state(path: Path) -> RunState:
    """Load run state from JSON."""
    import json

    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Invalid state file: {path}")
    return RunState.from_dict(data)


def save_state(path: Path, state: RunState) -> None:
    """Persist run state to JSON."""
    import json

    state.mark_updated()
    path.write_text(json.dumps(state.as_dict(), indent=2, sort_keys=False) + "\n")


def default_state_path(run_dir: Path) -> Path:
    """Resolve state file path for a run directory."""
    return run_dir / STATE_FILE_NAME
