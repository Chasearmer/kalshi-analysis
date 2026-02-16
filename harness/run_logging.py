"""Structured JSONL logging for Ralph loop execution."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

EVENTS_LOG_NAME = "events.jsonl"
ITERATIONS_LOG_NAME = "iterations.jsonl"


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO8601 format."""
    return datetime.now(UTC).isoformat()


def _to_jsonable(value: Any) -> Any:
    """Convert object graph to JSON-compatible data."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if dataclasses.is_dataclass(value):
        return _to_jsonable(dataclasses.asdict(value))
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]
    if hasattr(value, "__dict__"):
        return _to_jsonable(vars(value))
    return str(value)


class RunLogger:
    """JSONL append-only logger."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.logs_dir = run_dir / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.logs_dir / EVENTS_LOG_NAME
        self.iterations_path = self.logs_dir / ITERATIONS_LOG_NAME

    def append_event(
        self,
        *,
        event_type: str,
        iteration: int | None,
        payload: Any,
    ) -> None:
        """Append an event log line."""
        event = {
            "timestamp": utc_now_iso(),
            "event_type": event_type,
            "iteration": iteration,
            "payload": _to_jsonable(payload),
        }
        self._append_jsonl(self.events_path, event)

    def append_iteration(self, payload: dict[str, Any]) -> None:
        """Append one iteration summary entry."""
        event = {
            "timestamp": utc_now_iso(),
            **_to_jsonable(payload),
        }
        self._append_jsonl(self.iterations_path, event)

    @staticmethod
    def _append_jsonl(path: Path, obj: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, sort_keys=False) + "\n")
