"""Chunked Parquet storage for downloaded Kalshi data.

Writes records in chunks (default 10K rows) to numbered Parquet files,
enabling partial reads with DuckDB glob patterns.
"""

import json
import logging
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10_000
CURSOR_DIR = ".cursors"


class ParquetChunkWriter:
    """Accumulates records and flushes them to numbered Parquet files."""

    def __init__(
        self,
        output_dir: Path,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        name_prefix: str = "part",
    ):
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.name_prefix = name_prefix
        self._buffer: list[dict[str, Any]] = []
        self._file_counter = 0
        self._total_written = 0

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Count existing part files to resume numbering
        existing = sorted(self.output_dir.glob(f"{name_prefix}_*.parquet"))
        if existing:
            last = existing[-1].stem
            num_str = last.split("_")[-1]
            self._file_counter = int(num_str) + 1
            logger.info(
                "Found %d existing chunk files in %s, resuming from %d",
                len(existing),
                output_dir,
                self._file_counter,
            )

    def add_records(self, records: list[dict[str, Any]]) -> None:
        """Add records to the buffer, flushing full chunks to disk."""
        self._buffer.extend(records)
        while len(self._buffer) >= self.chunk_size:
            chunk = self._buffer[: self.chunk_size]
            self._buffer = self._buffer[self.chunk_size :]
            self._write_chunk(chunk)

    def flush(self) -> None:
        """Write any remaining buffered records to disk."""
        if self._buffer:
            self._write_chunk(self._buffer)
            self._buffer = []

    def _write_chunk(self, records: list[dict[str, Any]]) -> None:
        """Write a list of records as a Parquet file."""
        table = pa.Table.from_pylist(records)
        filename = f"{self.name_prefix}_{self._file_counter:06d}.parquet"
        filepath = self.output_dir / filename
        pq.write_table(table, filepath, compression="snappy")
        self._file_counter += 1
        self._total_written += len(records)
        logger.debug(
            "Wrote %d records to %s (total: %d)", len(records), filepath, self._total_written
        )

    @property
    def total_written(self) -> int:
        return self._total_written


class CursorStore:
    """Persist pagination cursors for resumable downloads.

    Stores cursor state as JSON files in data/.cursors/.
    """

    def __init__(self, data_dir: Path):
        self.cursor_dir = data_dir / CURSOR_DIR
        self.cursor_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        safe_key = key.replace("/", "__")
        return self.cursor_dir / f"{safe_key}.json"

    def save(self, key: str, cursor: str, records_so_far: int = 0) -> None:
        """Save cursor state for a download key."""
        state = {"cursor": cursor, "records_so_far": records_so_far}
        self._path(key).write_text(json.dumps(state))

    def load(self, key: str) -> tuple[str | None, int]:
        """Load cursor state. Returns (cursor, records_so_far) or (None, 0)."""
        path = self._path(key)
        if path.exists():
            state = json.loads(path.read_text())
            return state.get("cursor"), state.get("records_so_far", 0)
        return None, 0

    def clear(self, key: str) -> None:
        """Clear cursor state after successful completion."""
        path = self._path(key)
        if path.exists():
            path.unlink()
