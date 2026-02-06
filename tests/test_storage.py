"""Tests for the chunked Parquet storage module."""

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from download.storage import CursorStore, ParquetChunkWriter


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


class TestParquetChunkWriter:
    def test_writes_chunk_when_buffer_full(self, tmp_data_dir: Path) -> None:
        writer = ParquetChunkWriter(tmp_data_dir / "test", chunk_size=5)
        records = [{"id": i, "value": f"v{i}"} for i in range(5)]
        writer.add_records(records)

        files = sorted((tmp_data_dir / "test").glob("*.parquet"))
        assert len(files) == 1
        table = pq.read_table(files[0])
        assert len(table) == 5

    def test_flush_writes_remaining(self, tmp_data_dir: Path) -> None:
        writer = ParquetChunkWriter(tmp_data_dir / "test", chunk_size=10)
        records = [{"id": i} for i in range(3)]
        writer.add_records(records)

        # Nothing written yet (buffer not full)
        files = sorted((tmp_data_dir / "test").glob("*.parquet"))
        assert len(files) == 0

        writer.flush()
        files = sorted((tmp_data_dir / "test").glob("*.parquet"))
        assert len(files) == 1
        table = pq.read_table(files[0])
        assert len(table) == 3

    def test_multiple_chunks(self, tmp_data_dir: Path) -> None:
        writer = ParquetChunkWriter(tmp_data_dir / "test", chunk_size=3)
        records = [{"id": i} for i in range(10)]
        writer.add_records(records)
        writer.flush()

        files = sorted((tmp_data_dir / "test").glob("*.parquet"))
        # 10 records / 3 per chunk = 3 full chunks + 1 remainder
        assert len(files) == 4
        assert writer.total_written == 10

    def test_sequential_numbering(self, tmp_data_dir: Path) -> None:
        writer = ParquetChunkWriter(tmp_data_dir / "test", chunk_size=2, name_prefix="part")
        writer.add_records([{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}])

        files = sorted((tmp_data_dir / "test").glob("*.parquet"))
        assert files[0].name == "part_000000.parquet"
        assert files[1].name == "part_000001.parquet"

    def test_resumes_numbering(self, tmp_data_dir: Path) -> None:
        output_dir = tmp_data_dir / "test"

        # First writer creates some files
        writer1 = ParquetChunkWriter(output_dir, chunk_size=2, name_prefix="part")
        writer1.add_records([{"id": 1}, {"id": 2}])

        # Second writer should continue numbering
        writer2 = ParquetChunkWriter(output_dir, chunk_size=2, name_prefix="part")
        writer2.add_records([{"id": 3}, {"id": 4}])

        files = sorted(output_dir.glob("*.parquet"))
        assert len(files) == 2
        assert files[0].name == "part_000000.parquet"
        assert files[1].name == "part_000001.parquet"

    def test_empty_flush_is_noop(self, tmp_data_dir: Path) -> None:
        writer = ParquetChunkWriter(tmp_data_dir / "test", chunk_size=10)
        writer.flush()
        files = list((tmp_data_dir / "test").glob("*.parquet"))
        assert len(files) == 0


class TestCursorStore:
    def test_save_and_load(self, tmp_data_dir: Path) -> None:
        store = CursorStore(tmp_data_dir)
        store.save("markets", "abc123", 5000)

        cursor, count = store.load("markets")
        assert cursor == "abc123"
        assert count == 5000

    def test_load_missing_key(self, tmp_data_dir: Path) -> None:
        store = CursorStore(tmp_data_dir)
        cursor, count = store.load("nonexistent")
        assert cursor is None
        assert count == 0

    def test_clear(self, tmp_data_dir: Path) -> None:
        store = CursorStore(tmp_data_dir)
        store.save("markets", "abc123", 5000)
        store.clear("markets")

        cursor, count = store.load("markets")
        assert cursor is None
        assert count == 0

    def test_slash_in_key(self, tmp_data_dir: Path) -> None:
        store = CursorStore(tmp_data_dir)
        store.save("trades/AAPL", "cursor_val", 100)

        cursor, count = store.load("trades/AAPL")
        assert cursor == "cursor_val"
        assert count == 100
