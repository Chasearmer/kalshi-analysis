"""Tests for close-proximity efficiency analysis using synthetic Parquet fixtures."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.close_proximity import run


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create minimal Parquet dataset for close-proximity tests.

    3 markets (2 with close_time, 1 without):
    - M1: result='yes', close_time='2024-06-15T18:00:00Z'
      t1: created='2024-06-15T17:00:00Z' (1h before), yes-taker, taker_price=65, wins
    - M2: result='no', close_time='2024-06-16T12:00:00Z'
      t2: created='2024-06-15T12:00:00Z' (24h before), no-taker, taker_price=70, wins
    - M3: result='yes', close_time=None (should be excluded)
      t3: created='2024-06-15T12:00:00Z', no-taker, taker_price=40, loses
    """
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table(
        {
            "ticker": ["M1", "M2", "M3"],
            "event_ticker": ["E1", "E1", "E2"],
            "status": ["finalized", "finalized", "finalized"],
            "result": ["yes", "no", "yes"],
            "volume_fp": ["100.00", "200.00", "50.00"],
            "close_time": ["2024-06-15T18:00:00Z", "2024-06-16T12:00:00Z", None],
        }
    )
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table(
        {
            "trade_id": ["t1", "t2", "t3"],
            "ticker": ["M1", "M2", "M3"],
            "yes_price_dollars": ["0.6500", "0.3000", "0.6000"],
            "no_price_dollars": ["0.3500", "0.7000", "0.4000"],
            "count_fp": ["10.00", "10.00", "10.00"],
            "taker_side": ["yes", "no", "no"],
            "created_time": [
                "2024-06-15T17:00:00Z",
                "2024-06-15T12:00:00Z",
                "2024-06-15T12:00:00Z",
            ],
        }
    )
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    return tmp_path


class TestCloseProximityRun:
    def test_produces_output(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """run() returns AnalysisResult with 2 figures and a CSV."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        assert len(result.figure_paths) == 2
        for fig_path in result.figure_paths:
            assert fig_path.exists()
        assert result.csv_path.exists()
        assert len(result.summary) > 0

    def test_excludes_null_close_time(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """Markets without close_time are excluded from analysis."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        total = df["total_contracts"].sum()
        # Only t1 (10 contracts) and t2 (10 contracts) should be included
        assert total == pytest.approx(20.0)

    def test_csv_has_expected_columns(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """CSV has all required columns."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        expected = {
            "time_bucket",
            "taker_win_rate",
            "avg_taker_price",
            "excess_return",
            "total_contracts",
            "trade_count",
        }
        assert expected.issubset(set(df.columns))

    def test_time_buckets_assigned(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """Trades are assigned to correct time buckets."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        buckets = set(df["time_bucket"])
        # t1 is ~1h before close -> '0-1h', t2 is ~24h before -> '24-72h'
        assert "0-1h" in buckets or "1-6h" in buckets  # depending on exact time diff
        assert "24-72h" in buckets
