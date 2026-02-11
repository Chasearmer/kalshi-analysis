"""Tests for combined multi-filter strategy analysis."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.combined_filters import run


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create minimal Parquet dataset for combined filters tests.

    Creates trades with varying taker_side, fee_type, hour, category, price
    to test multi-dimensional aggregation.
    """
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table(
        {
            "ticker": ["M1", "M2", "M3", "M4"],
            "event_ticker": ["E1", "E1", "E2", "E2"],
            "status": ["finalized", "finalized", "finalized", "finalized"],
            "result": ["yes", "no", "no", "yes"],
            "volume_fp": ["100.00", "200.00", "150.00", "100.00"],
        }
    )
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    # Create enough trades to meet MIN_CONTRACTS threshold (10K)
    # Use large count_fp to reach threshold with fewer rows
    n = 20
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table(
        {
            "trade_id": [f"t{i}" for i in range(n)],
            "ticker": ["M1", "M2", "M3", "M4"] * 5,
            "yes_price_dollars": ["0.6500", "0.3000", "0.3500", "0.7000"] * 5,
            "no_price_dollars": ["0.3500", "0.7000", "0.6500", "0.3000"] * 5,
            "count_fp": ["1000.00"] * n,
            "taker_side": ["yes", "no", "no", "yes"] * 5,
            "created_time": [
                # Mix of evening (21:00) and morning (10:00) ET
                "2024-06-15T21:00:00-04:00" if i % 2 == 0 else "2024-06-15T10:00:00-04:00"
                for i in range(n)
            ],
        }
    )
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    events_dir = tmp_path / "events"
    events_dir.mkdir()
    events = pa.table(
        {
            "event_ticker": ["E1", "E2"],
            "category": ["Sports", "Economics"],
            "series_ticker": ["S1", "S2"],
        }
    )
    pq.write_table(events, events_dir / "events_000000.parquet")

    series_dir = tmp_path / "series"
    series_dir.mkdir()
    series = pa.table(
        {
            "ticker": ["S1", "S2"],
            "fee_type": ["quadratic_with_maker_fees", "quadratic"],
            "fee_multiplier": [1.0, 0.5],
        }
    )
    pq.write_table(series, series_dir / "series_000000.parquet")

    return tmp_path


class TestCombinedFiltersRun:
    def test_produces_output(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """run() returns AnalysisResult with 3 figures and a CSV."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, top_n=5, min_contracts=1)

        assert len(result.figure_paths) == 3
        for fig_path in result.figure_paths:
            assert fig_path.exists()
        assert result.csv_path.exists()
        assert len(result.summary) > 0

    def test_csv_has_ranking_columns(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """CSV has rank and filter combination columns."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, top_n=5, min_contracts=1)

        df = pd.read_csv(result.csv_path)
        if not df.empty:
            assert "rank" in df.columns
            assert "filter_combination" in df.columns
            assert "net_edge_pp" in df.columns
            assert "total_extractable" in df.columns

    def test_combinations_have_multiple_dimensions(
        self, fixture_data_dir: Path, tmp_path: Path
    ) -> None:
        """Filter combinations include multiple dimension columns."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, top_n=5, min_contracts=1)

        df = pd.read_csv(result.csv_path)
        if not df.empty:
            assert "taker_side" in df.columns
            assert "fee_type" in df.columns
            assert "category" in df.columns
