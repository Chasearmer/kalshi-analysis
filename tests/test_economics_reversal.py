"""Tests for Economics reversal / Elections strategy analysis."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.economics_reversal import run


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create minimal Parquet dataset for Economics reversal tests.

    Events: E1 (Economics), E2 (Elections), E3 (Sports)
    Markets:
    - M1 (E1, result=yes): economics favorite wins
    - M2 (E1, result=no): economics longshot loses
    - M3 (E2, result=no): elections
    - M4 (E3, result=yes): sports (excluded)

    Trades:
    - t1: M1, yes-taker, price=80 -> wins (Economics favorite)
    - t2: M2, no-taker, price=15 -> wins (Economics longshot NO)
    - t3: M3, no-taker, price=50 -> wins (Elections NO)
    - t4: M4, yes-taker, price=60 -> wins (Sports, excluded)
    """
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table(
        {
            "ticker": ["M1", "M2", "M3", "M4"],
            "event_ticker": ["E1", "E1", "E2", "E3"],
            "status": ["finalized", "finalized", "finalized", "finalized"],
            "result": ["yes", "no", "no", "yes"],
            "volume_fp": ["100.00", "200.00", "150.00", "300.00"],
        }
    )
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table(
        {
            "trade_id": ["t1", "t2", "t3", "t4"],
            "ticker": ["M1", "M2", "M3", "M4"],
            "yes_price_dollars": ["0.8000", "0.8500", "0.5000", "0.6000"],
            "no_price_dollars": ["0.2000", "0.1500", "0.5000", "0.4000"],
            "count_fp": ["10.00", "10.00", "10.00", "10.00"],
            "taker_side": ["yes", "no", "no", "yes"],
            "created_time": [
                "2024-06-15T12:00:00Z",
                "2024-06-15T13:00:00Z",
                "2024-06-15T14:00:00Z",
                "2024-06-15T15:00:00Z",
            ],
        }
    )
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    events_dir = tmp_path / "events"
    events_dir.mkdir()
    events = pa.table(
        {
            "event_ticker": ["E1", "E2", "E3"],
            "category": ["Economics", "Elections", "Sports"],
            "series_ticker": ["S1", "S1", "S2"],
        }
    )
    pq.write_table(events, events_dir / "events_000000.parquet")

    series_dir = tmp_path / "series"
    series_dir.mkdir()
    series = pa.table(
        {
            "ticker": ["S1", "S2"],
            "fee_type": ["quadratic", "quadratic"],
            "fee_multiplier": [1.0, 1.0],
        }
    )
    pq.write_table(series, series_dir / "series_000000.parquet")

    return tmp_path


class TestEconomicsReversalRun:
    def test_produces_output(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """run() returns AnalysisResult with 3 figures and a CSV."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, n_bins=5)

        assert len(result.figure_paths) == 3
        for fig_path in result.figure_paths:
            assert fig_path.exists()
        assert result.csv_path.exists()
        assert len(result.summary) > 0

    def test_excludes_sports(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """Sports category trades are not included."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, n_bins=5)

        df = pd.read_csv(result.csv_path)
        if not df.empty:
            categories = set(df["category"].unique())
            assert "Sports" not in categories

    def test_csv_has_strategy_columns(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """CSV has required strategy metric columns."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, n_bins=5)

        df = pd.read_csv(result.csv_path)
        if not df.empty:
            expected = {
                "category",
                "strategy",
                "gross_edge_pp",
                "net_edge_pp",
                "fee_cost_pp",
                "daily_cap",
                "kelly",
            }
            assert expected.issubset(set(df.columns))
