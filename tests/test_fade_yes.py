"""Tests for Fade YES strategy analysis using synthetic Parquet fixtures."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.fade_yes import run


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create minimal Parquet dataset for Fade YES tests.

    Markets: M1 (result='yes'), M2 (result='no'), M3 (result='no')
    Series: S1 (fee_mult=0.07)
    Events: E1 (category='Sports')

    Trades:
    - t1: M2, no-taker, no_price=0.70 (taker_price=70) -> wins (M2='no'), qualifies
    - t2: M3, no-taker, no_price=0.65 (taker_price=65) -> wins (M3='no'), qualifies
    - t3: M1, no-taker, no_price=0.80 (taker_price=80) -> loses (M1='yes'), qualifies
    - t4: M2, yes-taker, yes_price=0.65 -> excluded (not no-taker)
    - t5: M1, no-taker, no_price=0.50 (taker_price=50) -> excluded (price < 60)
    """
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table(
        {
            "ticker": ["M1", "M2", "M3"],
            "event_ticker": ["E1", "E1", "E1"],
            "status": ["finalized", "finalized", "finalized"],
            "result": ["yes", "no", "no"],
            "volume_fp": ["100.00", "200.00", "150.00"],
        }
    )
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table(
        {
            "trade_id": ["t1", "t2", "t3", "t4", "t5"],
            "ticker": ["M2", "M3", "M1", "M2", "M1"],
            "yes_price_dollars": ["0.3000", "0.3500", "0.2000", "0.6500", "0.5000"],
            "no_price_dollars": ["0.7000", "0.6500", "0.8000", "0.3500", "0.5000"],
            "count_fp": ["10.00", "10.00", "10.00", "10.00", "10.00"],
            "taker_side": ["no", "no", "no", "yes", "no"],
            "created_time": [
                "2024-06-15T12:00:00Z",
                "2024-06-15T13:00:00Z",
                "2024-06-15T14:00:00Z",
                "2024-06-15T15:00:00Z",
                "2024-06-15T16:00:00Z",
            ],
        }
    )
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    events_dir = tmp_path / "events"
    events_dir.mkdir()
    events = pa.table(
        {
            "event_ticker": ["E1"],
            "category": ["Sports"],
            "series_ticker": ["S1"],
        }
    )
    pq.write_table(events, events_dir / "events_000000.parquet")

    series_dir = tmp_path / "series"
    series_dir.mkdir()
    series = pa.table(
        {
            "ticker": ["S1"],
            "fee_type": ["quadratic"],
            "fee_multiplier": [1.0],
        }
    )
    pq.write_table(series, series_dir / "series_000000.parquet")

    return tmp_path


class TestFadeYesRun:
    def test_produces_output(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """run() returns AnalysisResult with 3 figures and a CSV."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        assert len(result.figure_paths) == 3
        for fig_path in result.figure_paths:
            assert fig_path.exists()
        assert result.csv_path.exists()
        assert len(result.summary) > 0

    def test_filters_correctly(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """Only NO-taker trades >= 60c are included."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        price_rows = df[df["breakdown_type"] == "price_bin"]
        total = price_rows["total_contracts"].sum()
        # t1 (10) + t2 (10) + t3 (10) = 30 contracts, t4 and t5 excluded
        assert total == pytest.approx(30.0)

    def test_csv_has_net_edge(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """CSV includes net_edge_pp column (gross - fees)."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        assert "net_edge_pp" in df.columns
        assert "gross_edge_pp" in df.columns
        assert "fee_cost_pp" in df.columns

    def test_net_edge_less_than_gross(self, fixture_data_dir: Path, tmp_path: Path) -> None:
        """Net edge is less than gross edge due to fees."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        price_rows = df[df["breakdown_type"] == "price_bin"]
        for _, row in price_rows.iterrows():
            assert row["net_edge_pp"] < row["gross_edge_pp"]
