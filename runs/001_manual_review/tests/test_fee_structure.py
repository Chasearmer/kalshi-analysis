"""Tests for fee structure calibration analysis using synthetic Parquet fixtures."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.fee_structure import run


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create minimal Parquet dataset for fee structure tests.

    Full chain: series -> events -> markets -> trades.
    - S1: fee_type='quadratic', fee_multiplier=1.0
    - S2: fee_type='quadratic_with_maker_fees', fee_multiplier=0.5
    - E1 -> S1 (Sports), E2 -> S2 (Politics)
    - M1 -> E1 (result='yes'), M2 -> E2 (result='no'), M3 -> E1 (result='yes')
    - t1: M1, yes-taker, yes_price=0.60, 10 contracts -> quadratic
    - t2: M2, no-taker,  no_price=0.70, 15 contracts -> quadratic_with_maker_fees
    - t3: M3, yes-taker, yes_price=0.50, 20 contracts -> quadratic
    """
    # Series
    series_dir = tmp_path / "series"
    series_dir.mkdir()
    series = pa.table(
        {
            "ticker": ["S1", "S2"],
            "fee_type": ["quadratic", "quadratic_with_maker_fees"],
            "fee_multiplier": [1.0, 0.5],
        }
    )
    pq.write_table(series, series_dir / "series_000000.parquet")

    # Events
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    events = pa.table(
        {
            "event_ticker": ["E1", "E2"],
            "category": ["Sports", "Politics"],
            "series_ticker": ["S1", "S2"],
        }
    )
    pq.write_table(events, events_dir / "events_000000.parquet")

    # Markets
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table(
        {
            "ticker": ["M1", "M2", "M3"],
            "event_ticker": ["E1", "E2", "E1"],
            "status": ["finalized", "finalized", "finalized"],
            "result": ["yes", "no", "yes"],
            "volume_fp": ["100.00", "200.00", "150.00"],
        }
    )
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    # Trades
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table(
        {
            "trade_id": ["t1", "t2", "t3"],
            "ticker": ["M1", "M2", "M3"],
            "yes_price_dollars": ["0.6000", "0.3000", "0.5000"],
            "no_price_dollars": ["0.4000", "0.7000", "0.5000"],
            "count_fp": ["10.00", "15.00", "20.00"],
            "taker_side": ["yes", "no", "yes"],
            "created_time": [
                "2024-06-15T12:00:00Z",
                "2024-06-15T13:00:00Z",
                "2024-06-15T14:00:00Z",
            ],
        }
    )
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    return tmp_path


class TestFeeStructureRun:
    def test_run_produces_output(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """run() returns AnalysisResult with 2 figures and a CSV."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, n_bins=1)

        assert len(result.figure_paths) == 2
        for fig_path in result.figure_paths:
            assert fig_path.exists()
        assert result.csv_path.exists()
        assert len(result.summary) > 0

    def test_csv_has_fee_types(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """CSV contains rows for both fee types."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, n_bins=1)

        df = pd.read_csv(result.csv_path)
        fee_types = set(df["fee_type"].unique())
        assert "quadratic" in fee_types
        assert "quadratic_with_maker_fees" in fee_types

    def test_fee_type_column_values(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """CSV only contains expected fee type values."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, n_bins=1)

        df = pd.read_csv(result.csv_path)
        allowed_fee_types = {"quadratic", "quadratic_with_maker_fees", "unknown"}
        actual_fee_types = set(df["fee_type"].unique())
        assert actual_fee_types.issubset(allowed_fee_types)
