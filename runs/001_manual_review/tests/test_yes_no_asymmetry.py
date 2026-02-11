"""Tests for YES/NO asymmetry analysis using synthetic Parquet fixtures."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.yes_no_asymmetry import run


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create minimal Parquet dataset for YES/NO asymmetry tests.

    4 markets, 4 trades: 2 YES-takers, 2 NO-takers.
    - M1: result='yes', t1: taker_side='yes', yes_price=0.65 -> taker_price=65, taker_won=1
    - M2: result='no',  t2: taker_side='no',  no_price=0.70  -> taker_price=70, taker_won=1
    - M3: result='no',  t3: taker_side='yes', yes_price=0.60 -> taker_price=60, taker_won=0
    - M4: result='yes', t4: taker_side='no',  no_price=0.30  -> taker_price=30, taker_won=0

    With n_bins=1: all trades land in a single bin (0-100, midpoint 50).
    YES-takers: t1 wins (10 contracts), t3 loses (10 contracts) -> win_rate = 50%
    NO-takers:  t2 wins (10 contracts), t4 loses (10 contracts) -> win_rate = 50%
    """
    # Markets
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table(
        {
            "ticker": ["M1", "M2", "M3", "M4"],
            "event_ticker": ["E1", "E1", "E2", "E2"],
            "status": ["finalized", "finalized", "finalized", "finalized"],
            "result": ["yes", "no", "no", "yes"],
            "volume_fp": ["100.00", "200.00", "150.00", "50.00"],
        }
    )
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    # Trades
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table(
        {
            "trade_id": ["t1", "t2", "t3", "t4"],
            "ticker": ["M1", "M2", "M3", "M4"],
            "yes_price_dollars": ["0.6500", "0.3000", "0.6000", "0.7000"],
            "no_price_dollars": ["0.3500", "0.7000", "0.4000", "0.3000"],
            "count_fp": ["10.00", "10.00", "10.00", "10.00"],
            "taker_side": ["yes", "no", "yes", "no"],
            "created_time": [
                "2024-06-15T12:00:00Z",
                "2024-06-15T13:00:00Z",
                "2024-06-15T14:00:00Z",
                "2024-06-15T15:00:00Z",
            ],
        }
    )
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    # Events (required for query infrastructure, not central to this analysis)
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

    return tmp_path


class TestYesNoAsymmetryRun:
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

    def test_csv_has_both_sides(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """CSV contains rows for both 'yes' and 'no' taker_side."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, n_bins=1)

        df = pd.read_csv(result.csv_path)
        sides = set(df["taker_side"].unique())
        assert "yes" in sides
        assert "no" in sides

    def test_csv_columns_present(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """CSV has all expected columns."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir, n_bins=1)

        df = pd.read_csv(result.csv_path)
        expected_cols = {
            "taker_side",
            "bin_start",
            "bin_midpoint",
            "win_rate",
            "implied_prob",
            "excess_return",
            "wins",
            "total_contracts",
        }
        assert expected_cols.issubset(set(df.columns))
