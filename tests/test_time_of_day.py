"""Tests for time-of-day analysis using synthetic Parquet fixtures."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.time_of_day import run


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create minimal Parquet dataset for time-of-day tests.

    Trades at different UTC times that map to known ET hours (EDT = UTC-4):
    - t1: 2024-06-15T16:00:00Z -> ET hour 12, M1, 10 contracts
    - t2: 2024-06-15T20:00:00Z -> ET hour 16, M2, 15 contracts
    - t3: 2024-06-15T04:00:00Z -> ET hour 0,  M3, 5 contracts
    - t4: 2024-06-15T23:00:00Z -> ET hour 19, M1, 20 contracts

    All trades are YES-takers. M1 result='yes' (taker wins), M2 result='no' (taker loses),
    M3 result='yes' (taker wins).
    """
    # Markets
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table(
        {
            "ticker": ["M1", "M2", "M3"],
            "event_ticker": ["E1", "E1", "E2"],
            "status": ["finalized", "finalized", "finalized"],
            "result": ["yes", "no", "yes"],
            "volume_fp": ["100.00", "200.00", "150.00"],
        }
    )
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    # Trades at specific UTC times
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table(
        {
            "trade_id": ["t1", "t2", "t3", "t4"],
            "ticker": ["M1", "M2", "M3", "M1"],
            "yes_price_dollars": ["0.6000", "0.5000", "0.7000", "0.4000"],
            "no_price_dollars": ["0.4000", "0.5000", "0.3000", "0.6000"],
            "count_fp": ["10.00", "15.00", "5.00", "20.00"],
            "taker_side": ["yes", "yes", "yes", "yes"],
            "created_time": [
                "2024-06-15T16:00:00Z",
                "2024-06-15T20:00:00Z",
                "2024-06-15T04:00:00Z",
                "2024-06-15T23:00:00Z",
            ],
        }
    )
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

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

    return tmp_path


class TestTimeOfDayRun:
    def test_run_produces_output(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """run() returns AnalysisResult with 2 figures and a CSV."""
        output_dir = tmp_path / "output"
        # Patch validate_row_count to accept fewer rows for our tiny fixture
        import analysis.time_of_day as mod

        original_validate = mod.validate_row_count
        mod.validate_row_count = lambda df, min_r, ctx: None
        try:
            result = run(fixture_data_dir, output_dir)
        finally:
            mod.validate_row_count = original_validate

        assert len(result.figure_paths) == 2
        for fig_path in result.figure_paths:
            assert fig_path.exists()
        assert result.csv_path.exists()
        assert len(result.summary) > 0

    def test_hours_in_csv(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """CSV has et_hour column with correct ET hour values.

        EDT (UTC-4) conversions:
        - 2024-06-15T16:00:00Z -> ET hour 12
        - 2024-06-15T20:00:00Z -> ET hour 16
        - 2024-06-15T04:00:00Z -> ET hour 0
        - 2024-06-15T23:00:00Z -> ET hour 19
        """
        output_dir = tmp_path / "output"
        import analysis.time_of_day as mod

        original_validate = mod.validate_row_count
        mod.validate_row_count = lambda df, min_r, ctx: None
        try:
            result = run(fixture_data_dir, output_dir)
        finally:
            mod.validate_row_count = original_validate

        df = pd.read_csv(result.csv_path)
        assert "et_hour" in df.columns
        hours = set(df["et_hour"].astype(int).tolist())
        assert {0, 12, 16, 19}.issubset(hours)

    def test_volume_per_hour(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """Verify contract counts per hour match fixture data.

        - Hour 0:  t3 = 5 contracts
        - Hour 12: t1 = 10 contracts
        - Hour 16: t2 = 15 contracts
        - Hour 19: t4 = 20 contracts
        """
        output_dir = tmp_path / "output"
        import analysis.time_of_day as mod

        original_validate = mod.validate_row_count
        mod.validate_row_count = lambda df, min_r, ctx: None
        try:
            result = run(fixture_data_dir, output_dir)
        finally:
            mod.validate_row_count = original_validate

        df = pd.read_csv(result.csv_path)
        hour_contracts = dict(zip(df["et_hour"].astype(int), df["total_contracts"]))
        assert hour_contracts[0] == pytest.approx(5.0)
        assert hour_contracts[12] == pytest.approx(10.0)
        assert hour_contracts[16] == pytest.approx(15.0)
        assert hour_contracts[19] == pytest.approx(20.0)
