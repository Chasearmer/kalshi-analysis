"""Tests for day-of-week and seasonality analysis using synthetic Parquet fixtures."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.day_of_week import run


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create minimal Parquet dataset for day-of-week tests.

    7 trades on known days of the week (EDT = UTC-4, midday so same day):
    - t1: 2024-06-10T12:00:00Z -> Monday (ET)
    - t2: 2024-06-11T12:00:00Z -> Tuesday
    - t3: 2024-06-12T12:00:00Z -> Wednesday
    - t4: 2024-06-13T12:00:00Z -> Thursday
    - t5: 2024-06-14T12:00:00Z -> Friday
    - t6: 2024-06-15T12:00:00Z -> Saturday
    - t7: 2024-06-16T12:00:00Z -> Sunday

    All YES-takers at various prices. M1 result='yes' (taker wins),
    M2 result='no' (taker loses).
    """
    # Markets
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table(
        {
            "ticker": ["M1", "M2"],
            "event_ticker": ["E1", "E1"],
            "status": ["finalized", "finalized"],
            "result": ["yes", "no"],
            "volume_fp": ["100.00", "200.00"],
        }
    )
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    # 7 trades covering all days of the week
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table(
        {
            "trade_id": ["t1", "t2", "t3", "t4", "t5", "t6", "t7"],
            "ticker": ["M1", "M2", "M1", "M2", "M1", "M2", "M1"],
            "yes_price_dollars": [
                "0.6000",
                "0.5000",
                "0.7000",
                "0.4000",
                "0.5500",
                "0.4500",
                "0.6500",
            ],
            "no_price_dollars": [
                "0.4000",
                "0.5000",
                "0.3000",
                "0.6000",
                "0.4500",
                "0.5500",
                "0.3500",
            ],
            "count_fp": [
                "10.00",
                "10.00",
                "10.00",
                "10.00",
                "10.00",
                "10.00",
                "10.00",
            ],
            "taker_side": ["yes", "yes", "yes", "yes", "yes", "yes", "yes"],
            "created_time": [
                "2024-06-10T12:00:00Z",  # Monday
                "2024-06-11T12:00:00Z",  # Tuesday
                "2024-06-12T12:00:00Z",  # Wednesday
                "2024-06-13T12:00:00Z",  # Thursday
                "2024-06-14T12:00:00Z",  # Friday
                "2024-06-15T12:00:00Z",  # Saturday
                "2024-06-16T12:00:00Z",  # Sunday
            ],
        }
    )
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    # Events
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

    return tmp_path


class TestDayOfWeekRun:
    def test_run_produces_output(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """run() returns AnalysisResult with 2 figures and a CSV."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        assert len(result.figure_paths) == 2
        for fig_path in result.figure_paths:
            assert fig_path.exists()
        assert result.csv_path.exists()
        assert len(result.summary) > 0

    def test_day_names_in_csv(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """CSV has correct day_name values for all 7 days."""
        output_dir = tmp_path / "output"
        result = run(fixture_data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        day_names = set(df["day_name"].tolist())
        expected_days = {
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        }
        assert day_names == expected_days

    def test_seasonality_csv_saved(
        self,
        fixture_data_dir: Path,
        tmp_path: Path,
    ) -> None:
        """seasonality.csv is also saved in the output data directory."""
        output_dir = tmp_path / "output"
        run(fixture_data_dir, output_dir)

        seasonality_path = output_dir / "data" / "seasonality.csv"
        assert seasonality_path.exists()
        df = pd.read_csv(seasonality_path)
        assert len(df) >= 1
        assert "taker_win_rate" in df.columns
