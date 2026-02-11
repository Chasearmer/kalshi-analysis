"""Tests for the core backtesting engine."""

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from simulation.backtest import compute_trade_pnl, fetch_strategy_trades, run_backtest
from simulation.strategy_def import StrategyFilter
from util.fees import kalshi_fee_cents


@pytest.fixture()
def backtest_data_dir(tmp_path: Path) -> Path:
    """Create synthetic Parquet data for backtest testing.

    Creates:
    - 4 markets: 2 Elections (M1 yes, M2 no), 2 Economics (M3 yes, M4 no)
    - 8 trades across these markets at various prices
    - Events mapping to Elections and Economics categories
    - Series with fee types
    """
    # Markets
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table({
        "ticker": ["M1", "M2", "M3", "M4"],
        "event_ticker": ["ELEC-1", "ELEC-2", "ECON-1", "ECON-2"],
        "status": ["finalized", "finalized", "finalized", "finalized"],
        "result": ["yes", "no", "yes", "no"],
        "volume_fp": ["1000.00", "2000.00", "500.00", "800.00"],
        "close_time": [
            "2024-06-01T12:00:00Z",
            "2024-06-15T12:00:00Z",
            "2024-07-01T12:00:00Z",
            "2024-07-15T12:00:00Z",
        ],
    })
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    # Trades: various taker_sides and prices
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table({
        "trade_id": ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"],
        "ticker": ["M1", "M1", "M2", "M2", "M3", "M3", "M4", "M4"],
        "yes_price_dollars": [
            "0.8000", "0.7500", "0.3000", "0.2000",
            "0.8500", "0.9000", "0.4000", "0.3500",
        ],
        "no_price_dollars": [
            "0.2000", "0.2500", "0.7000", "0.8000",
            "0.1500", "0.1000", "0.6000", "0.6500",
        ],
        "count_fp": [
            "10.00", "5.00", "20.00", "15.00",
            "8.00", "12.00", "6.00", "10.00",
        ],
        "taker_side": ["yes", "no", "yes", "no", "yes", "yes", "no", "yes"],
        "created_time": [
            "2024-05-01T15:00:00Z",
            "2024-05-02T21:00:00Z",
            "2024-06-01T10:00:00Z",
            "2024-06-10T22:00:00Z",
            "2024-06-20T14:00:00Z",
            "2024-06-25T16:00:00Z",
            "2024-07-05T21:30:00Z",
            "2024-07-10T09:00:00Z",
        ],
    })
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    # Events
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    events = pa.table({
        "event_ticker": ["ELEC-1", "ELEC-2", "ECON-1", "ECON-2"],
        "category": ["Elections", "Elections", "Economics", "Economics"],
        "series_ticker": ["S-ELEC", "S-ELEC", "S-ECON", "S-ECON"],
    })
    pq.write_table(events, events_dir / "events_000000.parquet")

    # Series
    series_dir = tmp_path / "series"
    series_dir.mkdir()
    series = pa.table({
        "ticker": ["S-ELEC", "S-ECON"],
        "fee_type": ["quadratic", "quadratic_with_maker_fees"],
        "fee_multiplier": [1.0, 0.5],
    })
    pq.write_table(series, series_dir / "series_000000.parquet")

    return tmp_path


ELECTIONS_YES_HIGH = StrategyFilter(
    name="Test Elections YES High",
    taker_side="yes",
    category="Elections",
    fee_type="quadratic",
    time_bucket="*",
    price_min=60.0,
    price_max=100.0,
)


class TestFetchStrategyTrades:
    def test_filters_correctly(self, backtest_data_dir: Path) -> None:
        """Fetches only trades matching the strategy filter."""
        df = fetch_strategy_trades(backtest_data_dir, ELECTIONS_YES_HIGH)
        # M1 t1: yes-taker at 80c (Elections, quadratic) ✓
        # M2 t3: yes-taker at 30c — below 60c threshold ✗
        assert len(df) == 1
        assert df.iloc[0]["taker_price"] == pytest.approx(80.0)

    def test_date_filtering(self, backtest_data_dir: Path) -> None:
        """Respects start_date and end_date filters."""
        # All trades before 2024-05-02 — should get t1 only
        df = fetch_strategy_trades(
            backtest_data_dir, ELECTIONS_YES_HIGH,
            end_date="2024-05-02",
        )
        assert len(df) == 1

    def test_empty_result_for_unmatched_filter(self, backtest_data_dir: Path) -> None:
        """Returns empty DataFrame when no trades match."""
        s = StrategyFilter(
            name="No match", taker_side="no", category="Elections",
            fee_type="quadratic", time_bucket="evening",
            price_min=95.0, price_max=100.0,
        )
        df = fetch_strategy_trades(backtest_data_dir, s)
        assert df.empty


class TestComputeTradePnl:
    def test_winning_trade_pnl(self) -> None:
        """YES-taker at 80c wins (result=yes): gross=(100-80)*10=200, fee deducted."""
        df = pd.DataFrame({
            "ticker": ["M1"],
            "taker_side": ["yes"],
            "taker_price": [80.0],
            "taker_won": [1],
            "contracts": [10.0],
            "created_time": ["2024-05-01T15:00:00Z"],
            "close_time": ["2024-06-01T12:00:00Z"],
            "fee_multiplier": [1.0],
        })
        result = compute_trade_pnl(df)

        expected_gross = (100.0 - 80.0) * 10.0  # 200 cents
        expected_fee = kalshi_fee_cents(80.0, 1.0) * 10.0
        expected_net = expected_gross - expected_fee

        assert result.iloc[0]["gross_pnl"] == pytest.approx(expected_gross)
        assert result.iloc[0]["fee"] == pytest.approx(expected_fee)
        assert result.iloc[0]["net_pnl"] == pytest.approx(expected_net)

    def test_losing_trade_pnl(self) -> None:
        """YES-taker at 80c loses (result=no): gross=-80*10=-800, fee deducted."""
        df = pd.DataFrame({
            "ticker": ["M2"],
            "taker_side": ["yes"],
            "taker_price": [80.0],
            "taker_won": [0],
            "contracts": [10.0],
            "created_time": ["2024-06-01T10:00:00Z"],
            "close_time": ["2024-06-15T12:00:00Z"],
            "fee_multiplier": [1.0],
        })
        result = compute_trade_pnl(df)

        expected_gross = -80.0 * 10.0  # -800 cents
        expected_fee = kalshi_fee_cents(80.0, 1.0) * 10.0
        expected_net = expected_gross - expected_fee

        assert result.iloc[0]["gross_pnl"] == pytest.approx(expected_gross)
        assert result.iloc[0]["net_pnl"] == pytest.approx(expected_net)

    def test_empty_dataframe(self) -> None:
        """Empty input returns DataFrame with P&L columns but no rows."""
        df = pd.DataFrame({
            "ticker": pd.Series(dtype=str),
            "taker_side": pd.Series(dtype=str),
            "taker_price": pd.Series(dtype=float),
            "taker_won": pd.Series(dtype=int),
            "contracts": pd.Series(dtype=float),
            "created_time": pd.Series(dtype=str),
            "close_time": pd.Series(dtype=str),
            "fee_multiplier": pd.Series(dtype=float),
        })
        result = compute_trade_pnl(df)
        assert "net_pnl" in result.columns
        assert len(result) == 0


class TestRunBacktest:
    def test_basic_backtest(self, backtest_data_dir: Path) -> None:
        """Run a basic backtest and verify result structure."""
        result = run_backtest(backtest_data_dir, ELECTIONS_YES_HIGH)
        assert result.total_trades >= 0
        assert "total_pnl" in result.metrics
        assert "sharpe" in result.metrics
        assert "max_drawdown" in result.metrics
        assert "win_rate" in result.metrics

    def test_fill_rate_reduces_trades(self, backtest_data_dir: Path) -> None:
        """Fill rate < 1.0 samples a subset of trades."""
        full = run_backtest(backtest_data_dir, ELECTIONS_YES_HIGH, fill_rate=1.0)
        # With only 1 matching trade, fill_rate=0.5 may or may not include it
        # Just verify it runs without error
        partial = run_backtest(backtest_data_dir, ELECTIONS_YES_HIGH, fill_rate=0.5, seed=0)
        assert partial.total_trades <= full.total_trades

    def test_reproducibility(self, backtest_data_dir: Path) -> None:
        """Same seed produces same results."""
        r1 = run_backtest(backtest_data_dir, ELECTIONS_YES_HIGH, fill_rate=0.5, seed=42)
        r2 = run_backtest(backtest_data_dir, ELECTIONS_YES_HIGH, fill_rate=0.5, seed=42)
        assert r1.metrics["total_pnl"] == pytest.approx(r2.metrics["total_pnl"])
        assert r1.total_trades == r2.total_trades

    def test_economics_strategy(self, backtest_data_dir: Path) -> None:
        """Economics strategy matches Economics trades."""
        s = StrategyFilter(
            name="Econ YES >=70c", taker_side="yes", category="Economics",
            fee_type="*", time_bucket="*",
            price_min=70.0, price_max=100.0,
        )
        result = run_backtest(backtest_data_dir, s)
        # M3 t5: yes at 85c (Economics) ✓, M3 t6: yes at 90c ✓
        assert result.total_trades == 2

    def test_empty_strategy(self, backtest_data_dir: Path) -> None:
        """Strategy matching no trades returns zero P&L."""
        s = StrategyFilter(
            name="No match", taker_side="no", category="Elections",
            fee_type="quadratic", time_bucket="evening",
            price_min=99.0, price_max=100.0,
        )
        result = run_backtest(backtest_data_dir, s)
        assert result.total_trades == 0
        assert result.metrics["total_pnl"] == pytest.approx(0.0)
