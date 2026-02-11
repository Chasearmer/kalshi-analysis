"""Tests for backtest metric computation helpers."""

import pandas as pd
import pytest

from simulation.metrics import (
    compute_daily_pnl,
    compute_max_drawdown,
    compute_profit_factor,
    compute_sharpe,
)


class TestComputeDailyPnl:
    def test_aggregates_by_date(self) -> None:
        """Groups and sums net_pnl by settle_date."""
        df = pd.DataFrame({
            "settle_date": ["2024-01-01", "2024-01-01", "2024-01-02"],
            "net_pnl": [10.0, 20.0, 5.0],
        })
        daily = compute_daily_pnl(df)
        assert daily["2024-01-01"] == pytest.approx(30.0)
        assert daily["2024-01-02"] == pytest.approx(5.0)

    def test_empty_dataframe(self) -> None:
        df = pd.DataFrame({"settle_date": [], "net_pnl": []})
        daily = compute_daily_pnl(df)
        assert daily.empty

    def test_sorted_by_date(self) -> None:
        df = pd.DataFrame({
            "settle_date": ["2024-01-03", "2024-01-01", "2024-01-02"],
            "net_pnl": [1.0, 2.0, 3.0],
        })
        daily = compute_daily_pnl(df)
        dates = list(daily.index)
        assert dates == sorted(dates)


class TestComputeSharpe:
    def test_positive_sharpe(self) -> None:
        """Consistently positive daily P&L gives high Sharpe."""
        daily = pd.Series([1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0])
        sharpe = compute_sharpe(daily)
        assert sharpe > 5.0  # Very high for near-constant positive returns

    def test_zero_sharpe(self) -> None:
        """Alternating +1/-1 has ~0 Sharpe."""
        daily = pd.Series([1.0, -1.0] * 50)
        sharpe = compute_sharpe(daily)
        assert abs(sharpe) < 0.5

    def test_negative_sharpe(self) -> None:
        """Consistently negative P&L gives negative Sharpe."""
        daily = pd.Series([-5.0, -3.0, -4.0, -6.0, -2.0])
        sharpe = compute_sharpe(daily)
        assert sharpe < 0

    def test_empty_series(self) -> None:
        assert compute_sharpe(pd.Series(dtype=float)) == pytest.approx(0.0)

    def test_single_value(self) -> None:
        assert compute_sharpe(pd.Series([1.0])) == pytest.approx(0.0)


class TestComputeMaxDrawdown:
    def test_no_drawdown(self) -> None:
        """Monotonically increasing equity has zero drawdown."""
        cum = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        dd, dd_pct = compute_max_drawdown(cum)
        assert dd == pytest.approx(0.0)
        assert dd_pct == pytest.approx(0.0)

    def test_known_drawdown(self) -> None:
        """Peak at 100, trough at 60 → drawdown 40, 40%."""
        cum = pd.Series([50.0, 100.0, 80.0, 60.0, 90.0])
        dd, dd_pct = compute_max_drawdown(cum)
        assert dd == pytest.approx(40.0)
        assert dd_pct == pytest.approx(0.4)

    def test_recovery(self) -> None:
        """Drawdown followed by full recovery still records peak drawdown."""
        cum = pd.Series([100.0, 50.0, 100.0, 150.0])
        dd, dd_pct = compute_max_drawdown(cum)
        assert dd == pytest.approx(50.0)
        assert dd_pct == pytest.approx(0.5)

    def test_empty_series(self) -> None:
        dd, dd_pct = compute_max_drawdown(pd.Series(dtype=float))
        assert dd == pytest.approx(0.0)
        assert dd_pct == pytest.approx(0.0)


class TestComputeProfitFactor:
    def test_profitable(self) -> None:
        """Wins > losses gives profit factor > 1."""
        assert compute_profit_factor(200.0, 100.0) == pytest.approx(2.0)

    def test_unprofitable(self) -> None:
        """Wins < losses gives profit factor < 1."""
        assert compute_profit_factor(50.0, 100.0) == pytest.approx(0.5)

    def test_no_losses(self) -> None:
        """No losses gives infinite profit factor."""
        assert compute_profit_factor(100.0, 0.0) == float("inf")

    def test_no_wins_or_losses(self) -> None:
        assert compute_profit_factor(0.0, 0.0) == pytest.approx(0.0)

    def test_no_wins(self) -> None:
        assert compute_profit_factor(0.0, 100.0) == pytest.approx(0.0)
