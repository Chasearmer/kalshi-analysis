"""Tests for multi-strategy portfolio analysis."""

import pandas as pd
import pytest

from simulation.backtest import BacktestResult
from simulation.portfolio import combined_equity_curve, portfolio_metrics, strategy_correlation
from simulation.strategy_def import StrategyFilter


def _make_result(name: str, daily_pnl: list[float], dates: list[str]) -> BacktestResult:
    """Helper to create a BacktestResult with known equity curve."""
    cumulative = []
    total = 0.0
    for p in daily_pnl:
        total += p
        cumulative.append(total)

    strategy = StrategyFilter(
        name=name, taker_side="yes", category="Elections",
        fee_type="*", time_bucket="*", price_min=60.0, price_max=100.0,
    )
    return BacktestResult(
        strategy=strategy,
        equity_curve=pd.DataFrame({
            "date": dates,
            "daily_pnl": daily_pnl,
            "cumulative_pnl": cumulative,
        }),
        metrics={"total_pnl": total, "sharpe": 1.0},
        total_trades=len(daily_pnl),
        total_contracts=100.0,
    )


class TestStrategyCorrelation:
    def test_perfect_correlation(self) -> None:
        """Identical series have correlation 1.0."""
        dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
        r1 = _make_result("A", [10.0, -5.0, 20.0], dates)
        r2 = _make_result("B", [10.0, -5.0, 20.0], dates)
        corr = strategy_correlation({"A": r1, "B": r2})
        assert corr.loc["A", "B"] == pytest.approx(1.0)

    def test_negative_correlation(self) -> None:
        """Opposite series have correlation -1.0."""
        dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
        r1 = _make_result("A", [10.0, -5.0, 20.0], dates)
        r2 = _make_result("B", [-10.0, 5.0, -20.0], dates)
        corr = strategy_correlation({"A": r1, "B": r2})
        assert corr.loc["A", "B"] == pytest.approx(-1.0)

    def test_single_strategy_returns_empty(self) -> None:
        """Cannot compute correlation with just one strategy."""
        r1 = _make_result("A", [10.0, -5.0], ["2024-01-01", "2024-01-02"])
        corr = strategy_correlation({"A": r1})
        assert corr.empty


class TestCombinedEquityCurve:
    def test_equal_weight(self) -> None:
        """Equal-weight portfolio sums daily P&L from all strategies."""
        dates = ["2024-01-01", "2024-01-02"]
        r1 = _make_result("A", [10.0, 20.0], dates)
        r2 = _make_result("B", [5.0, -10.0], dates)
        combined = combined_equity_curve({"A": r1, "B": r2})
        assert combined["daily_pnl"].tolist() == pytest.approx([15.0, 10.0])
        assert combined["cumulative_pnl"].tolist() == pytest.approx([15.0, 25.0])

    def test_custom_weights(self) -> None:
        """Custom weights scale each strategy's contribution."""
        dates = ["2024-01-01"]
        r1 = _make_result("A", [100.0], dates)
        r2 = _make_result("B", [100.0], dates)
        combined = combined_equity_curve(
            {"A": r1, "B": r2},
            weights={"A": 0.5, "B": 0.25},
        )
        assert combined["daily_pnl"].iloc[0] == pytest.approx(75.0)

    def test_empty_results(self) -> None:
        combined = combined_equity_curve({})
        assert combined.empty


class TestPortfolioMetrics:
    def test_basic_metrics(self) -> None:
        """Portfolio metrics are computed from equity curve."""
        ec = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "daily_pnl": [100.0, -50.0, 200.0],
            "cumulative_pnl": [100.0, 50.0, 250.0],
        })
        metrics = portfolio_metrics(ec)
        assert metrics["total_pnl"] == pytest.approx(250.0)
        assert metrics["total_pnl_dollars"] == pytest.approx(2.5)
        assert metrics["max_drawdown"] == pytest.approx(50.0)

    def test_empty_equity_curve(self) -> None:
        metrics = portfolio_metrics(pd.DataFrame())
        assert metrics["total_pnl"] == pytest.approx(0.0)
