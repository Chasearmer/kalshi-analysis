"""Multi-strategy portfolio analysis.

Computes correlation between strategy daily P&L series, constructs
equal-weight or custom-weight portfolio equity curves, and calculates
portfolio-level metrics.
"""

import pandas as pd

from simulation.backtest import BacktestResult
from simulation.metrics import compute_max_drawdown, compute_sharpe


def strategy_correlation(results: dict[str, BacktestResult]) -> pd.DataFrame:
    """Compute daily P&L correlation matrix across strategies.

    Args:
        results: Dict mapping strategy name to BacktestResult.

    Returns:
        Correlation matrix DataFrame (strategies x strategies).
    """
    daily_series = {}
    for name, result in results.items():
        if result.equity_curve.empty:
            continue
        series = result.equity_curve.set_index("date")["daily_pnl"]
        daily_series[name] = series

    if len(daily_series) < 2:
        return pd.DataFrame()

    combined = pd.DataFrame(daily_series).fillna(0.0)
    return combined.corr()


def combined_equity_curve(
    results: dict[str, BacktestResult],
    weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Construct a combined portfolio equity curve.

    Args:
        results: Dict mapping strategy name to BacktestResult.
        weights: Optional weight per strategy. If None, equal-weight (1.0 each).

    Returns:
        DataFrame with columns: date, daily_pnl, cumulative_pnl.
    """
    if weights is None:
        weights = {name: 1.0 for name in results}

    daily_series = {}
    for name, result in results.items():
        if result.equity_curve.empty or name not in weights:
            continue
        series = result.equity_curve.set_index("date")["daily_pnl"] * weights[name]
        daily_series[name] = series

    if not daily_series:
        return pd.DataFrame(columns=["date", "daily_pnl", "cumulative_pnl"])

    combined = pd.DataFrame(daily_series).fillna(0.0)
    portfolio_daily = combined.sum(axis=1).sort_index()
    portfolio_cum = portfolio_daily.cumsum()

    return pd.DataFrame({
        "date": portfolio_daily.index,
        "daily_pnl": portfolio_daily.values,
        "cumulative_pnl": portfolio_cum.values,
    })


def portfolio_metrics(equity_df: pd.DataFrame) -> dict:
    """Compute portfolio-level summary metrics.

    Args:
        equity_df: DataFrame from combined_equity_curve.

    Returns:
        Dict with total_pnl, sharpe, max_drawdown, max_drawdown_pct.
    """
    if equity_df.empty:
        return {
            "total_pnl": 0.0,
            "total_pnl_dollars": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_pct": 0.0,
        }

    daily = pd.Series(equity_df["daily_pnl"].values, index=equity_df["date"])
    cumulative = pd.Series(equity_df["cumulative_pnl"].values, index=equity_df["date"])

    total_pnl = float(cumulative.iloc[-1])
    sharpe = compute_sharpe(daily)
    max_dd, max_dd_pct = compute_max_drawdown(cumulative)

    return {
        "total_pnl": total_pnl,
        "total_pnl_dollars": total_pnl / 100.0,
        "sharpe": float(sharpe),
        "max_drawdown": float(max_dd),
        "max_drawdown_pct": float(max_dd_pct),
    }
