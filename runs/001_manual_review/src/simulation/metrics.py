"""Backtest metric computation helpers.

Computes Sharpe ratio, max drawdown, profit factor, and daily P&L aggregation
from trade-level backtest results.
"""

import math

import pandas as pd


def compute_daily_pnl(trade_df: pd.DataFrame, date_col: str = "settle_date") -> pd.Series:
    """Aggregate trade-level P&L into a daily P&L series.

    Args:
        trade_df: DataFrame with columns [date_col, 'net_pnl'].
        date_col: Column name containing the date to group by.

    Returns:
        Series indexed by date with daily net P&L values.
    """
    if trade_df.empty:
        return pd.Series(dtype=float)
    return trade_df.groupby(date_col)["net_pnl"].sum().sort_index()


def compute_sharpe(daily_pnl: pd.Series, trading_days: int = 252) -> float:
    """Compute annualized Sharpe ratio from a daily P&L series.

    Args:
        daily_pnl: Series of daily P&L values.
        trading_days: Number of trading days per year for annualization.

    Returns:
        Annualized Sharpe ratio. Returns 0.0 if std is zero or series is empty.
    """
    if daily_pnl.empty or len(daily_pnl) < 2:
        return 0.0
    mean = daily_pnl.mean()
    std = daily_pnl.std(ddof=1)
    if std == 0 or math.isnan(std):
        return 0.0
    return (mean / std) * math.sqrt(trading_days)


def compute_max_drawdown(cumulative_pnl: pd.Series) -> tuple[float, float]:
    """Compute maximum drawdown from a cumulative P&L series.

    Args:
        cumulative_pnl: Series of cumulative P&L values.

    Returns:
        Tuple of (max_drawdown_absolute, max_drawdown_from_peak_fraction).
        max_drawdown_absolute is always >= 0 (magnitude of worst drawdown).
        max_drawdown_from_peak_fraction is relative to peak equity (0.0-1.0+).
        Returns (0.0, 0.0) if series is empty or never draws down.
    """
    if cumulative_pnl.empty:
        return 0.0, 0.0

    running_max = cumulative_pnl.cummax()
    drawdown = running_max - cumulative_pnl
    max_dd = drawdown.max()

    if max_dd <= 0:
        return 0.0, 0.0

    # Find the peak at which the max drawdown started
    dd_end_idx = drawdown.idxmax()
    peak_at_dd = running_max.loc[:dd_end_idx].iloc[-1]
    if peak_at_dd > 0:
        dd_pct = max_dd / peak_at_dd
    else:
        dd_pct = 0.0

    return float(max_dd), float(dd_pct)


def compute_profit_factor(gross_wins: float, gross_losses: float) -> float:
    """Compute profit factor (gross wins / gross losses).

    Args:
        gross_wins: Total profit from winning trades (>= 0).
        gross_losses: Total loss from losing trades (>= 0, as magnitude).

    Returns:
        Profit factor. Returns float('inf') if no losses, 0.0 if no wins.
    """
    if gross_losses <= 0:
        return float("inf") if gross_wins > 0 else 0.0
    return gross_wins / gross_losses
