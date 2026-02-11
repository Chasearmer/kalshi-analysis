"""Core backtesting engine for Kalshi trading strategies.

Replays historical trades matching a strategy filter, computes per-trade P&L
with realistic fee deductions, and produces equity curves with summary metrics.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from simulation.metrics import compute_daily_pnl, compute_max_drawdown, compute_profit_factor, compute_sharpe
from simulation.strategy_def import StrategyFilter, strategy_where_clause
from util.fees import kalshi_fee_cents
from util.queries import (
    build_query,
    full_trade_outcomes_with_all_dims_sql,
    get_connection,
    resolved_markets_sql,
    with_fee_type_sql,
)

log = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """Container for backtest outputs."""

    strategy: StrategyFilter
    equity_curve: pd.DataFrame  # columns: date, daily_pnl, cumulative_pnl
    metrics: dict = field(default_factory=dict)
    total_trades: int = 0
    total_contracts: float = 0.0


def fetch_strategy_trades(
    data_dir: Path,
    strategy: StrategyFilter,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Fetch all trades matching a strategy filter from Parquet data.

    Args:
        data_dir: Path to root data directory.
        strategy: Strategy filter to apply.
        start_date: Optional ISO date string for start of period (inclusive).
        end_date: Optional ISO date string for end of period (exclusive).

    Returns:
        DataFrame with columns: ticker, taker_side, taker_price, taker_won,
        contracts, created_time, close_time, fee_multiplier.
        Ordered by created_time.
    """
    where = strategy_where_clause(strategy)
    extra_filters = []
    if start_date:
        extra_filters.append(f"created_time >= '{start_date}'")
    if end_date:
        extra_filters.append(f"created_time < '{end_date}'")

    all_filters = " AND ".join([where] + extra_filters)

    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("markets_with_fees", with_fee_type_sql(data_dir)),
            ("full_trades", full_trade_outcomes_with_all_dims_sql(data_dir)),
        ],
        select=f"""
            SELECT
                ticker,
                taker_side,
                taker_price,
                taker_won,
                contracts,
                created_time,
                close_time,
                fee_multiplier
            FROM full_trades
            WHERE taker_price > 0 AND taker_price < 100
                AND {all_filters}
            ORDER BY created_time
        """,
    )

    con = get_connection()
    df = con.execute(query).df()
    con.close()
    return df


def compute_trade_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """Add per-trade P&L columns to a trade DataFrame.

    Adds columns: fee, gross_pnl, net_pnl, settle_date.
    All values are in cents per contract, multiplied by contract count.

    Args:
        df: DataFrame from fetch_strategy_trades.

    Returns:
        Same DataFrame with added P&L columns.
    """
    if df.empty:
        df["fee"] = pd.Series(dtype=float)
        df["gross_pnl"] = pd.Series(dtype=float)
        df["net_pnl"] = pd.Series(dtype=float)
        df["settle_date"] = pd.Series(dtype="datetime64[ns]")
        return df

    # Per-contract fee
    df["fee"] = df.apply(
        lambda r: kalshi_fee_cents(r["taker_price"], r["fee_multiplier"]) * r["contracts"],
        axis=1,
    )

    # Gross P&L per trade (before fees)
    # Win: (100 - price) * contracts; Loss: -price * contracts
    df["gross_pnl"] = np.where(
        df["taker_won"] == 1,
        (100.0 - df["taker_price"]) * df["contracts"],
        -df["taker_price"] * df["contracts"],
    )

    # Net P&L = gross - fee (fee is always subtracted)
    df["net_pnl"] = df["gross_pnl"] - df["fee"]

    # Settlement date: use close_time if available, otherwise created_time
    df["settle_date"] = pd.to_datetime(
        df["close_time"].fillna(df["created_time"]), format="ISO8601"
    ).dt.date

    return df


def run_backtest(
    data_dir: Path,
    strategy: StrategyFilter,
    fill_rate: float = 1.0,
    seed: int = 42,
    start_date: str | None = None,
    end_date: str | None = None,
) -> BacktestResult:
    """Run a single backtest for one strategy.

    Args:
        data_dir: Path to root data directory.
        strategy: Strategy filter defining trade selection.
        fill_rate: Fraction of matching trades to include (0.0-1.0).
        seed: Random seed for fill rate sampling (for reproducibility).
        start_date: Optional start date filter (inclusive).
        end_date: Optional end date filter (exclusive).

    Returns:
        BacktestResult with equity curve, metrics, and trade counts.
    """
    log.info("Fetching trades for strategy: %s", strategy.name)
    trades_df = fetch_strategy_trades(data_dir, strategy, start_date, end_date)
    log.info("Found %d matching trades", len(trades_df))

    if trades_df.empty:
        return BacktestResult(
            strategy=strategy,
            equity_curve=pd.DataFrame(columns=["date", "daily_pnl", "cumulative_pnl"]),
            metrics={
                "total_pnl": 0.0,
                "sharpe": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "win_rate": 0.0,
                "avg_net_pnl": 0.0,
                "profit_factor": 0.0,
                "total_fee": 0.0,
            },
        )

    # Apply fill rate sampling
    if fill_rate < 1.0:
        rng = np.random.default_rng(seed)
        mask = rng.random(len(trades_df)) < fill_rate
        trades_df = trades_df[mask].reset_index(drop=True)
        log.info("After %.0f%% fill rate: %d trades", fill_rate * 100, len(trades_df))

    if trades_df.empty:
        return BacktestResult(
            strategy=strategy,
            equity_curve=pd.DataFrame(columns=["date", "daily_pnl", "cumulative_pnl"]),
            metrics={
                "total_pnl": 0.0,
                "sharpe": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "win_rate": 0.0,
                "avg_net_pnl": 0.0,
                "profit_factor": 0.0,
                "total_fee": 0.0,
            },
        )

    # Compute per-trade P&L
    trades_df = compute_trade_pnl(trades_df)

    # Build daily P&L series
    daily = compute_daily_pnl(trades_df, date_col="settle_date")
    cumulative = daily.cumsum()

    equity_curve = pd.DataFrame({
        "date": daily.index,
        "daily_pnl": daily.values,
        "cumulative_pnl": cumulative.values,
    })

    # Compute summary metrics
    total_contracts = trades_df["contracts"].sum()
    won_contracts = trades_df.loc[trades_df["taker_won"] == 1, "contracts"].sum()
    win_rate = won_contracts / total_contracts if total_contracts > 0 else 0.0

    gross_wins = trades_df.loc[trades_df["net_pnl"] > 0, "net_pnl"].sum()
    gross_losses = -trades_df.loc[trades_df["net_pnl"] < 0, "net_pnl"].sum()

    total_pnl = trades_df["net_pnl"].sum()
    total_fee = trades_df["fee"].sum()
    max_dd, max_dd_pct = compute_max_drawdown(cumulative)
    sharpe = compute_sharpe(daily)
    pf = compute_profit_factor(gross_wins, gross_losses)

    metrics = {
        "total_pnl": float(total_pnl),
        "total_pnl_dollars": float(total_pnl / 100.0),
        "sharpe": float(sharpe),
        "max_drawdown": float(max_dd),
        "max_drawdown_pct": float(max_dd_pct),
        "win_rate": float(win_rate),
        "avg_net_pnl": float(total_pnl / total_contracts) if total_contracts > 0 else 0.0,
        "profit_factor": float(pf),
        "total_fee": float(total_fee),
        "total_fee_dollars": float(total_fee / 100.0),
    }

    log.info(
        "Strategy '%s': P&L=$%.0f, Sharpe=%.2f, MaxDD=$%.0f (%.1f%%), WR=%.1f%%",
        strategy.name,
        metrics["total_pnl_dollars"],
        sharpe,
        max_dd / 100.0,
        max_dd_pct * 100,
        win_rate * 100,
    )

    return BacktestResult(
        strategy=strategy,
        equity_curve=equity_curve,
        metrics=metrics,
        total_trades=len(trades_df),
        total_contracts=float(total_contracts),
    )
