"""Strategy edge decay analysis over time.

Computes rolling net edge in 90-day windows for each Tier 1 strategy
and tests for statistically significant decay (negative slope).
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.base import AnalysisResult, ensure_output_dirs
from simulation.backtest import compute_trade_pnl, fetch_strategy_trades
from simulation.strategy_def import TIER1_STRATEGIES

log = logging.getLogger(__name__)

WINDOW_DAYS = 90


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run strategy decay analysis.

    Args:
        data_dir: Path to root data directory.
        output_dir: Path to output directory for report artifacts.

    Returns:
        AnalysisResult with rolling edge figures and trend statistics.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    rows = []
    rolling_data = {}

    for strategy in TIER1_STRATEGIES:
        log.info("Computing rolling edge for: %s", strategy.name)
        trades_df = fetch_strategy_trades(data_dir, strategy)

        if trades_df.empty or len(trades_df) < 100:
            log.info("Insufficient trades for %s, skipping", strategy.name)
            continue

        trades_df = compute_trade_pnl(trades_df)
        trades_df["trade_date"] = pd.to_datetime(trades_df["created_time"]).dt.date

        # Compute daily average net P&L per contract
        daily = trades_df.groupby("trade_date").agg(
            total_net_pnl=("net_pnl", "sum"),
            total_contracts=("contracts", "sum"),
        )
        daily["avg_net_pnl"] = daily["total_net_pnl"] / daily["total_contracts"]

        # Rolling mean over WINDOW_DAYS
        rolling = daily["avg_net_pnl"].rolling(window=WINDOW_DAYS, min_periods=30).mean()
        rolling = rolling.dropna()

        if len(rolling) < 10:
            continue

        rolling_data[strategy.name] = rolling

        # Linear trend: regress rolling edge against time index
        x = np.arange(len(rolling), dtype=float)
        y = rolling.values
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        # Slope per day, annualized
        slope_annual = slope * 365.0

        # Correlation
        corr = np.corrcoef(x, y)[0, 1]

        rows.append({
            "strategy": strategy.name,
            "slope_per_day": float(slope),
            "slope_annual": float(slope_annual),
            "correlation": float(corr),
            "first_rolling_edge": float(rolling.iloc[0]),
            "last_rolling_edge": float(rolling.iloc[-1]),
            "edge_change": float(rolling.iloc[-1] - rolling.iloc[0]),
            "n_days": len(rolling),
        })

    decay_df = pd.DataFrame(rows)
    csv_path = csv_dir / "decay_stats.csv"
    decay_df.to_csv(csv_path, index=False)

    # --- Figure: Rolling edge over time ---
    plt.style.use("seaborn-v0_8-whitegrid")
    n = len(rolling_data)
    if n == 0:
        fig, ax = plt.subplots(figsize=(14, 4))
        ax.text(0.5, 0.5, "Insufficient data for decay analysis", ha="center",
                va="center", fontsize=14, transform=ax.transAxes)
        fig_path = figures_dir / "strategy_decay_rolling.png"
        fig.savefig(fig_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return AnalysisResult(
            figure_paths=[fig_path],
            csv_path=csv_path,
            summary="Insufficient data for decay analysis.",
        )

    fig, axes = plt.subplots(n, 1, figsize=(14, 3.5 * n), sharex=True)
    if n == 1:
        axes = [axes]

    colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2", "#CCB974"]
    for i, (name, rolling) in enumerate(rolling_data.items()):
        ax = axes[i]
        ax.plot(rolling.index, rolling.values, color=colors[i % len(colors)], linewidth=1.5)
        ax.axhline(y=0, color="black", linewidth=0.5, linestyle="--")

        # Trend line
        x = np.arange(len(rolling), dtype=float)
        coeffs = np.polyfit(x, rolling.values, 1)
        trend_y = np.polyval(coeffs, x)
        ax.plot(rolling.index, trend_y, color="gray", linewidth=1, linestyle="--", alpha=0.7)

        ax.set_ylabel("Avg Net P&L\n(cents/contract)", fontsize=9)

        decay_row = decay_df[decay_df["strategy"] == name]
        if not decay_row.empty:
            r = decay_row.iloc[0]
            ax.set_title(
                f"{name}  |  slope={r['slope_annual']:.3f}/yr  r={r['correlation']:.2f}",
                fontsize=9, fontweight="bold",
            )

    axes[-1].set_xlabel("Date", fontsize=12)
    fig.suptitle(
        f"Rolling {WINDOW_DAYS}-Day Average Net Edge",
        fontsize=14, fontweight="bold", y=1.01,
    )
    fig.autofmt_xdate()
    fig.tight_layout()

    fig_path = figures_dir / "strategy_decay_rolling.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Summary
    if not decay_df.empty:
        decaying = (decay_df["slope_annual"] < 0).sum()
        summary = (
            f"Decay analysis for {len(decay_df)} strategies ({WINDOW_DAYS}-day rolling). "
            f"{decaying}/{len(decay_df)} show negative slope (potential decay). "
        )
        worst = decay_df.loc[decay_df["slope_annual"].idxmin()]
        summary += (
            f"Steepest decay: {worst['strategy']} "
            f"(slope={worst['slope_annual']:.3f}/yr, r={worst['correlation']:.2f})."
        )
    else:
        summary = "No strategies had sufficient data for decay analysis."

    return AnalysisResult(
        figure_paths=[fig_path],
        csv_path=csv_path,
        summary=summary,
    )
