"""Drawdown and risk analysis for backtested strategies.

Computes maximum drawdown, drawdown duration, and produces underwater
curve plots for each Tier 1 strategy.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from analysis.base import AnalysisResult, ensure_output_dirs
from simulation.backtest import run_backtest
from simulation.strategy_def import TIER1_STRATEGIES

log = logging.getLogger(__name__)


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run drawdown analysis.

    Args:
        data_dir: Path to root data directory.
        output_dir: Path to output directory for report artifacts.

    Returns:
        AnalysisResult with underwater curves and drawdown statistics.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    rows = []
    curves = {}

    for strategy in TIER1_STRATEGIES:
        result = run_backtest(data_dir, strategy, fill_rate=1.0)
        m = result.metrics

        if result.equity_curve.empty:
            continue

        ec = result.equity_curve
        cumulative = pd.Series(ec["cumulative_pnl"].values, index=ec["date"])
        running_max = cumulative.cummax()
        drawdown = running_max - cumulative
        underwater = -drawdown / 100.0  # Convert to dollars, negative

        curves[strategy.name] = underwater

        # Find drawdown duration: longest continuous stretch below peak
        is_dd = drawdown > 0
        dd_groups = (~is_dd).cumsum()
        if is_dd.any():
            dd_lengths = is_dd.groupby(dd_groups).sum()
            max_dd_duration = int(dd_lengths.max())
        else:
            max_dd_duration = 0

        rows.append({
            "strategy": strategy.name,
            "max_drawdown_cents": m.get("max_drawdown", 0.0),
            "max_drawdown_dollars": m.get("max_drawdown", 0.0) / 100.0,
            "max_drawdown_pct": m.get("max_drawdown_pct", 0.0),
            "max_dd_duration_days": max_dd_duration,
            "total_pnl_dollars": m.get("total_pnl_dollars", 0.0),
            "sharpe": m.get("sharpe", 0.0),
            "calmar_ratio": (
                m.get("total_pnl_dollars", 0.0) / (m.get("max_drawdown", 0.0) / 100.0)
                if m.get("max_drawdown", 0.0) > 0 else 0.0
            ),
        })

    dd_df = pd.DataFrame(rows)
    csv_path = csv_dir / "drawdown_stats.csv"
    dd_df.to_csv(csv_path, index=False)

    # --- Figure: Underwater curves ---
    plt.style.use("seaborn-v0_8-whitegrid")
    n = len(curves)
    if n == 0:
        fig, ax = plt.subplots(figsize=(14, 4))
        ax.text(0.5, 0.5, "No drawdown data", ha="center", va="center", fontsize=14,
                transform=ax.transAxes)
        fig_path = figures_dir / "drawdown_underwater.png"
        fig.savefig(fig_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return AnalysisResult(
            figure_paths=[fig_path],
            csv_path=csv_path,
            summary="No strategies had trades for drawdown analysis.",
        )

    fig, axes = plt.subplots(n, 1, figsize=(14, 3 * n), sharex=True)
    if n == 1:
        axes = [axes]

    colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2", "#CCB974"]
    for i, (name, underwater) in enumerate(curves.items()):
        ax = axes[i]
        ax.fill_between(underwater.index, 0, underwater.values,
                        color=colors[i % len(colors)], alpha=0.4)
        ax.plot(underwater.index, underwater.values,
                color=colors[i % len(colors)], linewidth=0.8)
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.set_ylabel("Drawdown ($)", fontsize=9)

        dd_row = dd_df[dd_df["strategy"] == name]
        if not dd_row.empty:
            r = dd_row.iloc[0]
            ax.set_title(
                f"{name}  |  MaxDD=${r['max_drawdown_dollars']:,.0f} "
                f"({r['max_drawdown_pct'] * 100:.1f}%)  "
                f"Duration={r['max_dd_duration_days']}d  "
                f"Calmar={r['calmar_ratio']:.2f}",
                fontsize=9, fontweight="bold",
            )

    axes[-1].set_xlabel("Date", fontsize=12)
    fig.suptitle("Underwater Curves (Drawdown from Peak)", fontsize=14, fontweight="bold", y=1.01)
    fig.autofmt_xdate()
    fig.tight_layout()

    fig_path = figures_dir / "drawdown_underwater.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Summary
    if not dd_df.empty:
        worst = dd_df.loc[dd_df["max_drawdown_dollars"].idxmax()]
        summary = (
            f"Drawdown analysis for {len(dd_df)} strategies. "
            f"Worst max drawdown: {worst['strategy']} "
            f"(${worst['max_drawdown_dollars']:,.0f}, "
            f"{worst['max_drawdown_pct'] * 100:.1f}%, "
            f"{worst['max_dd_duration_days']}d duration)."
        )
    else:
        summary = "No drawdown data available."

    return AnalysisResult(
        figure_paths=[fig_path],
        csv_path=csv_path,
        summary=summary,
    )
