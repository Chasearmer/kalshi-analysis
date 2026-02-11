"""Walk-forward validation analysis.

Splits data at 2025-01-01, computes strategy metrics on both in-sample and
out-of-sample periods, and tests whether edge persists in unseen data.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from analysis.base import AnalysisResult, ensure_output_dirs
from simulation.backtest import run_backtest
from simulation.strategy_def import TIER1_STRATEGIES

log = logging.getLogger(__name__)

SPLIT_DATE = "2025-01-01"


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run walk-forward validation analysis.

    Args:
        data_dir: Path to root data directory.
        output_dir: Path to output directory for report artifacts.

    Returns:
        AnalysisResult with comparison table and figures.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    rows = []

    for strategy in TIER1_STRATEGIES:
        log.info("Walk-forward for: %s", strategy.name)

        is_result = run_backtest(data_dir, strategy, end_date=SPLIT_DATE)
        oos_result = run_backtest(data_dir, strategy, start_date=SPLIT_DATE)

        is_m = is_result.metrics
        oos_m = oos_result.metrics

        rows.append({
            "strategy": strategy.name,
            "is_total_pnl_dollars": is_m.get("total_pnl_dollars", 0.0),
            "is_sharpe": is_m.get("sharpe", 0.0),
            "is_win_rate": is_m.get("win_rate", 0.0),
            "is_avg_net_pnl": is_m.get("avg_net_pnl", 0.0),
            "is_max_drawdown_pct": is_m.get("max_drawdown_pct", 0.0),
            "is_trades": is_result.total_trades,
            "oos_total_pnl_dollars": oos_m.get("total_pnl_dollars", 0.0),
            "oos_sharpe": oos_m.get("sharpe", 0.0),
            "oos_win_rate": oos_m.get("win_rate", 0.0),
            "oos_avg_net_pnl": oos_m.get("avg_net_pnl", 0.0),
            "oos_max_drawdown_pct": oos_m.get("max_drawdown_pct", 0.0),
            "oos_trades": oos_result.total_trades,
            "edge_decay": (
                oos_m.get("avg_net_pnl", 0.0) - is_m.get("avg_net_pnl", 0.0)
            ),
        })

    comp_df = pd.DataFrame(rows)
    csv_path = csv_dir / "walk_forward_comparison.csv"
    comp_df.to_csv(csv_path, index=False)

    # --- Figure: IS vs OOS avg_net_pnl comparison ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))

    if not comp_df.empty:
        import numpy as np

        x = np.arange(len(comp_df))
        width = 0.35

        ax.bar(x - width / 2, comp_df["is_avg_net_pnl"], width, label="In-Sample (pre-2025)",
               color="#4C72B0", alpha=0.85)
        ax.bar(x + width / 2, comp_df["oos_avg_net_pnl"], width, label="Out-of-Sample (2025+)",
               color="#C44E52", alpha=0.85)
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.set_xticks(x)
        ax.set_xticklabels(comp_df["strategy"], rotation=30, ha="right", fontsize=8)
        ax.set_ylabel("Avg Net P&L (cents/contract)", fontsize=12)
        ax.set_title(
            f"Walk-Forward Validation: In-Sample vs Out-of-Sample (split: {SPLIT_DATE})",
            fontsize=13,
            fontweight="bold",
        )
        ax.legend(fontsize=10)

    fig.tight_layout()
    fig_path = figures_dir / "walk_forward_comparison.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # --- Figure: IS vs OOS Sharpe comparison ---
    fig2, ax2 = plt.subplots(figsize=(12, 6))

    if not comp_df.empty:
        ax2.bar(x - width / 2, comp_df["is_sharpe"], width, label="In-Sample",
                color="#4C72B0", alpha=0.85)
        ax2.bar(x + width / 2, comp_df["oos_sharpe"], width, label="Out-of-Sample",
                color="#C44E52", alpha=0.85)
        ax2.axhline(y=0, color="black", linewidth=0.5)
        ax2.set_xticks(x)
        ax2.set_xticklabels(comp_df["strategy"], rotation=30, ha="right", fontsize=8)
        ax2.set_ylabel("Annualized Sharpe Ratio", fontsize=12)
        ax2.set_title("Walk-Forward: Sharpe Ratio Comparison", fontsize=13, fontweight="bold")
        ax2.legend(fontsize=10)

    fig2.tight_layout()
    fig2_path = figures_dir / "walk_forward_sharpe.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # Summary
    if not comp_df.empty:
        survived = (comp_df["oos_avg_net_pnl"] > 0).sum()
        summary = (
            f"Walk-forward validation (split {SPLIT_DATE}): "
            f"{survived}/{len(comp_df)} strategies maintain positive edge out-of-sample. "
        )
        if survived > 0:
            best_oos = comp_df.loc[comp_df["oos_avg_net_pnl"].idxmax()]
            summary += (
                f"Best OOS: {best_oos['strategy']} "
                f"(avg {best_oos['oos_avg_net_pnl']:.2f} cents/contract, "
                f"Sharpe={best_oos['oos_sharpe']:.2f})."
            )
    else:
        summary = "No strategies evaluated."

    return AnalysisResult(
        figure_paths=[fig_path, fig2_path],
        csv_path=csv_path,
        summary=summary,
    )
