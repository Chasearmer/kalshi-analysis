"""Fill rate sensitivity analysis.

Tests strategy robustness at different fill rates (10%, 25%, 50%, 100%)
with multiple random seeds to produce confidence bands.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.base import AnalysisResult, ensure_output_dirs
from simulation.backtest import run_backtest
from simulation.strategy_def import TIER1_STRATEGIES

log = logging.getLogger(__name__)

FILL_RATES = [0.10, 0.25, 0.50, 1.0]
N_SEEDS = 20


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run fill rate sensitivity analysis.

    Args:
        data_dir: Path to root data directory.
        output_dir: Path to output directory for report artifacts.

    Returns:
        AnalysisResult with sensitivity figures and summary table.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    rows = []

    for strategy in TIER1_STRATEGIES:
        log.info("Fill rate sensitivity for: %s", strategy.name)

        for fill_rate in FILL_RATES:
            pnl_samples = []
            sharpe_samples = []

            n_seeds = 1 if fill_rate == 1.0 else N_SEEDS
            for seed in range(n_seeds):
                result = run_backtest(data_dir, strategy, fill_rate=fill_rate, seed=seed)
                pnl_samples.append(result.metrics.get("total_pnl_dollars", 0.0))
                sharpe_samples.append(result.metrics.get("sharpe", 0.0))

            rows.append({
                "strategy": strategy.name,
                "fill_rate": fill_rate,
                "pnl_median": float(np.median(pnl_samples)),
                "pnl_p5": float(np.percentile(pnl_samples, 5)),
                "pnl_p95": float(np.percentile(pnl_samples, 95)),
                "sharpe_median": float(np.median(sharpe_samples)),
                "sharpe_p5": float(np.percentile(sharpe_samples, 5)),
                "sharpe_p95": float(np.percentile(sharpe_samples, 95)),
                "n_seeds": n_seeds,
            })

    sens_df = pd.DataFrame(rows)
    csv_path = csv_dir / "fill_rate_sensitivity.csv"
    sens_df.to_csv(csv_path, index=False)

    # --- Figure: P&L by fill rate per strategy ---
    plt.style.use("seaborn-v0_8-whitegrid")
    n = len(TIER1_STRATEGIES)
    fig, axes = plt.subplots(1, min(n, 5), figsize=(4 * min(n, 5), 5), sharey=True)
    if n == 1:
        axes = [axes]

    colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2", "#CCB974"]
    for i, strategy in enumerate(TIER1_STRATEGIES):
        if i >= len(axes):
            break
        ax = axes[i]
        sdf = sens_df[sens_df["strategy"] == strategy.name]
        if sdf.empty:
            continue

        ax.errorbar(
            sdf["fill_rate"] * 100,
            sdf["pnl_median"],
            yerr=[
                sdf["pnl_median"] - sdf["pnl_p5"],
                sdf["pnl_p95"] - sdf["pnl_median"],
            ],
            fmt="o-",
            color=colors[i % len(colors)],
            capsize=4,
            linewidth=1.5,
            markersize=6,
        )
        ax.axhline(y=0, color="black", linewidth=0.5, linestyle="--")
        ax.set_xlabel("Fill Rate (%)", fontsize=10)
        if i == 0:
            ax.set_ylabel("Total P&L ($)", fontsize=11)
        ax.set_title(strategy.name, fontsize=8, fontweight="bold")

    fig.suptitle(
        "Fill Rate Sensitivity (median with 5th/95th percentile bands)",
        fontsize=13, fontweight="bold",
    )
    fig.tight_layout()

    fig_path = figures_dir / "fill_rate_sensitivity.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Summary
    if not sens_df.empty:
        # Check which strategies are profitable even at 10% fill rate
        at_10 = sens_df[sens_df["fill_rate"] == 0.10]
        profitable_at_10 = (at_10["pnl_median"] > 0).sum() if not at_10.empty else 0
        summary = (
            f"Fill rate sensitivity across {len(TIER1_STRATEGIES)} strategies "
            f"at {len(FILL_RATES)} fill rates ({N_SEEDS} seeds each). "
            f"{profitable_at_10}/{len(TIER1_STRATEGIES)} profitable at 10% fill rate."
        )
    else:
        summary = "No fill rate sensitivity data."

    return AnalysisResult(
        figure_paths=[fig_path],
        csv_path=csv_path,
        summary=summary,
    )
