"""Multi-strategy portfolio analysis.

Computes correlation between strategy daily P&L, constructs an equal-weight
portfolio equity curve, and measures diversification benefit.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.base import AnalysisResult, ensure_output_dirs
from simulation.backtest import BacktestResult, run_backtest
from simulation.portfolio import combined_equity_curve, portfolio_metrics, strategy_correlation
from simulation.strategy_def import TIER1_STRATEGIES

log = logging.getLogger(__name__)


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run multi-strategy portfolio analysis.

    Args:
        data_dir: Path to root data directory.
        output_dir: Path to output directory for report artifacts.

    Returns:
        AnalysisResult with correlation heatmap, portfolio curves, and metrics.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)

    # Run all strategies
    results: dict[str, BacktestResult] = {}
    for strategy in TIER1_STRATEGIES:
        result = run_backtest(data_dir, strategy, fill_rate=1.0)
        results[strategy.name] = result

    # Correlation matrix
    corr_df = strategy_correlation(results)
    corr_csv = csv_dir / "correlation_matrix.csv"
    corr_df.to_csv(corr_csv)

    # Portfolio equity curve
    portfolio_ec = combined_equity_curve(results)
    port_metrics = portfolio_metrics(portfolio_ec)

    # Individual best Sharpe for comparison
    individual_sharpes = {
        name: r.metrics.get("sharpe", 0.0) for name, r in results.items()
    }
    best_individual_sharpe = max(individual_sharpes.values()) if individual_sharpes else 0.0

    port_metrics["best_individual_sharpe"] = best_individual_sharpe
    port_metrics["diversification_ratio"] = (
        port_metrics["sharpe"] / best_individual_sharpe
        if best_individual_sharpe > 0 else 0.0
    )

    # Save portfolio metrics
    port_df = pd.DataFrame([port_metrics])
    port_csv = csv_dir / "portfolio_metrics.csv"
    port_df.to_csv(port_csv, index=False)

    # --- Figure 1: Correlation heatmap ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax1 = plt.subplots(figsize=(10, 8))

    if not corr_df.empty:
        im = ax1.imshow(corr_df.values, cmap="RdYlBu_r", vmin=-1, vmax=1, aspect="auto")
        ax1.set_xticks(range(len(corr_df.columns)))
        ax1.set_yticks(range(len(corr_df.index)))
        ax1.set_xticklabels(corr_df.columns, rotation=45, ha="right", fontsize=7)
        ax1.set_yticklabels(corr_df.index, fontsize=7)

        # Annotate cells
        for i in range(len(corr_df)):
            for j in range(len(corr_df.columns)):
                val = corr_df.iloc[i, j]
                ax1.text(j, i, f"{val:.2f}", ha="center", va="center",
                         fontsize=8, color="black" if abs(val) < 0.5 else "white")

        fig1.colorbar(im, ax=ax1, shrink=0.8, label="Correlation")
        ax1.set_title("Daily P&L Correlation Matrix", fontsize=14, fontweight="bold")
    else:
        ax1.text(0.5, 0.5, "Insufficient data for correlation", ha="center",
                 va="center", fontsize=14, transform=ax1.transAxes)

    fig1.tight_layout()
    fig1_path = figures_dir / "portfolio_correlation.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Portfolio vs individual equity curves ---
    fig2, ax2 = plt.subplots(figsize=(14, 7))

    colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2", "#CCB974"]
    for i, (name, result) in enumerate(results.items()):
        if result.equity_curve.empty:
            continue
        ec = result.equity_curve
        ax2.plot(ec["date"], ec["cumulative_pnl"] / 100.0, alpha=0.4,
                 color=colors[i % len(colors)], linewidth=1, label=name)

    if not portfolio_ec.empty:
        ax2.plot(portfolio_ec["date"], portfolio_ec["cumulative_pnl"] / 100.0,
                 color="black", linewidth=2.5, label="Equal-Weight Portfolio")

    ax2.axhline(y=0, color="gray", linewidth=0.5)
    ax2.set_xlabel("Date", fontsize=12)
    ax2.set_ylabel("Cumulative P&L ($)", fontsize=12)
    ax2.set_title(
        f"Portfolio vs Individual Strategies  |  "
        f"Portfolio Sharpe={port_metrics.get('sharpe', 0):.2f}  "
        f"Best Individual={best_individual_sharpe:.2f}",
        fontsize=13, fontweight="bold",
    )
    ax2.legend(fontsize=7, loc="upper left")
    fig2.autofmt_xdate()
    fig2.tight_layout()

    fig2_path = figures_dir / "portfolio_equity_curve.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # Summary
    summary = (
        f"Equal-weight portfolio of {len(results)} strategies: "
        f"P&L=${port_metrics.get('total_pnl_dollars', 0):,.0f}, "
        f"Sharpe={port_metrics.get('sharpe', 0):.2f} "
        f"(best individual: {best_individual_sharpe:.2f}, "
        f"diversification ratio: {port_metrics.get('diversification_ratio', 0):.2f}). "
        f"Max drawdown: ${port_metrics.get('max_drawdown', 0) / 100:.0f} "
        f"({port_metrics.get('max_drawdown_pct', 0) * 100:.1f}%)."
    )

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path],
        csv_path=port_csv,
        summary=summary,
    )
