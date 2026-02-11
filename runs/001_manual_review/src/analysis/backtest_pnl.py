"""Per-strategy P&L backtest analysis.

Runs full-period backtests for each Tier 1 strategy and produces equity curves,
summary metrics table, and combined overlay figure.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from analysis.base import AnalysisResult, ensure_output_dirs
from simulation.backtest import BacktestResult, run_backtest
from simulation.strategy_def import TIER1_STRATEGIES

log = logging.getLogger(__name__)


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run per-strategy P&L backtest analysis.

    Args:
        data_dir: Path to root data directory.
        output_dir: Path to output directory for report artifacts.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    results: dict[str, BacktestResult] = {}

    for strategy in TIER1_STRATEGIES:
        result = run_backtest(data_dir, strategy, fill_rate=1.0)
        results[strategy.name] = result

    # --- Build summary CSV ---
    rows = []
    for name, result in results.items():
        m = result.metrics
        rows.append({
            "strategy": name,
            "total_pnl_cents": m.get("total_pnl", 0.0),
            "total_pnl_dollars": m.get("total_pnl_dollars", 0.0),
            "sharpe": m.get("sharpe", 0.0),
            "max_drawdown_cents": m.get("max_drawdown", 0.0),
            "max_drawdown_pct": m.get("max_drawdown_pct", 0.0),
            "win_rate": m.get("win_rate", 0.0),
            "avg_net_pnl_cents": m.get("avg_net_pnl", 0.0),
            "profit_factor": m.get("profit_factor", 0.0),
            "total_fee_dollars": m.get("total_fee_dollars", 0.0),
            "total_trades": result.total_trades,
            "total_contracts": result.total_contracts,
        })

    summary_df = pd.DataFrame(rows)
    csv_path = csv_dir / "strategy_metrics.csv"
    summary_df.to_csv(csv_path, index=False)

    # --- Figure: Combined equity curves ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(14, 7))

    colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2", "#CCB974"]
    for i, (name, result) in enumerate(results.items()):
        if result.equity_curve.empty:
            continue
        ec = result.equity_curve
        # Convert cents to dollars for readability
        ax.plot(
            ec["date"],
            ec["cumulative_pnl"] / 100.0,
            label=f"{name} (${result.metrics.get('total_pnl_dollars', 0):,.0f})",
            color=colors[i % len(colors)],
            linewidth=1.5,
        )

    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Cumulative P&L ($)", fontsize=12)
    ax.set_title("Tier 1 Strategy Equity Curves (100% fill rate)", fontsize=14, fontweight="bold")
    ax.legend(fontsize=8, loc="upper left")
    fig.autofmt_xdate()
    fig.tight_layout()

    fig_path = figures_dir / "backtest_equity_curves.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # --- Figure: Per-strategy individual panels ---
    n = len(results)
    fig2, axes = plt.subplots(n, 1, figsize=(14, 4 * n), sharex=True)
    if n == 1:
        axes = [axes]

    for i, (name, result) in enumerate(results.items()):
        ax = axes[i]
        if result.equity_curve.empty:
            ax.set_title(f"{name} (no trades)", fontsize=10)
            continue
        ec = result.equity_curve
        m = result.metrics
        ax.fill_between(
            ec["date"],
            0,
            ec["cumulative_pnl"] / 100.0,
            alpha=0.3,
            color=colors[i % len(colors)],
        )
        ax.plot(
            ec["date"],
            ec["cumulative_pnl"] / 100.0,
            color=colors[i % len(colors)],
            linewidth=1.5,
        )
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.set_ylabel("Cum. P&L ($)", fontsize=10)
        ax.set_title(
            f"{name}  |  P&L=${m.get('total_pnl_dollars', 0):,.0f}  "
            f"Sharpe={m.get('sharpe', 0):.2f}  "
            f"MaxDD={m.get('max_drawdown_pct', 0) * 100:.1f}%  "
            f"WR={m.get('win_rate', 0) * 100:.1f}%",
            fontsize=9,
            fontweight="bold",
        )

    axes[-1].set_xlabel("Date", fontsize=12)
    fig2.suptitle("Individual Strategy Equity Curves", fontsize=14, fontweight="bold", y=1.01)
    fig2.autofmt_xdate()
    fig2.tight_layout()

    fig2_path = figures_dir / "backtest_individual_curves.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Summary text ---
    if not summary_df.empty:
        best = summary_df.loc[summary_df["total_pnl_dollars"].idxmax()]
        total_pnl = summary_df["total_pnl_dollars"].sum()
        summary = (
            f"{len(TIER1_STRATEGIES)} Tier 1 strategies backtested. "
            f"Combined P&L: ${total_pnl:,.0f}. "
            f"Best: {best['strategy']} (${best['total_pnl_dollars']:,.0f}, "
            f"Sharpe={best['sharpe']:.2f})."
        )
    else:
        summary = "No strategies backtested."

    return AnalysisResult(
        figure_paths=[fig_path, fig2_path],
        csv_path=csv_path,
        summary=summary,
    )
