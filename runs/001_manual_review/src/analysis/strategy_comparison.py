"""Strategy comparison summary: unified ranking of all Round 3 strategy candidates.

Reads CSVs from other Round 3 analysis modules and produces a single comparison
table ranking all strategies by total extractable edge (net_edge × capacity).
Assigns strategies to tiers for Round 4 advancement decisions.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.base import (
    AnalysisResult,
    ensure_output_dirs,
)
from util.strategy import sharpe_proxy

log = logging.getLogger(__name__)

# Tier thresholds
TIER1_NET_EDGE = 2.0  # pp
TIER1_CAPACITY = 5000  # contracts/day
TIER2_NET_EDGE = 0.5  # pp


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run strategy comparison analysis.

    Reads CSVs from fade_yes, economics_reversal, and combined_filters modules.

    Args:
        data_dir: Path to the root data directory (unused, but kept for interface consistency).
        output_dir: Path to the output directory (same as other modules).

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)

    strategies = []

    # --- Read Fade YES results ---
    fade_csv = csv_dir / "fade_yes.csv"
    if fade_csv.exists():
        fade_df = pd.read_csv(fade_csv)
        price_rows = fade_df[fade_df["breakdown_type"] == "price_bin"]
        if not price_rows.empty:
            # Overall fade YES strategy (aggregate all price bins)
            total_c = price_rows["total_contracts"].sum()
            tc = price_rows["total_contracts"]
            avg_net = (price_rows["net_edge_pp"] * tc).sum() / total_c
            avg_gross = (price_rows["gross_edge_pp"] * tc).sum() / total_c
            avg_fee = (price_rows["fee_cost_pp"] * tc).sum() / total_c
            avg_wr = (price_rows["win_rate_pct"] * tc).sum() / total_c
            avg_price = (price_rows["avg_price"] * tc).sum() / total_c
            avg_daily = price_rows["daily_cap"].sum()
            avg_kelly = (price_rows["kelly"] * price_rows["total_contracts"]).sum() / total_c

            strategies.append(
                {
                    "strategy_name": "Fade YES (NO >=60c)",
                    "source": "fade_yes",
                    "gross_edge_pp": avg_gross,
                    "fee_cost_pp": avg_fee,
                    "net_edge_pp": avg_net,
                    "win_rate_pct": avg_wr,
                    "avg_price": avg_price,
                    "total_contracts": total_c,
                    "daily_cap": avg_daily,
                    "kelly": avg_kelly,
                }
            )

            # Best individual price bin
            best = price_rows.loc[price_rows["net_edge_pp"].idxmax()]
            strategies.append(
                {
                    "strategy_name": f"Fade YES ({best['breakdown_value']})",
                    "source": "fade_yes",
                    "gross_edge_pp": best["gross_edge_pp"],
                    "fee_cost_pp": best["fee_cost_pp"],
                    "net_edge_pp": best["net_edge_pp"],
                    "win_rate_pct": best["win_rate_pct"],
                    "avg_price": best["avg_price"],
                    "total_contracts": best["total_contracts"],
                    "daily_cap": best["daily_cap"],
                    "kelly": best["kelly"],
                }
            )
    else:
        log.warning("fade_yes.csv not found at %s", fade_csv)

    # --- Read Economics reversal results ---
    econ_csv = csv_dir / "economics_reversal.csv"
    if econ_csv.exists():
        econ_df = pd.read_csv(econ_csv)
        if not econ_df.empty and "net_edge_pp" in econ_df.columns:
            for _, row in econ_df.iterrows():
                strategies.append(
                    {
                        "strategy_name": row.get("description", row.get("strategy", "Unknown")),
                        "source": "economics_reversal",
                        "gross_edge_pp": row["gross_edge_pp"],
                        "fee_cost_pp": row["fee_cost_pp"],
                        "net_edge_pp": row["net_edge_pp"],
                        "win_rate_pct": row["win_rate_pct"],
                        "avg_price": row["avg_price"],
                        "total_contracts": row["total_contracts"],
                        "daily_cap": row["daily_cap"],
                        "kelly": row["kelly"],
                    }
                )
    else:
        log.warning("economics_reversal.csv not found at %s", econ_csv)

    # --- Read Combined filters results (top 3) ---
    combined_csv = csv_dir / "combined_filters.csv"
    if combined_csv.exists():
        combined_df = pd.read_csv(combined_csv)
        if not combined_df.empty and "net_edge_pp" in combined_df.columns:
            top_combined = combined_df.head(3)
            for _, row in top_combined.iterrows():
                strategies.append(
                    {
                        "strategy_name": f"Combined: {row['filter_combination']}",
                        "source": "combined_filters",
                        "gross_edge_pp": row["gross_edge_pp"],
                        "fee_cost_pp": row["fee_cost_pp"],
                        "net_edge_pp": row["net_edge_pp"],
                        "win_rate_pct": row["win_rate_pct"],
                        "avg_price": row["avg_price"],
                        "total_contracts": row["total_contracts"],
                        "daily_cap": row["daily_cap"],
                        "kelly": row["kelly"],
                    }
                )
    else:
        log.warning("combined_filters.csv not found at %s", combined_csv)

    # Build comparison DataFrame
    if not strategies:
        comp_df = pd.DataFrame()
        summary = "No strategy data available for comparison."
        # Create empty figures
        for name in [
            "strategy_comparison_edge",
            "strategy_comparison_scatter",
            "strategy_comparison_sharpe",
        ]:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes, ha="center")
            path = figures_dir / f"{name}.png"
            fig.savefig(path, dpi=150)
            plt.close(fig)

        csv_path = csv_dir / "strategy_comparison.csv"
        pd.DataFrame().to_csv(csv_path, index=False)
        return AnalysisResult(
            figure_paths=[
                figures_dir / f"{n}.png"
                for n in [
                    "strategy_comparison_edge",
                    "strategy_comparison_scatter",
                    "strategy_comparison_sharpe",
                ]
            ],
            csv_path=csv_path,
            summary=summary,
        )

    comp_df = pd.DataFrame(strategies)

    # Compute derived metrics
    comp_df["total_extractable"] = comp_df["net_edge_pp"] * comp_df["daily_cap"]
    comp_df["sharpe"] = comp_df.apply(
        lambda r: sharpe_proxy(r["net_edge_pp"], r["win_rate_pct"] / 100, r["avg_price"]),
        axis=1,
    )

    # Assign tiers
    def assign_tier(row: pd.Series) -> str:
        if row["net_edge_pp"] >= TIER1_NET_EDGE and row["daily_cap"] >= TIER1_CAPACITY:
            return "Tier 1: Advance"
        elif row["net_edge_pp"] >= TIER2_NET_EDGE:
            return "Tier 2: Monitor"
        else:
            return "Tier 3: Reject"

    comp_df["tier"] = comp_df.apply(assign_tier, axis=1)

    # Sort by total extractable edge
    comp_df = comp_df.sort_values("total_extractable", ascending=False).reset_index(drop=True)
    comp_df["rank"] = range(1, len(comp_df) + 1)

    log.info("Strategy comparison results:")
    for _, row in comp_df.iterrows():
        log.info(
            "  #%d [%s]: %s — net=%.2fpp, cap=%.0f/day, sharpe=%.2f",
            row["rank"],
            row["tier"],
            row["strategy_name"],
            row["net_edge_pp"],
            row["daily_cap"],
            row["sharpe"],
        )

    # --- Figure 1: Strategy comparison by net edge ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax1 = plt.subplots(figsize=(12, max(4, len(comp_df) * 0.5 + 1)))

    tier_colors = {
        "Tier 1: Advance": "#55A868",
        "Tier 2: Monitor": "#FFD700",
        "Tier 3: Reject": "#C44E52",
    }
    sorted_df = comp_df.sort_values("net_edge_pp", ascending=True)
    y = np.arange(len(sorted_df))
    colors = [tier_colors.get(t, "#999999") for t in sorted_df["tier"]]
    ax1.barh(y, sorted_df["net_edge_pp"], color=colors, alpha=0.85, edgecolor="white")
    ax1.axvline(x=0, color="black", linewidth=0.5)
    ax1.set_yticks(y)
    ax1.set_yticklabels(sorted_df["strategy_name"], fontsize=9)
    ax1.set_xlabel("Net Edge (pp)", fontsize=12)
    ax1.set_title("Strategy Comparison: Net Edge After Fees", fontsize=14, fontweight="bold")

    # Add tier legend
    for tier, color in tier_colors.items():
        ax1.barh([], [], color=color, label=tier)
    ax1.legend(fontsize=9, loc="lower right")

    fig1.tight_layout()
    fig1_path = figures_dir / "strategy_comparison_edge.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Edge vs capacity scatter ---
    fig2, ax2 = plt.subplots(figsize=(10, 7))

    for tier, color in tier_colors.items():
        tier_data = comp_df[comp_df["tier"] == tier]
        if not tier_data.empty:
            ax2.scatter(
                tier_data["daily_cap"],
                tier_data["net_edge_pp"],
                s=100,
                color=color,
                edgecolors="black",
                alpha=0.8,
                label=tier,
                zorder=5,
            )

    ax2.axhline(y=0, color="black", linewidth=0.5)
    ax2.axhline(y=TIER1_NET_EDGE, color="green", linewidth=0.5, linestyle=":")
    ax2.axvline(x=TIER1_CAPACITY, color="green", linewidth=0.5, linestyle=":")
    ax2.set_xlabel("Daily Capacity (contracts/day)", fontsize=12)
    ax2.set_ylabel("Net Edge (pp)", fontsize=12)
    ax2.set_title("Strategy Map: Edge vs. Capacity", fontsize=14, fontweight="bold")
    ax2.legend(fontsize=10)

    fig2.tight_layout()
    fig2_path = figures_dir / "strategy_comparison_scatter.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Figure 3: Sharpe ratio comparison ---
    fig3, ax3 = plt.subplots(figsize=(12, max(4, len(comp_df) * 0.5 + 1)))

    sorted_sharpe = comp_df.sort_values("sharpe", ascending=True)
    y = np.arange(len(sorted_sharpe))
    colors = [tier_colors.get(t, "#999999") for t in sorted_sharpe["tier"]]
    ax3.barh(y, sorted_sharpe["sharpe"], color=colors, alpha=0.85, edgecolor="white")
    ax3.axvline(x=0, color="black", linewidth=0.5)
    ax3.set_yticks(y)
    ax3.set_yticklabels(sorted_sharpe["strategy_name"], fontsize=9)
    ax3.set_xlabel("Sharpe Proxy (annualized)", fontsize=12)
    ax3.set_title("Strategy Comparison: Risk-Adjusted Returns", fontsize=14, fontweight="bold")

    fig3.tight_layout()
    fig3_path = figures_dir / "strategy_comparison_sharpe.png"
    fig3.savefig(fig3_path, dpi=150, bbox_inches="tight")
    plt.close(fig3)

    # --- CSV ---
    csv_path = csv_dir / "strategy_comparison.csv"
    comp_df.to_csv(csv_path, index=False)

    # --- Summary ---
    tier1 = comp_df[comp_df["tier"] == "Tier 1: Advance"]
    tier2 = comp_df[comp_df["tier"] == "Tier 2: Monitor"]
    best = comp_df.iloc[0]

    summary = (
        f"{len(comp_df)} strategies evaluated. "
        f"Tier 1 (advance to backtesting): {len(tier1)}. "
        f"Tier 2 (monitor): {len(tier2)}. "
        f"Top strategy: {best['strategy_name']} "
        f"(net {best['net_edge_pp']:+.2f}pp, "
        f"capacity {best['daily_cap']:,.0f}/day, "
        f"sharpe={best['sharpe']:.2f})."
    )

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path, fig3_path],
        csv_path=csv_path,
        summary=summary,
    )
