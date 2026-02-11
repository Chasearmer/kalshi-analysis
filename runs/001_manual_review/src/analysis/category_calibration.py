"""Category-level calibration curves: win rate vs. implied probability by market category.

Computes calibration curves broken out by category to reveal whether longshot
bias is stronger in Sports, Crypto, Politics, etc. Produces a small-multiple
grid of calibration curves and an excess-return heatmap.
"""

import logging
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from analysis.base import (
    AnalysisResult,
    ensure_output_dirs,
    validate_prices,
    validate_row_count,
)
from util.queries import (
    build_query,
    categorized_trade_outcomes_sql,
    get_connection,
    resolved_markets_sql,
    trade_outcomes_sql,
    with_category_sql,
)
from util.stats import calibration_error

log = logging.getLogger(__name__)


def run(
    data_dir: Path,
    output_dir: Path,
    n_bins: int = 10,
    min_category_contracts: int = 1_000_000,
) -> AnalysisResult:
    """Run category-level calibration analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.
        n_bins: Number of price bins (default 10 = deciles).
        min_category_contracts: Minimum total contracts for a category to be included.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()
    bin_width = 100.0 / n_bins

    log.info(
        "Running category calibration query (%d bins, width=%.1f, min contracts=%s)...",
        n_bins,
        bin_width,
        f"{min_category_contracts:,}",
    )
    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("categorized", with_category_sql(data_dir)),
            ("trade_outcomes", trade_outcomes_sql(data_dir)),
            ("cat_trades", categorized_trade_outcomes_sql(data_dir)),
        ],
        select=f"""
            SELECT
                category,
                FLOOR(taker_price / {bin_width}) * {bin_width} AS bin_start,
                FLOOR(taker_price / {bin_width}) * {bin_width} + {bin_width} / 2.0
                    AS bin_midpoint,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) * 100 AS win_rate,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM cat_trades
            WHERE taker_price > 0 AND taker_price < 100
            GROUP BY category, bin_start
            ORDER BY category, bin_start
        """,
    )
    df = con.execute(query).df()
    con.close()

    # Validations
    validate_row_count(df, n_bins, "Category calibration bins")
    validate_prices(df, "win_rate")

    # Compute derived columns
    df["implied_prob"] = df["bin_midpoint"]
    df["excess_return"] = df["win_rate"] - df["implied_prob"]

    # Filter to categories meeting the minimum contract threshold
    category_volumes = df.groupby("category")["total_contracts"].sum()
    qualifying = category_volumes[category_volumes >= min_category_contracts]
    log.info(
        "Categories meeting threshold (%s contracts): %d of %d",
        f"{min_category_contracts:,}",
        len(qualifying),
        len(category_volumes),
    )
    df = df[df["category"].isin(qualifying.index)].copy()

    # Compute per-category calibration error
    cal_errors = {}
    for cat in df["category"].unique():
        cat_df = df[df["category"] == cat]
        cal_errors[cat] = calibration_error(
            cat_df, "win_rate", "implied_prob", "total_contracts"
        )
    df["calibration_error"] = df["category"].map(cal_errors)

    # Sort categories by total volume descending, take top 8
    sorted_cats = (
        qualifying.loc[qualifying.index.isin(df["category"].unique())]
        .sort_values(ascending=False)
        .index.tolist()
    )
    top_cats = sorted_cats[:8]
    df = df[df["category"].isin(top_cats)].copy()
    log.info("Top categories for plotting: %s", top_cats)

    # --- Figure 1: Small-multiple calibration grid ---
    plt.style.use("seaborn-v0_8-whitegrid")
    n_cats = len(top_cats)
    ncols = 4
    nrows = math.ceil(n_cats / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(16, 4 * nrows))
    axes_flat = axes.flatten() if n_cats > 1 else [axes]
    colors = plt.cm.tab10.colors

    for idx, cat in enumerate(top_cats):
        ax = axes_flat[idx]
        cat_df = df[df["category"] == cat].sort_values("bin_midpoint")
        color = colors[idx % len(colors)]
        ax.plot(
            cat_df["bin_midpoint"],
            cat_df["win_rate"],
            "o-",
            color=color,
            linewidth=2,
            markersize=5,
            label="Actual",
        )
        ax.plot([0, 100], [0, 100], "--", color="gray", linewidth=1, label="Perfect")
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.set_title(
            f"{cat}\nCE={cal_errors[cat]:.2f}pp",
            fontsize=10,
            fontweight="bold",
        )
        ax.set_xlabel("Implied Prob (%)", fontsize=8)
        ax.set_ylabel("Win Rate (%)", fontsize=8)
        ax.legend(fontsize=7, loc="upper left")

    # Hide unused subplots
    for idx in range(n_cats, len(axes_flat)):
        axes_flat[idx].set_visible(False)

    fig.suptitle(
        "Calibration Curves by Category",
        fontsize=14,
        fontweight="bold",
        y=1.02,
    )
    fig.tight_layout()
    grid_path = figures_dir / "category_calibration_grid.png"
    fig.savefig(grid_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # --- Figure 2: Excess return heatmap ---
    plt.style.use("seaborn-v0_8-whitegrid")
    bin_midpoints = sorted(df["bin_midpoint"].unique())
    n_bins_actual = len(bin_midpoints)
    heatmap_data = np.full((n_cats, n_bins_actual), np.nan)

    for i, cat in enumerate(top_cats):
        cat_df = df[df["category"] == cat]
        for _, row in cat_df.iterrows():
            j = bin_midpoints.index(row["bin_midpoint"])
            heatmap_data[i, j] = row["excess_return"]

    # Determine symmetric color limits
    max_abs = np.nanmax(np.abs(heatmap_data))
    fig, ax = plt.subplots(figsize=(14, max(4, n_cats * 0.6 + 2)))
    from matplotlib.colors import TwoSlopeNorm

    norm = TwoSlopeNorm(vmin=-max_abs, vcenter=0, vmax=max_abs)
    im = ax.pcolormesh(
        np.arange(n_bins_actual + 1),
        np.arange(n_cats + 1),
        heatmap_data,
        cmap="RdYlGn",
        norm=norm,
    )
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Excess Return (pp)", fontsize=10)

    # Labels
    ax.set_xticks(np.arange(n_bins_actual) + 0.5)
    ax.set_xticklabels([f"{m:.0f}" for m in bin_midpoints], fontsize=8)
    ax.set_yticks(np.arange(n_cats) + 0.5)
    ax.set_yticklabels(top_cats, fontsize=9)
    ax.set_xlabel("Price Bin Midpoint (cents)", fontsize=10)
    ax.set_title(
        "Excess Return by Category and Price Bin",
        fontsize=14,
        fontweight="bold",
    )

    # Annotate cells if <= 8 categories
    if n_cats <= 8:
        for i in range(n_cats):
            for j in range(n_bins_actual):
                val = heatmap_data[i, j]
                if not np.isnan(val):
                    text_color = "white" if abs(val) > max_abs * 0.6 else "black"
                    ax.text(
                        j + 0.5,
                        i + 0.5,
                        f"{val:+.1f}",
                        ha="center",
                        va="center",
                        fontsize=7,
                        color=text_color,
                    )

    fig.tight_layout()
    heatmap_path = figures_dir / "category_excess_return_heatmap.png"
    fig.savefig(heatmap_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # --- CSV ---
    csv_path = csv_dir / "category_calibration.csv"
    df[
        [
            "category",
            "bin_start",
            "bin_midpoint",
            "win_rate",
            "implied_prob",
            "excess_return",
            "total_contracts",
            "trade_count",
            "calibration_error",
        ]
    ].to_csv(csv_path, index=False)

    # --- Summary ---
    # Category with strongest longshot bias (most negative excess return at lowest bin)
    lowest_bin = min(bin_midpoints)
    low_bin_df = df[df["bin_midpoint"] == lowest_bin]
    if not low_bin_df.empty:
        worst_longshot = low_bin_df.loc[low_bin_df["excess_return"].idxmin()]
        longshot_cat = worst_longshot["category"]
        longshot_excess = worst_longshot["excess_return"]
    else:
        longshot_cat = "N/A"
        longshot_excess = 0.0

    # Category with best (lowest) calibration error
    best_cat = min(cal_errors, key=lambda c: cal_errors[c] if c in top_cats else float("inf"))
    best_ce = cal_errors[best_cat]

    # Category with worst (highest) calibration error
    worst_cat = max(cal_errors, key=lambda c: cal_errors[c] if c in top_cats else 0.0)
    worst_ce = cal_errors[worst_cat]

    total_contracts = df["total_contracts"].sum()
    summary = (
        f"{n_cats} categories qualify (>={min_category_contracts:,} contracts). "
        f"Strongest longshot bias: {longshot_cat} at {lowest_bin:.0f}c "
        f"(excess {longshot_excess:+.1f}pp). "
        f"Best calibration: {best_cat} ({best_ce:.2f}pp). "
        f"Calibration error range: {best_ce:.2f}pp ({best_cat}) to "
        f"{worst_ce:.2f}pp ({worst_cat}) across {total_contracts:,.0f} contracts."
    )

    return AnalysisResult(
        figure_paths=[grid_path, heatmap_path],
        csv_path=csv_path,
        summary=summary,
    )
