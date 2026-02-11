"""Volume distribution analysis across categories and over time.

Examines how trading volume is distributed across market categories
and how it has evolved over the platform's history.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

from analysis.base import AnalysisResult, ensure_output_dirs, validate_row_count
from util.queries import (
    build_query,
    get_connection,
    resolved_markets_sql,
    trade_outcomes_sql,
    with_category_sql,
)

log = logging.getLogger(__name__)


def _format_volume(value: float) -> str:
    """Format a volume number as human-readable string."""
    if value >= 1e9:
        return f"${value / 1e9:.1f}B"
    if value >= 1e6:
        return f"${value / 1e6:.0f}M"
    if value >= 1e3:
        return f"${value / 1e3:.0f}K"
    return f"${value:.0f}"


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run volume distribution analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()

    # --- Query 1: Volume by category ---
    log.info("Querying volume by category...")
    category_query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("categorized", with_category_sql(data_dir)),
        ],
        select="""
            SELECT
                category,
                COUNT(*) AS market_count,
                SUM(volume) AS total_volume
            FROM categorized
            GROUP BY category
            ORDER BY total_volume DESC
        """,
    )
    category_df = con.execute(category_query).df()
    total_volume = category_df["total_volume"].sum()
    category_df["pct_of_total"] = (category_df["total_volume"] / total_volume * 100).round(2)
    validate_row_count(category_df, 1, "Category volume")

    # --- Query 2: Monthly volume by category ---
    log.info("Querying monthly volume by category...")
    monthly_query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("categorized", with_category_sql(data_dir)),
            ("trade_outcomes", trade_outcomes_sql(data_dir)),
        ],
        select="""
            SELECT
                DATE_TRUNC('month', CAST(t.created_time AS TIMESTAMP)) AS month,
                c.category,
                SUM(t.contracts) AS contracts
            FROM trade_outcomes t
            JOIN categorized c ON t.ticker = c.ticker
            GROUP BY month, c.category
            ORDER BY month
        """,
    )
    monthly_df = con.execute(monthly_query).df()
    con.close()

    # Identify top-5 categories, bucket rest as "Other"
    top5 = category_df.head(5)["category"].tolist()
    monthly_df["category_group"] = monthly_df["category"].apply(
        lambda c: c if c in top5 else "Other"
    )
    monthly_pivot = monthly_df.groupby(["month", "category_group"])["contracts"].sum().reset_index()
    monthly_pivot = monthly_pivot.pivot(
        index="month", columns="category_group", values="contracts"
    ).fillna(0)

    # Reorder columns: top5 in order, then Other
    col_order = [c for c in top5 if c in monthly_pivot.columns]
    if "Other" in monthly_pivot.columns:
        col_order.append("Other")
    monthly_pivot = monthly_pivot[col_order]

    # --- Figure (2x1) ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10))
    fig.suptitle("Kalshi Volume Distribution", fontsize=16, fontweight="bold", y=0.98)

    # (a) Volume by category
    bars = ax1.barh(category_df["category"], category_df["total_volume"], color="#4C72B0")
    ax1.set_xlabel("Total Volume (contracts)")
    ax1.set_title("(a) Volume by Category")
    ax1.invert_yaxis()
    for bar, vol in zip(bars, category_df["total_volume"]):
        ax1.text(
            bar.get_width(), bar.get_y() + bar.get_height() / 2,
            f" {_format_volume(vol)}", va="center", fontsize=8,
        )
    ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _format_volume(x)))

    # (b) Monthly volume stacked area by top-5 categories
    colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2", "#CCB974", "#AAAAAA"]
    ax2.stackplot(
        monthly_pivot.index, *[monthly_pivot[c] for c in monthly_pivot.columns],
        labels=monthly_pivot.columns, colors=colors[:len(monthly_pivot.columns)],
        alpha=0.8,
    )
    ax2.set_xlabel("Month")
    ax2.set_ylabel("Contracts")
    ax2.set_title("(b) Monthly Volume by Category (Top 5 + Other)")
    ax2.legend(loc="upper left", fontsize=8)
    ax2.tick_params(axis="x", rotation=45)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x / 1e6:.0f}M"))

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig_path = figures_dir / "volume_distribution.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # --- CSV ---
    csv_path = csv_dir / "volume_distribution.csv"
    category_df[["category", "market_count", "total_volume", "pct_of_total"]].to_csv(
        csv_path, index=False
    )

    # --- Summary ---
    top_cat = category_df.iloc[0]
    first_month = monthly_pivot.index.min()
    last_month = monthly_pivot.index.max()
    first_vol = monthly_pivot.iloc[0].sum()
    last_vol = monthly_pivot.iloc[-1].sum()

    summary = (
        f"{top_cat['category']} accounts for {top_cat['pct_of_total']:.0f}% of all volume. "
        f"Platform grew from {_format_volume(first_vol)}/month "
        f"({pd.Timestamp(first_month):%Y-%m}) to {_format_volume(last_vol)}/month "
        f"({pd.Timestamp(last_month):%Y-%m})."
    )

    return AnalysisResult(
        figure_paths=[fig_path], csv_path=csv_path, summary=summary,
    )
