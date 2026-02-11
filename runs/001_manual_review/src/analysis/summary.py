"""Dataset summary statistics for Kalshi markets, trades, events, and series.

Produces an overview of the dataset including market counts by status,
volume distribution by category, monthly trade volume trends, and
result distribution for finalized markets.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

from analysis.base import AnalysisResult, ensure_output_dirs
from util.queries import (
    build_query,
    get_connection,
    resolved_markets_sql,
    with_category_sql,
)

log = logging.getLogger(__name__)


def _format_volume(value: float) -> str:
    """Format a dollar volume as a human-readable string."""
    if value >= 1e9:
        return f"${value / 1e9:.1f}B"
    if value >= 1e6:
        return f"${value / 1e6:.0f}M"
    if value >= 1e3:
        return f"${value / 1e3:.0f}K"
    return f"${value:.0f}"


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run dataset summary analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.

    Returns:
        AnalysisResult with figure path, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()

    log.info("Querying market status counts...")
    status_df = con.execute(f"""
        SELECT status, COUNT(*) AS cnt, SUM(CAST(volume_fp AS DOUBLE)) AS vol
        FROM '{data_dir}/markets/*.parquet'
        GROUP BY status
        ORDER BY vol DESC
    """).df()

    log.info("Querying market result counts...")
    result_df = con.execute(f"""
        SELECT result, COUNT(*) AS cnt
        FROM '{data_dir}/markets/*.parquet'
        WHERE status = 'finalized'
        GROUP BY result
        ORDER BY COUNT(*) DESC
    """).df()

    log.info("Querying trade date range...")
    trade_stats = con.execute(f"""
        SELECT
            COUNT(*) AS trade_count,
            MIN(created_time) AS min_time,
            MAX(created_time) AS max_time
        FROM '{data_dir}/trades/*.parquet'
    """).df()

    trade_count = int(trade_stats["trade_count"].iloc[0])
    min_time = pd.Timestamp(trade_stats["min_time"].iloc[0])
    max_time = pd.Timestamp(trade_stats["max_time"].iloc[0])

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
    category_df["pct_of_volume"] = (category_df["total_volume"] / total_volume * 100).round(2)

    log.info("Querying monthly trade volume...")
    monthly_df = con.execute(f"""
        SELECT
            DATE_TRUNC('month', CAST(created_time AS TIMESTAMP)) AS month,
            SUM(CAST(count_fp AS DOUBLE)) AS contracts
        FROM '{data_dir}/trades/*.parquet'
        GROUP BY month
        ORDER BY month
    """).df()

    event_count = int(con.execute(f"""
        SELECT COUNT(*) AS cnt FROM '{data_dir}/events/*.parquet'
    """).df()["cnt"].iloc[0])

    series_count = int(con.execute(f"""
        SELECT COUNT(*) AS cnt FROM '{data_dir}/series/*.parquet'
    """).df()["cnt"].iloc[0])

    con.close()

    total_markets = int(status_df["cnt"].sum())
    finalized_markets = int(
        status_df.loc[status_df["status"] == "finalized", "cnt"].sum()
    )
    top_category = category_df.iloc[0]
    top_pct = top_category["pct_of_volume"]

    # --- Figure (2x2) ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle("Kalshi Dataset Overview", fontsize=16, fontweight="bold", y=0.98)

    ax_a = axes[0, 0]
    ax_a.barh(status_df["status"], status_df["cnt"], color="#4C72B0")
    ax_a.set_xlabel("Number of Markets")
    ax_a.set_title("(a) Markets by Status")
    ax_a.invert_yaxis()

    ax_b = axes[0, 1]
    cat_plot = category_df.head(10)
    bars = ax_b.barh(cat_plot["category"], cat_plot["total_volume"], color="#55A868")
    ax_b.set_xlabel("Total Volume (contracts)")
    ax_b.set_title("(b) Volume by Category")
    ax_b.invert_yaxis()
    for bar, vol in zip(bars, cat_plot["total_volume"]):
        ax_b.text(
            bar.get_width(), bar.get_y() + bar.get_height() / 2,
            f" {_format_volume(vol)}", va="center", fontsize=8,
        )
    ax_b.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _format_volume(x)))

    ax_c = axes[1, 0]
    ax_c.plot(monthly_df["month"], monthly_df["contracts"], color="#C44E52", linewidth=1.5)
    ax_c.set_xlabel("Month")
    ax_c.set_ylabel("Contracts Traded")
    ax_c.set_title("(c) Monthly Trade Volume")
    ax_c.tick_params(axis="x", rotation=45)
    ax_c.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x / 1e6:.0f}M"))

    ax_d = axes[1, 1]
    ax_d.bar(result_df["result"], result_df["cnt"], color="#8172B2")
    ax_d.set_xlabel("Result")
    ax_d.set_ylabel("Number of Markets")
    ax_d.set_title("(d) Finalized Markets by Result")

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig_path = figures_dir / "dataset_overview.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    csv_path = csv_dir / "dataset_summary.csv"
    category_df[["category", "market_count", "total_volume", "pct_of_volume"]].to_csv(
        csv_path, index=False
    )

    summary = (
        f"Dataset contains {total_markets:,} markets ({finalized_markets:,} finalized), "
        f"{trade_count:,} trades spanning {min_time:%Y-%m-%d} to {max_time:%Y-%m-%d}. "
        f"{event_count:,} events across {series_count:,} series. "
        f"{top_category['category']} dominates with {top_pct:.0f}% of volume."
    )

    return AnalysisResult(figure_paths=[fig_path], csv_path=csv_path, summary=summary)
