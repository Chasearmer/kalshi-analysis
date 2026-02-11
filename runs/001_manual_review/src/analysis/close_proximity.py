"""Close-proximity efficiency analysis: taker returns vs time-to-settlement.

Investigates whether market prices become more efficient (lower taker excess
return magnitude) as settlement approaches. Bins trades by hours until market
close_time and computes contract-weighted taker win rates per bucket.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from analysis.base import (
    AnalysisResult,
    ensure_output_dirs,
    validate_row_count,
)
from util.queries import (
    build_query,
    get_connection,
    resolved_markets_sql,
    trade_outcomes_with_timing_sql,
)

log = logging.getLogger(__name__)

# Ordered from closest to farthest from settlement
TIME_BUCKET_ORDER = ["after_close", "0-1h", "1-6h", "6-24h", "24-72h", "72h+"]
TIME_BUCKET_LABELS = ["After\nclose", "<1h", "1-6h", "6-24h", "1-3d", "3d+"]


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run close-proximity efficiency analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()

    log.info("Running close-proximity efficiency analysis...")
    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("trade_outcomes_timing", trade_outcomes_with_timing_sql(data_dir)),
        ],
        select="""
            SELECT
                CASE
                    WHEN hours_to_close < 0 THEN 'after_close'
                    WHEN hours_to_close < 1 THEN '0-1h'
                    WHEN hours_to_close < 6 THEN '1-6h'
                    WHEN hours_to_close < 24 THEN '6-24h'
                    WHEN hours_to_close < 72 THEN '24-72h'
                    ELSE '72h+'
                END AS time_bucket,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) * 100 AS taker_win_rate,
                SUM(taker_price * contracts) / SUM(contracts) AS avg_taker_price,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM trade_outcomes_timing
            WHERE taker_price > 0 AND taker_price < 100
            GROUP BY time_bucket
        """,
    )
    df = con.execute(query).df()
    con.close()

    validate_row_count(df, 1, "Close-proximity time buckets")

    # Compute excess return
    df["excess_return"] = df["taker_win_rate"] - df["avg_taker_price"]

    # Sort by bucket order
    bucket_order = {b: i for i, b in enumerate(TIME_BUCKET_ORDER)}
    df["sort_key"] = df["time_bucket"].map(bucket_order)
    df = df.sort_values("sort_key").reset_index(drop=True)

    # Map labels for plotting
    label_map = dict(zip(TIME_BUCKET_ORDER, TIME_BUCKET_LABELS))
    df["label"] = df["time_bucket"].map(label_map)

    log.info("Time buckets found: %s", list(df["time_bucket"]))
    for _, row in df.iterrows():
        log.info(
            "  %s: excess=%.2fpp, contracts=%.0f, trades=%.0f",
            row["time_bucket"],
            row["excess_return"],
            row["total_contracts"],
            row["trade_count"],
        )

    # --- Figure 1: Excess return vs time-to-close ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax1 = plt.subplots(figsize=(10, 6))

    colors = ["#C44E52" if er < 0 else "#55A868" for er in df["excess_return"]]
    x = np.arange(len(df))
    bars = ax1.bar(x, df["excess_return"], color=colors, alpha=0.85, edgecolor="white")

    ax1.axhline(y=0, color="black", linewidth=0.5)
    ax1.set_xlabel("Time to Market Close", fontsize=12)
    ax1.set_ylabel("Taker Excess Return (pp)", fontsize=12)
    ax1.set_title(
        "Market Efficiency vs. Time to Settlement",
        fontsize=14,
        fontweight="bold",
    )
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["label"], fontsize=10)

    # Annotate bars
    for bar, val in zip(bars, df["excess_return"]):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{val:+.2f}",
            ha="center",
            va="bottom" if val >= 0 else "top",
            fontsize=9,
        )

    fig1.tight_layout()
    fig1_path = figures_dir / "close_proximity_excess_return.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Volume by time bucket ---
    fig2, ax2 = plt.subplots(figsize=(10, 5))

    ax2.bar(x, df["total_contracts"] / 1e6, color="#4C72B0", alpha=0.85, edgecolor="white")
    ax2.set_xlabel("Time to Market Close", fontsize=12)
    ax2.set_ylabel("Contracts (millions)", fontsize=12)
    ax2.set_title("Trading Volume by Time to Settlement", fontsize=14, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels(df["label"], fontsize=10)

    fig2.tight_layout()
    fig2_path = figures_dir / "close_proximity_volume.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- CSV ---
    csv_path = csv_dir / "close_proximity.csv"
    df[
        [
            "time_bucket",
            "taker_win_rate",
            "avg_taker_price",
            "excess_return",
            "total_contracts",
            "trade_count",
        ]
    ].to_csv(csv_path, index=False)

    # --- Summary ---
    best_row = df.loc[df["excess_return"].abs().idxmin()]
    worst_row = df.loc[df["excess_return"].abs().idxmax()]
    total = df["total_contracts"].sum()

    summary = (
        f"Analyzed {total:,.0f} contracts across {len(df)} time buckets. "
        f"Most efficient bucket: {best_row['time_bucket']} "
        f"(excess {best_row['excess_return']:+.2f}pp). "
        f"Least efficient: {worst_row['time_bucket']} "
        f"(excess {worst_row['excess_return']:+.2f}pp)."
    )

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path],
        csv_path=csv_path,
        summary=summary,
    )
