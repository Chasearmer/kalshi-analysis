"""Maker-taker asymmetry analysis.

Decomposes each trade into taker and maker perspectives, bins by each
side's own price, and computes contract-weighted win rates. Reveals the
systematic advantage of passive liquidity providers (makers) over
aggressive order initiators (takers).
"""

import logging
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
    get_connection,
    resolved_markets_sql,
    trade_outcomes_sql,
)
from util.stats import two_proportion_z_test

log = logging.getLogger(__name__)


def run(data_dir: Path, output_dir: Path, n_bins: int = 10) -> AnalysisResult:
    """Run maker/taker asymmetry analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.
        n_bins: Number of price bins (default 10 = deciles).

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()
    bin_width = 100.0 / n_bins

    log.info("Running maker/taker decomposition (%d bins)...", n_bins)
    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("trade_outcomes", trade_outcomes_sql(data_dir)),
        ],
        select=f"""
            SELECT 'taker' AS side,
                   FLOOR(taker_price / {bin_width}) * {bin_width} AS bin_start,
                   FLOOR(taker_price / {bin_width}) * {bin_width} + {bin_width} / 2.0
                       AS bin_midpoint,
                   SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                       / SUM(contracts) * 100 AS win_rate,
                   SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END) AS wins,
                   SUM(contracts) AS total_contracts
            FROM trade_outcomes
            WHERE taker_price > 0 AND taker_price < 100
            GROUP BY bin_start
            UNION ALL
            SELECT 'maker' AS side,
                   FLOOR(maker_price / {bin_width}) * {bin_width} AS bin_start,
                   FLOOR(maker_price / {bin_width}) * {bin_width} + {bin_width} / 2.0
                       AS bin_midpoint,
                   SUM(CASE WHEN maker_won = 1 THEN contracts ELSE 0 END)
                       / SUM(contracts) * 100 AS win_rate,
                   SUM(CASE WHEN maker_won = 1 THEN contracts ELSE 0 END) AS wins,
                   SUM(contracts) AS total_contracts
            FROM trade_outcomes
            WHERE maker_price > 0 AND maker_price < 100
            GROUP BY bin_start
            ORDER BY side, bin_start
        """,
    )
    df = con.execute(query).df()
    con.close()

    taker_df = df[df["side"] == "taker"].reset_index(drop=True)
    maker_df = df[df["side"] == "maker"].reset_index(drop=True)

    validate_row_count(taker_df, n_bins, "Taker bins")
    validate_row_count(maker_df, n_bins, "Maker bins")
    validate_prices(taker_df, "win_rate")
    validate_prices(maker_df, "win_rate")

    # Compute derived columns
    df["implied_prob"] = df["bin_midpoint"]
    df["excess_return"] = df["win_rate"] - df["implied_prob"]

    taker_df["implied_prob"] = taker_df["bin_midpoint"]
    taker_df["excess_return"] = taker_df["win_rate"] - taker_df["implied_prob"]
    maker_df["implied_prob"] = maker_df["bin_midpoint"]
    maker_df["excess_return"] = maker_df["win_rate"] - maker_df["implied_prob"]

    # Overall maker vs taker significance test
    total_taker_wins = int(taker_df["wins"].sum())
    total_taker_n = int(taker_df["total_contracts"].sum())
    total_maker_wins = int(maker_df["wins"].sum())
    total_maker_n = int(maker_df["total_contracts"].sum())
    z_stat, p_value = two_proportion_z_test(
        total_maker_wins, total_maker_n, total_taker_wins, total_taker_n
    )

    maker_overall_wr = total_maker_wins / total_maker_n * 100
    taker_overall_wr = total_taker_wins / total_taker_n * 100
    gap_pp = maker_overall_wr - taker_overall_wr

    log.info(
        "Maker WR: %.2f%%, Taker WR: %.2f%%, Gap: %.2f pp, z=%.2f, p=%.2e",
        maker_overall_wr, taker_overall_wr, gap_pp, z_stat, p_value,
    )

    # --- Figure 1: Calibration curves ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax = plt.subplots(figsize=(10, 7))

    ax.plot(
        taker_df["bin_midpoint"], taker_df["win_rate"],
        "o-", color="#C44E52", linewidth=2, markersize=8, label="Taker",
    )
    ax.plot(
        maker_df["bin_midpoint"], maker_df["win_rate"],
        "s-", color="#4C72B0", linewidth=2, markersize=8, label="Maker",
    )
    ax.plot([0, 100], [0, 100], "--", color="gray", linewidth=1, label="Fair value")
    ax.set_xlabel("Price (cents)", fontsize=12)
    ax.set_ylabel("Win Rate (%)", fontsize=12)
    ax.set_title(
        f"Maker vs. Taker Calibration (gap: {gap_pp:+.2f}pp, p={p_value:.1e})",
        fontsize=14, fontweight="bold",
    )
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.legend(fontsize=11)

    fig1.tight_layout()
    fig1_path = figures_dir / "maker_taker_calibration.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Excess return bars ---
    fig2, ax2 = plt.subplots(figsize=(10, 6))

    x = np.arange(len(taker_df))
    width = 0.35
    ax2.bar(
        x - width / 2, taker_df["excess_return"],
        width, label="Taker", color="#C44E52", alpha=0.8,
    )
    ax2.bar(
        x + width / 2, maker_df["excess_return"],
        width, label="Maker", color="#4C72B0", alpha=0.8,
    )
    ax2.axhline(y=0, color="black", linewidth=0.5)
    ax2.set_xlabel("Price Bin (cents)", fontsize=12)
    ax2.set_ylabel("Excess Return (pp)", fontsize=12)
    ax2.set_title("Excess Return by Price Bin: Maker vs. Taker", fontsize=14, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"{int(m)}" for m in taker_df["bin_midpoint"]])
    ax2.legend(fontsize=11)

    fig2.tight_layout()
    fig2_path = figures_dir / "maker_taker_excess.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- CSV ---
    csv_path = csv_dir / "maker_taker.csv"
    df[["side", "bin_start", "bin_midpoint", "win_rate", "implied_prob",
        "excess_return", "total_contracts"]].to_csv(csv_path, index=False)

    # --- Summary ---
    summary = (
        f"Makers outperform takers by {gap_pp:+.2f}pp overall "
        f"(maker WR: {maker_overall_wr:.2f}%, taker WR: {taker_overall_wr:.2f}%, "
        f"z={z_stat:.1f}, p={p_value:.1e}). "
        f"Gap is largest at price extremes: "
        f"at {taker_df.iloc[0]['bin_midpoint']:.0f}c taker excess = "
        f"{taker_df.iloc[0]['excess_return']:+.1f}pp, "
        f"maker excess = {maker_df.iloc[0]['excess_return']:+.1f}pp."
    )

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path], csv_path=csv_path, summary=summary,
    )
