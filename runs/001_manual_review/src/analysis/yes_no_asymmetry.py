"""YES/NO taker-side asymmetry analysis.

Decomposes trades by taker_side (yes vs no) and computes contract-weighted
win rates at each price bin. Tests whether NO-side takers systematically
outperform YES-side takers, revealing directional bias among market
participants.
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
from util.stats import bonferroni_correct, two_proportion_z_test

log = logging.getLogger(__name__)


def run(data_dir: Path, output_dir: Path, n_bins: int = 10) -> AnalysisResult:
    """Run YES/NO taker-side asymmetry analysis.

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

    log.info("Running YES/NO asymmetry analysis (%d bins)...", n_bins)
    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("trade_outcomes", trade_outcomes_sql(data_dir)),
        ],
        select=f"""
            SELECT
                taker_side,
                FLOOR(taker_price / {bin_width}) * {bin_width} AS bin_start,
                FLOOR(taker_price / {bin_width}) * {bin_width} + {bin_width} / 2.0
                    AS bin_midpoint,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END) AS wins,
                SUM(contracts) AS total_contracts,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) * 100 AS win_rate
            FROM trade_outcomes
            WHERE taker_price > 0 AND taker_price < 100
            GROUP BY taker_side, bin_start
            ORDER BY taker_side, bin_start
        """,
    )
    df = con.execute(query).df()
    con.close()

    # Split by taker side
    yes_df = df[df["taker_side"] == "yes"].reset_index(drop=True)
    no_df = df[df["taker_side"] == "no"].reset_index(drop=True)

    validate_row_count(yes_df, n_bins, "YES-taker bins")
    validate_row_count(no_df, n_bins, "NO-taker bins")
    validate_prices(yes_df, "win_rate")
    validate_prices(no_df, "win_rate")

    # Compute derived columns
    df["implied_prob"] = df["bin_midpoint"]
    df["excess_return"] = df["win_rate"] - df["implied_prob"]

    yes_df["implied_prob"] = yes_df["bin_midpoint"]
    yes_df["excess_return"] = yes_df["win_rate"] - yes_df["implied_prob"]
    no_df["implied_prob"] = no_df["bin_midpoint"]
    no_df["excess_return"] = no_df["win_rate"] - no_df["implied_prob"]

    # Per-bin z-tests: NO outperforms YES?
    p_values = []
    for i in range(len(no_df)):
        no_wins = int(no_df.iloc[i]["wins"])
        no_n = int(no_df.iloc[i]["total_contracts"])
        yes_wins = int(yes_df.iloc[i]["wins"])
        yes_n = int(yes_df.iloc[i]["total_contracts"])
        _, p = two_proportion_z_test(no_wins, no_n, yes_wins, yes_n)
        p_values.append(p)

    corrected_p = bonferroni_correct(p_values)
    no_df["p_value_corrected"] = corrected_p

    # Find bin with largest NO advantage (excess_return_no - excess_return_yes)
    no_advantage = no_df["excess_return"].values - yes_df["excess_return"].values
    best_bin_idx = int(np.argmax(no_advantage))
    best_bin_midpoint = no_df.iloc[best_bin_idx]["bin_midpoint"]
    best_bin_no_adv = no_advantage[best_bin_idx]

    # Overall YES vs NO significance test
    total_yes_wins = int(yes_df["wins"].sum())
    total_yes_n = int(yes_df["total_contracts"].sum())
    total_no_wins = int(no_df["wins"].sum())
    total_no_n = int(no_df["total_contracts"].sum())
    z_stat, p_value = two_proportion_z_test(
        total_no_wins, total_no_n, total_yes_wins, total_yes_n
    )

    yes_overall_wr = total_yes_wins / total_yes_n * 100
    no_overall_wr = total_no_wins / total_no_n * 100
    gap_pp = no_overall_wr - yes_overall_wr

    log.info(
        "YES WR: %.2f%%, NO WR: %.2f%%, Gap: %.2f pp, z=%.2f, p=%.2e",
        yes_overall_wr, no_overall_wr, gap_pp, z_stat, p_value,
    )

    # --- Figure 1: Calibration curves ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax = plt.subplots(figsize=(10, 7))

    ax.plot(
        yes_df["bin_midpoint"], yes_df["win_rate"],
        "o-", color="#C44E52", linewidth=2, markersize=8, label="YES-taker",
    )
    ax.plot(
        no_df["bin_midpoint"], no_df["win_rate"],
        "s-", color="#4C72B0", linewidth=2, markersize=8, label="NO-taker",
    )
    ax.plot([0, 100], [0, 100], "--", color="gray", linewidth=1, label="Fair value")
    ax.set_xlabel("Taker Price (cents)", fontsize=12)
    ax.set_ylabel("Win Rate (%)", fontsize=12)
    ax.set_title(
        f"YES vs. NO Taker Calibration (gap: {gap_pp:+.2f}pp, p={p_value:.1e})",
        fontsize=14, fontweight="bold",
    )
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.legend(fontsize=11)

    fig1.tight_layout()
    fig1_path = figures_dir / "yes_no_calibration.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Excess return grouped bars ---
    fig2, ax2 = plt.subplots(figsize=(10, 6))

    x = np.arange(len(yes_df))
    width = 0.35
    ax2.bar(
        x - width / 2, yes_df["excess_return"],
        width, label="YES-taker", color="#C44E52", alpha=0.8,
    )
    ax2.bar(
        x + width / 2, no_df["excess_return"],
        width, label="NO-taker", color="#4C72B0", alpha=0.8,
    )
    ax2.axhline(y=0, color="black", linewidth=0.5)
    ax2.set_xlabel("Price Bin (cents)", fontsize=12)
    ax2.set_ylabel("Excess Return (pp)", fontsize=12)
    ax2.set_title("Excess Return by Price Bin: YES vs. NO Takers", fontsize=14, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"{int(m)}" for m in yes_df["bin_midpoint"]])
    ax2.legend(fontsize=11)

    fig2.tight_layout()
    fig2_path = figures_dir / "yes_no_excess_return.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- CSV ---
    csv_path = csv_dir / "yes_no_asymmetry.csv"
    df[["taker_side", "bin_start", "bin_midpoint", "win_rate", "implied_prob",
        "excess_return", "wins", "total_contracts"]].to_csv(csv_path, index=False)

    # --- Summary ---
    summary = (
        f"YES-taker WR: {yes_overall_wr:.2f}%, NO-taker WR: {no_overall_wr:.2f}%, "
        f"gap: {gap_pp:+.2f}pp (z={z_stat:.1f}, p={p_value:.1e}). "
        f"Largest NO advantage at {best_bin_midpoint:.0f}c bin: "
        f"{best_bin_no_adv:+.2f}pp excess return difference."
    )

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path], csv_path=csv_path, summary=summary,
    )
