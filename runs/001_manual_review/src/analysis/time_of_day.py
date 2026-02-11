"""Intraday patterns in taker/maker returns by hour of day (Eastern Time).

Groups all resolved trades by the hour they occurred (converted to US Eastern),
computes contract-weighted taker and maker win rates per hour, and tests whether
taker win rate is independent of hour using a chi-squared test.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from analysis.base import AnalysisResult, ensure_output_dirs, validate_row_count
from util.queries import build_query, get_connection, resolved_markets_sql, trade_outcomes_sql
from util.stats import chi_squared_independence

log = logging.getLogger(__name__)


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run time-of-day analysis on taker/maker returns.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()

    log.info("Running time-of-day query...")
    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("trade_outcomes", trade_outcomes_sql(data_dir)),
        ],
        select="""
            SELECT
                EXTRACT(HOUR FROM CAST(created_time AS TIMESTAMPTZ)
                    AT TIME ZONE 'America/New_York') AS et_hour,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END) AS taker_wins,
                SUM(CASE WHEN maker_won = 1 THEN contracts ELSE 0 END) AS maker_wins,
                SUM(contracts) AS total_contracts,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) * 100 AS taker_win_rate,
                SUM(CASE WHEN maker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) * 100 AS maker_win_rate,
                SUM(taker_price * contracts) / SUM(contracts) AS avg_taker_price,
                COUNT(*) AS trade_count
            FROM trade_outcomes
            GROUP BY et_hour
            ORDER BY et_hour
        """,
    )
    df = con.execute(query).df()
    con.close()

    # Validations
    validate_row_count(df, 20, "Time-of-day hours")
    log.info("Hours with data: %d, total contracts: %s",
             len(df), f"{df['total_contracts'].sum():,.0f}")

    # Compute excess returns
    df["taker_excess_return"] = df["taker_win_rate"] - df["avg_taker_price"]
    df["maker_excess_return"] = df["maker_win_rate"] - (100 - df["avg_taker_price"])

    # Chi-squared test: is taker win rate independent of hour?
    taker_losses = df["total_contracts"] - df["taker_wins"]
    contingency_table = np.column_stack([
        df["taker_wins"].values,
        taker_losses.values,
    ])
    chi2, p_value, dof = chi_squared_independence(contingency_table)
    significant = p_value < 0.01

    log.info("Chi-squared: %.2f, p=%.2e, dof=%d, significant at 0.01: %s",
             chi2, p_value, dof, significant)

    # Best and worst hours for takers
    best_idx = df["taker_excess_return"].idxmax()
    worst_idx = df["taker_excess_return"].idxmin()
    best_hour = int(df.loc[best_idx, "et_hour"])
    worst_hour = int(df.loc[worst_idx, "et_hour"])
    best_excess = df.loc[best_idx, "taker_excess_return"]
    worst_excess = df.loc[worst_idx, "taker_excess_return"]

    # --- Figure 1: Returns by hour ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax1 = plt.subplots(figsize=(12, 6))

    ax1.plot(
        df["et_hour"], df["taker_excess_return"],
        "o-", color="#C44E52", linewidth=2, markersize=6, label="Taker excess return",
    )
    ax1.plot(
        df["et_hour"], df["maker_excess_return"],
        "s-", color="#4C72B0", linewidth=2, markersize=6, label="Maker excess return",
    )
    ax1.axhline(y=0, color="black", linewidth=0.8, linestyle="-")
    ax1.set_xlabel("Hour (Eastern Time)", fontsize=12)
    ax1.set_ylabel("Excess Return (pp)", fontsize=12)
    ax1.set_title(
        f"Taker/Maker Excess Returns by Hour of Day "
        f"(\u03c7\u00b2 p={p_value:.2e})",
        fontsize=14, fontweight="bold",
    )
    ax1.set_xticks(range(24))
    ax1.set_xlim(-0.5, 23.5)
    ax1.legend(loc="best", fontsize=11)

    # Secondary y-axis for volume
    ax1b = ax1.twinx()
    ax1b.bar(
        df["et_hour"], df["total_contracts"],
        alpha=0.15, color="gray", width=0.8, label="Volume",
    )
    ax1b.set_ylabel("Total Contracts", fontsize=12)
    ax1b.tick_params(axis="y", labelcolor="gray")

    fig1.tight_layout()
    fig1_path = figures_dir / "time_of_day_returns.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Volume by hour ---
    fig2, ax2 = plt.subplots(figsize=(12, 5))

    ax2.bar(
        df["et_hour"], df["total_contracts"],
        color="#4C72B0", alpha=0.8, width=0.8,
    )
    ax2.set_xlabel("Hour (Eastern Time)", fontsize=12)
    ax2.set_ylabel("Total Contracts", fontsize=12)
    ax2.set_title("Trading Volume by Hour of Day", fontsize=14, fontweight="bold")
    ax2.set_xticks(range(24))
    ax2.set_xlim(-0.5, 23.5)

    fig2.tight_layout()
    fig2_path = figures_dir / "time_of_day_volume.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- CSV ---
    csv_path = csv_dir / "time_of_day.csv"
    df[["et_hour", "taker_win_rate", "maker_win_rate", "taker_excess_return",
        "maker_excess_return", "avg_taker_price", "total_contracts",
        "trade_count"]].to_csv(csv_path, index=False)

    # --- Summary ---
    sig_label = "IS" if significant else "is NOT"
    summary = (
        f"Chi-squared test: \u03c7\u00b2={chi2:.1f}, p={p_value:.2e}, dof={dof}. "
        f"Taker win rate {sig_label} significantly dependent on hour (at 0.01 level). "
        f"Best hour for takers: {best_hour}:00 ET (excess {best_excess:+.2f}pp). "
        f"Worst hour for takers: {worst_hour}:00 ET (excess {worst_excess:+.2f}pp)."
    )

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path], csv_path=csv_path, summary=summary,
    )
