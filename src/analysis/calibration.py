"""Overall calibration curve: actual win rate vs. implied probability.

Bins trades by taker price into deciles, computes contract-weighted win rate
within each bin, and compares against perfect calibration (y=x diagonal).
Reveals the longshot bias and overall market efficiency.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt

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
from util.stats import calibration_error

log = logging.getLogger(__name__)


def run(data_dir: Path, output_dir: Path, n_bins: int = 10) -> AnalysisResult:
    """Run calibration curve analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.
        n_bins: Number of price bins (default 10 = deciles).

    Returns:
        AnalysisResult with figure path, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()
    bin_width = 100.0 / n_bins

    log.info("Running calibration query (%d bins, width=%.1f)...", n_bins, bin_width)
    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("trade_outcomes", trade_outcomes_sql(data_dir)),
        ],
        select=f"""
            SELECT
                FLOOR(taker_price / {bin_width}) * {bin_width} AS bin_start,
                FLOOR(taker_price / {bin_width}) * {bin_width} + {bin_width} / 2.0
                    AS bin_midpoint,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) * 100 AS win_rate,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM trade_outcomes
            WHERE taker_price > 0 AND taker_price < 100
            GROUP BY bin_start
            ORDER BY bin_start
        """,
    )
    df = con.execute(query).df()
    con.close()

    # Validations
    validate_row_count(df, n_bins, "Calibration bins")
    validate_prices(df, "win_rate")
    total_contracts = df["total_contracts"].sum()
    log.info("Total contracts across all bins: %s", f"{total_contracts:,.0f}")

    # Compute derived columns
    df["implied_prob"] = df["bin_midpoint"]
    df["excess_return"] = df["win_rate"] - df["implied_prob"]

    cal_err = calibration_error(df, "win_rate", "implied_prob", "total_contracts")

    # --- Figure ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax1 = plt.subplots(figsize=(10, 7))

    ax1.plot(
        df["bin_midpoint"], df["win_rate"],
        "o-", color="#4C72B0", linewidth=2, markersize=8, label="Actual win rate",
    )
    ax1.plot([0, 100], [0, 100], "--", color="gray", linewidth=1, label="Perfect calibration")
    ax1.set_xlabel("Taker Price (cents)", fontsize=12)
    ax1.set_ylabel("Win Rate (%)", fontsize=12)
    ax1.set_title("Kalshi Taker Calibration Curve", fontsize=14, fontweight="bold")
    ax1.set_xlim(0, 100)
    ax1.set_ylim(0, 100)
    ax1.legend(loc="upper left")

    # Excess return bars on secondary axis
    ax2 = ax1.twinx()
    colors = ["#C44E52" if er < 0 else "#55A868" for er in df["excess_return"]]
    ax2.bar(
        df["bin_midpoint"], df["excess_return"],
        width=bin_width * 0.6, alpha=0.3, color=colors,
    )
    ax2.set_ylabel("Excess Return (pp)", fontsize=12)
    ax2.axhline(y=0, color="black", linewidth=0.5)

    # Annotate contract counts
    for _, row in df.iterrows():
        ax1.annotate(
            f"{row['total_contracts'] / 1e6:.0f}M",
            xy=(row["bin_midpoint"], row["win_rate"]),
            textcoords="offset points", xytext=(0, 12),
            fontsize=7, ha="center", color="gray",
        )

    fig.tight_layout()
    fig_path = figures_dir / "calibration_curve.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # --- CSV ---
    csv_path = csv_dir / "calibration_curve.csv"
    df[["bin_start", "bin_midpoint", "win_rate", "implied_prob", "excess_return",
        "total_contracts", "trade_count"]].to_csv(csv_path, index=False)

    # --- Summary ---
    low_bin = df.iloc[0]
    high_bin = df.iloc[-1]
    summary = (
        f"Longshot bias confirmed: takers at {low_bin['bin_midpoint']:.0f}c win "
        f"{low_bin['win_rate']:.1f}% vs implied {low_bin['implied_prob']:.0f}% "
        f"(excess {low_bin['excess_return']:+.1f}pp). "
        f"Favorites at {high_bin['bin_midpoint']:.0f}c win {high_bin['win_rate']:.1f}% "
        f"vs implied {high_bin['implied_prob']:.0f}% "
        f"(excess {high_bin['excess_return']:+.1f}pp). "
        f"Weighted calibration error: {cal_err:.2f}pp across {total_contracts:,.0f} contracts."
    )

    return AnalysisResult(
        figure_paths=[fig_path], csv_path=csv_path, summary=summary,
    )
