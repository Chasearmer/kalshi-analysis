"""Day-of-week and quarterly seasonality analysis of taker/maker returns.

Aggregates trade outcomes by day of week and calendar quarter to detect
temporal patterns in maker-taker edge. Tests whether day-of-week differences
are statistically significant using a chi-squared independence test.
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
    """Run day-of-week and quarterly seasonality analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()

    ctes = [
        ("resolved_markets", resolved_markets_sql(data_dir)),
        ("trade_outcomes", trade_outcomes_sql(data_dir)),
    ]

    # --- Query 1: Day of week ---
    log.info("Running day-of-week query...")
    et = "CAST(created_time AS TIMESTAMPTZ) AT TIME ZONE 'America/New_York'"
    dow_select = f"""
        SELECT
            DAYNAME({et}) AS day_name,
            EXTRACT(DOW FROM {et}) AS day_num,
            SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                AS taker_wins,
            SUM(CASE WHEN maker_won = 1 THEN contracts ELSE 0 END)
                AS maker_wins,
            SUM(contracts) AS total_contracts,
            SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                / SUM(contracts) * 100 AS taker_win_rate,
            SUM(CASE WHEN maker_won = 1 THEN contracts ELSE 0 END)
                / SUM(contracts) * 100 AS maker_win_rate,
            SUM(taker_price * contracts) / SUM(contracts)
                AS avg_taker_price,
            COUNT(*) AS trade_count
        FROM trade_outcomes
        GROUP BY day_name, day_num
        ORDER BY day_num
    """
    dow_query = build_query(ctes, dow_select)
    dow_df = con.execute(dow_query).df()

    # --- Query 2: Quarterly seasonality ---
    log.info("Running quarterly seasonality query...")
    quarterly_select = f"""
        SELECT
            DATE_TRUNC('quarter', {et}) AS quarter,
            SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                / SUM(contracts) * 100 AS taker_win_rate,
            SUM(CASE WHEN maker_won = 1 THEN contracts ELSE 0 END)
                / SUM(contracts) * 100 AS maker_win_rate,
            SUM(taker_price * contracts) / SUM(contracts)
                AS avg_taker_price,
            SUM(contracts) AS total_contracts,
            COUNT(*) AS trade_count
        FROM trade_outcomes
        GROUP BY quarter
        ORDER BY quarter
    """
    quarterly_query = build_query(ctes, quarterly_select)
    quarterly_df = con.execute(quarterly_query).df()
    con.close()

    # --- Validations ---
    validate_row_count(dow_df, 7, "Day-of-week rows")
    validate_row_count(quarterly_df, 1, "Quarterly rows")

    # --- Day-of-week analysis ---
    dow_df["taker_excess"] = dow_df["taker_win_rate"] - dow_df["avg_taker_price"]
    dow_df["maker_excess"] = dow_df["maker_win_rate"] - (100 - dow_df["avg_taker_price"])

    log.info("Day-of-week taker excess range: %.2f to %.2f pp",
             dow_df["taker_excess"].min(), dow_df["taker_excess"].max())

    # Chi-squared test: is day-of-week independent of taker win/loss?
    contingency = np.column_stack([
        dow_df["taker_wins"].values,
        dow_df["total_contracts"].values - dow_df["taker_wins"].values,
    ])
    chi2, p_value, dof = chi_squared_independence(contingency)
    log.info("Chi-squared test: chi2=%.2f, p=%.2e, dof=%d", chi2, p_value, dof)

    # Best/worst day for takers
    best_day_idx = dow_df["taker_excess"].idxmax()
    worst_day_idx = dow_df["taker_excess"].idxmin()
    best_day = dow_df.loc[best_day_idx]
    worst_day = dow_df.loc[worst_day_idx]

    # --- Quarterly analysis ---
    quarterly_df["taker_excess"] = quarterly_df["taker_win_rate"] - quarterly_df["avg_taker_price"]
    quarterly_df["maker_excess"] = (
        quarterly_df["maker_win_rate"] - (100 - quarterly_df["avg_taker_price"])
    )

    # Filter for significant quarters (>= 100M contracts)
    significant_mask = quarterly_df["total_contracts"] >= 100_000_000

    # Quarter labels for display
    quarterly_df["quarter_label"] = quarterly_df["quarter"].apply(
        lambda q: f"{q.year}-Q{(q.month - 1) // 3 + 1}"
    )

    # Detect quarterly trend (correlation of taker excess with time for significant quarters)
    sig_quarters = quarterly_df[significant_mask]
    if len(sig_quarters) >= 3:
        quarter_indices = np.arange(len(sig_quarters))
        corr = np.corrcoef(quarter_indices, sig_quarters["taker_excess"].values)[0, 1]
        if corr > 0.3:
            trend_desc = f"improving trend (r={corr:.2f})"
        elif corr < -0.3:
            trend_desc = f"worsening trend (r={corr:.2f})"
        else:
            trend_desc = f"no clear trend (r={corr:.2f})"
    else:
        trend_desc = "insufficient data for trend"

    # --- Figure 1: Day-of-week grouped bar chart ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax1 = plt.subplots(figsize=(10, 6))

    x = np.arange(len(dow_df))
    width = 0.35
    ax1.bar(
        x - width / 2, dow_df["taker_excess"],
        width, label="Taker excess", color="#C44E52", alpha=0.8,
    )
    ax1.bar(
        x + width / 2, dow_df["maker_excess"],
        width, label="Maker excess", color="#4C72B0", alpha=0.8,
    )
    ax1.axhline(y=0, color="black", linewidth=0.5)
    ax1.set_xlabel("Day of Week", fontsize=12)
    ax1.set_ylabel("Excess Return (pp)", fontsize=12)
    ax1.set_title(
        f"Excess Return by Day of Week (chi-squared p={p_value:.2e})",
        fontsize=14, fontweight="bold",
    )
    ax1.set_xticks(x)
    ax1.set_xticklabels(dow_df["day_name"])
    ax1.legend(fontsize=11)

    fig1.tight_layout()
    fig1_path = figures_dir / "day_of_week_returns.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Seasonality dual-axis chart ---
    fig2, ax2 = plt.subplots(figsize=(12, 6))

    # Right axis: volume bars (behind the lines)
    ax3 = ax2.twinx()
    ax3.bar(
        np.arange(len(quarterly_df)),
        quarterly_df["total_contracts"],
        color="gray", alpha=0.3, label="Total contracts",
    )
    ax3.set_ylabel("Total Contracts", fontsize=12)

    # Left axis: excess return lines
    # Filled markers for significant quarters, open for others
    for i, row in quarterly_df.iterrows():
        is_sig = row["total_contracts"] >= 100_000_000
        ax2.plot(
            i, row["taker_excess"],
            marker="o" if is_sig else "o",
            markerfacecolor="#C44E52" if is_sig else "none",
            markeredgecolor="#C44E52",
            markersize=8,
            zorder=5,
        )
        ax2.plot(
            i, row["maker_excess"],
            marker="s" if is_sig else "s",
            markerfacecolor="#4C72B0" if is_sig else "none",
            markeredgecolor="#4C72B0",
            markersize=8,
            zorder=5,
        )

    # Draw connecting lines
    ax2.plot(
        np.arange(len(quarterly_df)), quarterly_df["taker_excess"],
        color="#C44E52", linewidth=2, label="Taker excess", zorder=4,
    )
    ax2.plot(
        np.arange(len(quarterly_df)), quarterly_df["maker_excess"],
        color="#4C72B0", linewidth=2, label="Maker excess", zorder=4,
    )
    ax2.axhline(y=0, color="black", linewidth=0.5)
    ax2.set_xlabel("Quarter", fontsize=12)
    ax2.set_ylabel("Excess Return (pp)", fontsize=12)
    ax2.set_title("Quarterly Seasonality of Excess Returns", fontsize=14, fontweight="bold")
    ax2.set_xticks(np.arange(len(quarterly_df)))
    ax2.set_xticklabels(quarterly_df["quarter_label"], rotation=45, ha="right")
    ax2.legend(loc="upper left", fontsize=11)
    ax3.legend(loc="upper right", fontsize=11)

    fig2.tight_layout()
    fig2_path = figures_dir / "seasonality_returns.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- CSVs ---
    dow_csv_path = csv_dir / "day_of_week.csv"
    dow_df[[
        "day_name", "day_num", "taker_win_rate", "maker_win_rate",
        "taker_excess", "maker_excess", "avg_taker_price",
        "total_contracts", "trade_count",
    ]].rename(columns={
        "taker_excess": "taker_excess_return",
        "maker_excess": "maker_excess_return",
    }).to_csv(dow_csv_path, index=False)

    seasonality_csv_path = csv_dir / "seasonality.csv"
    quarterly_df[[
        "quarter", "taker_win_rate", "maker_win_rate",
        "taker_excess", "maker_excess", "avg_taker_price",
        "total_contracts", "trade_count",
    ]].rename(columns={
        "taker_excess": "taker_excess_return",
        "maker_excess": "maker_excess_return",
    }).to_csv(seasonality_csv_path, index=False)

    # --- Summary ---
    summary = (
        f"Day-of-week chi-squared: chi2={chi2:.1f}, p={p_value:.2e} (dof={dof}). "
        f"Best day for takers: {best_day['day_name']} "
        f"(excess {best_day['taker_excess']:+.2f}pp). "
        f"Worst day for takers: {worst_day['day_name']} "
        f"(excess {worst_day['taker_excess']:+.2f}pp). "
        f"Quarterly taker excess: {trend_desc}."
    )

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path],
        csv_path=dow_csv_path,
        summary=summary,
    )
