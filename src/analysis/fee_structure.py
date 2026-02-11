"""Fee structure calibration analysis: quadratic vs quadratic_with_maker_fees.

Analyzes whether different fee types create different calibration patterns by
joining trades -> markets -> events -> series. Compares taker calibration
curves and excess returns across fee regimes to identify structural differences
in market efficiency driven by fee design.
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
    with_fee_type_sql,
)
from util.stats import calibration_error

log = logging.getLogger(__name__)

FEE_TYPE_COLORS = {
    "quadratic": "#4C72B0",
    "quadratic_with_maker_fees": "#55A868",
    "unknown": "#CCCCCC",
}

MIN_CONTRACTS_THRESHOLD = 1_000_000


def _fee_trade_outcomes_sql(data_dir: Path) -> str:
    """SQL for a CTE decomposing trades into taker/maker outcomes with fee type.

    Depends on: markets_with_fees CTE.
    Columns: fee_type, fee_multiplier, taker_price, taker_won, maker_won, contracts
    """
    return f"""
        SELECT
            mf.fee_type,
            mf.fee_multiplier,
            CASE WHEN t.taker_side = 'yes'
                 THEN CAST(t.yes_price_dollars AS DOUBLE) * 100
                 ELSE CAST(t.no_price_dollars AS DOUBLE) * 100
            END AS taker_price,
            CASE WHEN t.taker_side = mf.result THEN 1 ELSE 0 END AS taker_won,
            CASE WHEN t.taker_side != mf.result THEN 1 ELSE 0 END AS maker_won,
            CAST(t.count_fp AS DOUBLE) AS contracts
        FROM '{data_dir}/trades/*.parquet' t
        INNER JOIN markets_with_fees mf ON t.ticker = mf.ticker
    """


def run(data_dir: Path, output_dir: Path, n_bins: int = 10) -> AnalysisResult:
    """Run fee structure calibration analysis.

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

    log.info("Running fee structure analysis (%d bins, width=%.1f)...", n_bins, bin_width)

    ctes = [
        ("resolved_markets", resolved_markets_sql(data_dir)),
        ("markets_with_fees", with_fee_type_sql(data_dir)),
        ("fee_trades", _fee_trade_outcomes_sql(data_dir)),
    ]

    query = build_query(
        ctes=ctes,
        select=f"""
            SELECT
                fee_type,
                FLOOR(taker_price / {bin_width}) * {bin_width} AS bin_start,
                FLOOR(taker_price / {bin_width}) * {bin_width} + {bin_width} / 2.0
                    AS bin_midpoint,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) * 100 AS taker_win_rate,
                SUM(CASE WHEN maker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) * 100 AS maker_win_rate,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM fee_trades
            WHERE taker_price > 0 AND taker_price < 100
            GROUP BY fee_type, bin_start
            ORDER BY fee_type, bin_start
        """,
    )
    df = con.execute(query).df()
    con.close()

    validate_row_count(df, 1, "Fee structure bins")

    # Derived columns
    df["implied_prob"] = df["bin_midpoint"]
    df["taker_excess_return"] = df["taker_win_rate"] - df["implied_prob"]
    df["maker_excess_return"] = df["maker_win_rate"] - (100 - df["implied_prob"])

    # Group by fee type and compute per-type metrics
    fee_types = df["fee_type"].unique()
    log.info("Found %d fee types: %s", len(fee_types), list(fee_types))

    type_metrics = {}
    for ft in fee_types:
        ft_df = df[df["fee_type"] == ft].reset_index(drop=True)
        ft_total = ft_df["total_contracts"].sum()

        if ft_total < MIN_CONTRACTS_THRESHOLD:
            log.info("Skipping fee type '%s' (%.0f contracts < %d threshold)",
                     ft, ft_total, MIN_CONTRACTS_THRESHOLD)
            continue

        validate_prices(ft_df, "taker_win_rate")
        cal_err = calibration_error(ft_df, "taker_win_rate", "implied_prob", "total_contracts")

        taker_wins = (ft_df["taker_win_rate"] / 100 * ft_df["total_contracts"]).sum()
        maker_wins = (ft_df["maker_win_rate"] / 100 * ft_df["total_contracts"]).sum()
        overall_taker_wr = taker_wins / ft_total * 100
        overall_maker_wr = maker_wins / ft_total * 100

        type_metrics[ft] = {
            "df": ft_df,
            "total_contracts": ft_total,
            "cal_err": cal_err,
            "taker_wr": overall_taker_wr,
            "maker_wr": overall_maker_wr,
        }
        log.info(
            "Fee type '%s': %.0f contracts, cal_err=%.2fpp, taker_wr=%.2f%%, maker_wr=%.2f%%",
            ft, ft_total, cal_err, overall_taker_wr, overall_maker_wr,
        )

    qualified_types = sorted(type_metrics.keys())
    log.info(
        "%d fee types with >= %d contracts: %s",
        len(qualified_types), MIN_CONTRACTS_THRESHOLD, qualified_types,
    )

    # --- Figure 1: Calibration curves by fee type ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax1 = plt.subplots(figsize=(10, 7))

    ax1.plot([0, 100], [0, 100], "--", color="gray", linewidth=1, label="Perfect calibration")

    for ft in qualified_types:
        m = type_metrics[ft]
        ft_df = m["df"]
        color = FEE_TYPE_COLORS.get(ft, "#999999")
        label = f"{ft} (cal err: {m['cal_err']:.2f}pp)"
        ax1.plot(
            ft_df["bin_midpoint"], ft_df["taker_win_rate"],
            "o-", color=color, linewidth=2, markersize=7, label=label,
        )

    ax1.set_xlabel("Taker Price (cents)", fontsize=12)
    ax1.set_ylabel("Taker Win Rate (%)", fontsize=12)
    ax1.set_title("Taker Calibration by Fee Type", fontsize=14, fontweight="bold")
    ax1.set_xlim(0, 100)
    ax1.set_ylim(0, 100)
    ax1.legend(fontsize=10, loc="upper left")

    fig1.tight_layout()
    fig1_path = figures_dir / "fee_calibration_by_type.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Excess return comparison ---
    fig2, ax2 = plt.subplots(figsize=(10, 6))

    if qualified_types:
        ref_df = type_metrics[qualified_types[0]]["df"]
        x = np.arange(len(ref_df))
        n_types = len(qualified_types)
        bar_width = 0.7 / max(n_types, 1)

        for i, ft in enumerate(qualified_types):
            ft_df = type_metrics[ft]["df"]
            color = FEE_TYPE_COLORS.get(ft, "#999999")
            offset = (i - (n_types - 1) / 2) * bar_width
            ax2.bar(
                x + offset, ft_df["taker_excess_return"],
                bar_width, label=ft, color=color, alpha=0.8,
            )

        ax2.axhline(y=0, color="black", linewidth=0.5)
        ax2.set_xlabel("Price Bin (cents)", fontsize=12)
        ax2.set_ylabel("Taker Excess Return (pp)", fontsize=12)
        ax2.set_title("Taker Excess Return by Fee Type", fontsize=14, fontweight="bold")
        ax2.set_xticks(x)
        ax2.set_xticklabels([f"{int(m)}" for m in ref_df["bin_midpoint"]])
        ax2.legend(fontsize=10)

    fig2.tight_layout()
    fig2_path = figures_dir / "fee_excess_return_comparison.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- CSV ---
    csv_path = csv_dir / "fee_structure.csv"
    df[["fee_type", "bin_start", "bin_midpoint", "taker_win_rate", "maker_win_rate",
        "taker_excess_return", "maker_excess_return", "total_contracts",
        "trade_count"]].to_csv(csv_path, index=False)

    # --- Summary ---
    summary_parts = [
        f"{len(qualified_types)} fee type(s) with >= {MIN_CONTRACTS_THRESHOLD:,} contracts.",
    ]

    for ft in qualified_types:
        m = type_metrics[ft]
        summary_parts.append(
            f"  {ft}: cal_err={m['cal_err']:.2f}pp, "
            f"taker_wr={m['taker_wr']:.2f}%, maker_wr={m['maker_wr']:.2f}%, "
            f"volume={m['total_contracts']:,.0f} contracts."
        )

    if len(qualified_types) >= 2:
        ft_a, ft_b = qualified_types[0], qualified_types[1]
        cal_diff = type_metrics[ft_a]["cal_err"] - type_metrics[ft_b]["cal_err"]
        taker_diff = type_metrics[ft_a]["taker_wr"] - type_metrics[ft_b]["taker_wr"]
        summary_parts.append(
            f"Calibration error difference ({ft_a} - {ft_b}): {cal_diff:+.2f}pp. "
            f"Taker win rate difference: {taker_diff:+.2f}pp."
        )
    elif len(qualified_types) == 1:
        summary_parts.append("Only one fee type has sufficient data for comparison.")

    summary = " ".join(summary_parts)

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path], csv_path=csv_path, summary=summary,
    )
