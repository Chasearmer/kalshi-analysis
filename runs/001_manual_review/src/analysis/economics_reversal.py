"""Economics reverse-S and Elections directional bias strategy analysis.

Round 2 found that Economics markets show a distinctive "reverse-S" calibration
pattern: strong longshot bias at low prices and favorite underpayment at high
prices. Elections markets show extreme miscalibration (12.90pp). This analysis
estimates whether these patterns survive fee costs as trading strategies.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.base import (
    AnalysisResult,
    ensure_output_dirs,
    validate_row_count,
)
from util.fees import kalshi_fee_cents
from util.queries import (
    build_query,
    get_connection,
    resolved_markets_sql,
    with_fee_type_sql,
)
from util.strategy import daily_capacity, kelly_fraction, payout_ratio_from_price

log = logging.getLogger(__name__)

DATASET_DAYS = 1680.0
TARGET_CATEGORIES = ["Economics", "Elections"]


def _cat_fee_trades_sql(data_dir: Path) -> str:
    """SQL CTE for trade outcomes with category and fee info.

    Depends on: resolved_markets, markets_with_fees CTEs.
    """
    return f"""
        SELECT
            t.ticker,
            t.taker_side,
            CASE WHEN t.taker_side = 'yes'
                 THEN CAST(t.yes_price_dollars AS DOUBLE) * 100
                 ELSE CAST(t.no_price_dollars AS DOUBLE) * 100
            END AS taker_price,
            CASE WHEN t.taker_side = mf.result THEN 1 ELSE 0 END AS taker_won,
            CAST(t.count_fp AS DOUBLE) AS contracts,
            mf.category,
            mf.fee_multiplier
        FROM '{data_dir}/trades/*.parquet' t
        INNER JOIN markets_with_fees mf ON t.ticker = mf.ticker
        WHERE mf.category IN ({", ".join(f"'{c}'" for c in TARGET_CATEGORIES)})
    """


def run(data_dir: Path, output_dir: Path, n_bins: int = 10) -> AnalysisResult:
    """Run Economics reversal and Elections directional bias analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.
        n_bins: Number of price bins.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()
    bin_width = 100.0 / n_bins

    log.info("Running Economics reversal / Elections analysis...")

    # --- Query: calibration by category and price bin ---
    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("markets_with_fees", with_fee_type_sql(data_dir)),
            ("cat_fee_trades", _cat_fee_trades_sql(data_dir)),
        ],
        select=f"""
            SELECT
                category,
                taker_side,
                FLOOR(taker_price / {bin_width}) * {bin_width} AS bin_start,
                FLOOR(taker_price / {bin_width}) * {bin_width} + {bin_width} / 2.0
                    AS bin_midpoint,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) AS win_rate,
                SUM(taker_price * contracts) / SUM(contracts) AS avg_price,
                AVG(fee_multiplier) AS avg_fee_mult,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM cat_fee_trades
            WHERE taker_price > 0 AND taker_price < 100
            GROUP BY category, taker_side, bin_start
            ORDER BY category, taker_side, bin_start
        """,
    )
    df = con.execute(query).df()
    con.close()

    validate_row_count(df, 1, "Economics/Elections bins")

    # Compute metrics
    df["win_rate_pct"] = df["win_rate"] * 100
    df["gross_edge_pp"] = df["win_rate_pct"] - df["bin_midpoint"]
    df["fee_cost_pp"] = df.apply(
        lambda r: kalshi_fee_cents(r["avg_price"], r["avg_fee_mult"]), axis=1
    )
    df["net_edge_pp"] = df["gross_edge_pp"] - df["fee_cost_pp"]

    # --- Define sub-strategies ---
    strategies = []

    for cat in TARGET_CATEGORIES:
        cat_df = df[df["category"] == cat]
        if cat_df.empty:
            continue

        # Strategy: Buy favorites (YES-taker > 70c)
        fav = cat_df[(cat_df["taker_side"] == "yes") & (cat_df["bin_midpoint"] >= 75)]
        if not fav.empty:
            total_c = fav["total_contracts"].sum()
            wr = (fav["win_rate"] * fav["total_contracts"]).sum() / total_c
            avg_p = (fav["avg_price"] * fav["total_contracts"]).sum() / total_c
            avg_fm = (fav["avg_fee_mult"] * fav["total_contracts"]).sum() / total_c
            gross = wr * 100 - avg_p
            fee = kalshi_fee_cents(avg_p, avg_fm)
            net = gross - fee
            pr = payout_ratio_from_price(min(max(avg_p, 1), 99))
            strategies.append(
                {
                    "category": cat,
                    "strategy": "favorites_yes_70c+",
                    "description": f"{cat}: YES-taker >=70c",
                    "gross_edge_pp": gross,
                    "fee_cost_pp": fee,
                    "net_edge_pp": net,
                    "win_rate_pct": wr * 100,
                    "avg_price": avg_p,
                    "total_contracts": total_c,
                    "daily_cap": daily_capacity(total_c, DATASET_DAYS),
                    "kelly": kelly_fraction(wr, pr),
                }
            )

        # Strategy: Sell longshots (NO-taker at low prices, i.e., taker_side='no', price < 30c)
        long = cat_df[(cat_df["taker_side"] == "no") & (cat_df["bin_midpoint"] <= 25)]
        if not long.empty:
            total_c = long["total_contracts"].sum()
            wr = (long["win_rate"] * long["total_contracts"]).sum() / total_c
            avg_p = (long["avg_price"] * long["total_contracts"]).sum() / total_c
            avg_fm = (long["avg_fee_mult"] * long["total_contracts"]).sum() / total_c
            gross = wr * 100 - avg_p
            fee = kalshi_fee_cents(avg_p, avg_fm)
            net = gross - fee
            pr = payout_ratio_from_price(min(max(avg_p, 1), 99))
            strategies.append(
                {
                    "category": cat,
                    "strategy": "longshots_no_30c-",
                    "description": f"{cat}: NO-taker <=30c",
                    "gross_edge_pp": gross,
                    "fee_cost_pp": fee,
                    "net_edge_pp": net,
                    "win_rate_pct": wr * 100,
                    "avg_price": avg_p,
                    "total_contracts": total_c,
                    "daily_cap": daily_capacity(total_c, DATASET_DAYS),
                    "kelly": kelly_fraction(wr, pr),
                }
            )

        # Strategy: NO-taker at all prices (overall directional bias)
        no_all = cat_df[cat_df["taker_side"] == "no"]
        if not no_all.empty:
            total_c = no_all["total_contracts"].sum()
            wr = (no_all["win_rate"] * no_all["total_contracts"]).sum() / total_c
            avg_p = (no_all["avg_price"] * no_all["total_contracts"]).sum() / total_c
            avg_fm = (no_all["avg_fee_mult"] * no_all["total_contracts"]).sum() / total_c
            gross = wr * 100 - avg_p
            fee = kalshi_fee_cents(avg_p, avg_fm)
            net = gross - fee
            pr = payout_ratio_from_price(min(max(avg_p, 1), 99))
            strategies.append(
                {
                    "category": cat,
                    "strategy": "no_all_prices",
                    "description": f"{cat}: NO-taker all prices",
                    "gross_edge_pp": gross,
                    "fee_cost_pp": fee,
                    "net_edge_pp": net,
                    "win_rate_pct": wr * 100,
                    "avg_price": avg_p,
                    "total_contracts": total_c,
                    "daily_cap": daily_capacity(total_c, DATASET_DAYS),
                    "kelly": kelly_fraction(wr, pr),
                }
            )

    strat_df = pd.DataFrame(strategies) if strategies else pd.DataFrame()

    # --- Figure 1: Calibration with strategy zones ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, axes = plt.subplots(1, len(TARGET_CATEGORIES), figsize=(12, 5))
    if len(TARGET_CATEGORIES) == 1:
        axes = [axes]

    cat_colors = {"Economics": "#4C72B0", "Elections": "#C44E52"}
    for idx, cat in enumerate(TARGET_CATEGORIES):
        ax = axes[idx]
        cat_all = df[(df["category"] == cat)]

        # Aggregate across taker_side for calibration
        def _weighted_wr(x):
            weights = cat_all.loc[x.index, "total_contracts"]
            return (x * weights).sum() / weights.sum()

        cat_bins = (
            cat_all.groupby("bin_midpoint")
            .agg(
                win_rate_pct=("win_rate_pct", _weighted_wr),
                total_contracts=("total_contracts", "sum"),
            )
            .reset_index()
        )

        if cat_bins.empty:
            ax.set_title(f"{cat}\n(no data)", fontsize=10)
            continue

        color = cat_colors.get(cat, "#333333")
        ax.plot(
            cat_bins["bin_midpoint"],
            cat_bins["win_rate_pct"],
            "o-",
            color=color,
            linewidth=2,
            markersize=5,
        )
        ax.plot([0, 100], [0, 100], "--", color="gray", linewidth=1)

        # Highlight strategy zones
        ax.axvspan(0, 30, alpha=0.08, color="red", label="Longshots (<30c)")
        ax.axvspan(70, 100, alpha=0.08, color="green", label="Favorites (>70c)")

        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.set_title(cat, fontsize=12, fontweight="bold")
        ax.set_xlabel("Implied Prob (%)", fontsize=10)
        ax.set_ylabel("Win Rate (%)", fontsize=10)
        ax.legend(fontsize=7, loc="upper left")

    fig1.suptitle("Calibration with Strategy Zones", fontsize=14, fontweight="bold", y=1.02)
    fig1.tight_layout()
    fig1_path = figures_dir / "economics_calibration_strategies.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Net edge by sub-strategy ---
    fig2, ax2 = plt.subplots(figsize=(10, 6))

    if not strat_df.empty:
        strat_sorted = strat_df.sort_values("net_edge_pp", ascending=True)
        colors = ["#55A868" if e >= 0 else "#C44E52" for e in strat_sorted["net_edge_pp"]]
        y = np.arange(len(strat_sorted))
        ax2.barh(y, strat_sorted["net_edge_pp"], color=colors, alpha=0.85)
        ax2.axvline(x=0, color="black", linewidth=0.5)
        ax2.set_yticks(y)
        ax2.set_yticklabels(strat_sorted["description"], fontsize=9)
        ax2.set_xlabel("Net Edge (pp)", fontsize=12)
        ax2.set_title("Category Strategy Net Edge (after fees)", fontsize=14, fontweight="bold")
    else:
        ax2.text(
            0.5,
            0.5,
            "No qualifying strategies",
            transform=ax2.transAxes,
            ha="center",
            va="center",
            fontsize=14,
        )

    fig2.tight_layout()
    fig2_path = figures_dir / "economics_edge_by_strategy.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Figure 3: Capacity comparison ---
    fig3, ax3 = plt.subplots(figsize=(10, 5))

    if not strat_df.empty:
        strat_sorted_cap = strat_df.sort_values("daily_cap", ascending=True)
        y = np.arange(len(strat_sorted_cap))
        ax3.barh(y, strat_sorted_cap["daily_cap"], color="#4C72B0", alpha=0.85)
        ax3.set_yticks(y)
        ax3.set_yticklabels(strat_sorted_cap["description"], fontsize=9)
        ax3.set_xlabel("Daily Capacity (contracts/day)", fontsize=12)
        ax3.set_title("Strategy Capacity", fontsize=14, fontweight="bold")

    fig3.tight_layout()
    fig3_path = figures_dir / "economics_capacity.png"
    fig3.savefig(fig3_path, dpi=150, bbox_inches="tight")
    plt.close(fig3)

    # --- CSV ---
    csv_path = csv_dir / "economics_reversal.csv"
    if not strat_df.empty:
        strat_df.to_csv(csv_path, index=False)
    else:
        pd.DataFrame(
            columns=[
                "category",
                "strategy",
                "description",
                "gross_edge_pp",
                "fee_cost_pp",
                "net_edge_pp",
                "win_rate_pct",
                "avg_price",
                "total_contracts",
                "daily_cap",
                "kelly",
            ]
        ).to_csv(csv_path, index=False)

    # --- Summary ---
    if not strat_df.empty:
        best = strat_df.loc[strat_df["net_edge_pp"].idxmax()]
        n_positive = (strat_df["net_edge_pp"] > 0).sum()
        summary = (
            f"{len(strat_df)} sub-strategies evaluated across "
            f"{', '.join(TARGET_CATEGORIES)}. "
            f"{n_positive} have positive net edge after fees. "
            f"Best: {best['description']} "
            f"(net {best['net_edge_pp']:+.2f}pp, "
            f"capacity {best['daily_cap']:,.0f}/day, "
            f"kelly={best['kelly']:.3f})."
        )
    else:
        summary = "No qualifying data found for target categories."

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path, fig3_path],
        csv_path=csv_path,
        summary=summary,
    )
