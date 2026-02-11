"""Fade YES strategy analysis: take NO side at prices >= 60c.

The primary strategy candidate from Round 2. The YES/NO asymmetry (13.7pp gap)
suggests that YES-takers overpay at high prices. This analysis estimates edge,
fees, net profitability, and capacity for a systematic NO-side strategy.
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

# Dataset spans ~1680 days (2021-06-30 to 2026-02-06)
DATASET_DAYS = 1680.0

MIN_PRICE = 60
MAX_PRICE = 100
PRICE_BIN_WIDTH = 10


def _strategy_trades_sql(data_dir: Path) -> str:
    """SQL CTE for NO-taker trades at >= 60c with fee info and category.

    Depends on: resolved_markets, markets_with_fees CTEs.
    """
    return f"""
        SELECT
            t.ticker,
            t.taker_side,
            CAST(t.no_price_dollars AS DOUBLE) * 100 AS taker_price,
            CASE WHEN t.taker_side = mf.result THEN 1 ELSE 0 END AS taker_won,
            CAST(t.count_fp AS DOUBLE) AS contracts,
            t.created_time,
            mf.category,
            mf.fee_type,
            mf.fee_multiplier
        FROM '{data_dir}/trades/*.parquet' t
        INNER JOIN markets_with_fees mf ON t.ticker = mf.ticker
        WHERE t.taker_side = 'no'
          AND CAST(t.no_price_dollars AS DOUBLE) * 100 >= {MIN_PRICE}
          AND CAST(t.no_price_dollars AS DOUBLE) * 100 < {MAX_PRICE}
    """


def run(data_dir: Path, output_dir: Path) -> AnalysisResult:
    """Run Fade YES strategy analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()

    log.info("Running Fade YES strategy analysis (NO-taker >= %dc)...", MIN_PRICE)

    # --- Query: aggregate by price bin ---
    query_price = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("markets_with_fees", with_fee_type_sql(data_dir)),
            ("strategy_trades", _strategy_trades_sql(data_dir)),
        ],
        select=f"""
            SELECT
                FLOOR(taker_price / {PRICE_BIN_WIDTH}) * {PRICE_BIN_WIDTH} AS price_bin,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) AS win_rate,
                SUM(taker_price * contracts) / SUM(contracts) AS avg_price,
                AVG(fee_multiplier) AS avg_fee_mult,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM strategy_trades
            GROUP BY price_bin
            ORDER BY price_bin
        """,
    )
    price_df = con.execute(query_price).df()

    # --- Query: aggregate by category (top 5) ---
    query_cat = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("markets_with_fees", with_fee_type_sql(data_dir)),
            ("strategy_trades", _strategy_trades_sql(data_dir)),
        ],
        select="""
            SELECT
                category,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) AS win_rate,
                SUM(taker_price * contracts) / SUM(contracts) AS avg_price,
                AVG(fee_multiplier) AS avg_fee_mult,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM strategy_trades
            GROUP BY category
            HAVING SUM(contracts) >= 100000
            ORDER BY SUM(contracts) DESC
            LIMIT 8
        """,
    )
    cat_df = con.execute(query_cat).df()
    con.close()

    validate_row_count(price_df, 1, "Fade YES price bins")

    # --- Compute strategy metrics ---
    def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["win_rate_pct"] = df["win_rate"] * 100
        df["gross_edge_pp"] = df["win_rate_pct"] - df["avg_price"]
        df["fee_cost_pp"] = df.apply(
            lambda r: kalshi_fee_cents(r["avg_price"], r["avg_fee_mult"]), axis=1
        )
        df["net_edge_pp"] = df["gross_edge_pp"] - df["fee_cost_pp"]
        df["daily_cap"] = df["total_contracts"].apply(lambda c: daily_capacity(c, DATASET_DAYS))
        df["kelly"] = df.apply(
            lambda r: kelly_fraction(
                r["win_rate"],
                payout_ratio_from_price(min(max(r["avg_price"], 1), 99)),
            ),
            axis=1,
        )
        return df

    price_df = compute_metrics(price_df)
    cat_df = compute_metrics(cat_df)

    log.info("Price bin results:")
    for _, row in price_df.iterrows():
        log.info(
            "  %dc: gross=%.2fpp, fee=%.2fpp, net=%.2fpp, cap=%.0f/day, kelly=%.3f",
            row["price_bin"],
            row["gross_edge_pp"],
            row["fee_cost_pp"],
            row["net_edge_pp"],
            row["daily_cap"],
            row["kelly"],
        )

    # --- Figure 1: Gross vs net edge by price bin ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax1 = plt.subplots(figsize=(10, 6))

    x = np.arange(len(price_df))
    width = 0.35
    ax1.bar(
        x - width / 2,
        price_df["gross_edge_pp"],
        width,
        label="Gross edge",
        color="#4C72B0",
        alpha=0.85,
    )
    ax1.bar(
        x + width / 2,
        price_df["net_edge_pp"],
        width,
        label="Net edge (after fees)",
        color="#55A868",
        alpha=0.85,
    )
    ax1.axhline(y=0, color="black", linewidth=0.5)
    ax1.set_xlabel("Price Bin (cents)", fontsize=12)
    ax1.set_ylabel("Edge (percentage points)", fontsize=12)
    ax1.set_title(
        "Fade YES Strategy: Gross vs. Net Edge by Price",
        fontsize=14,
        fontweight="bold",
    )
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{int(p)}-{int(p + PRICE_BIN_WIDTH)}" for p in price_df["price_bin"]])
    ax1.legend(fontsize=11)

    fig1.tight_layout()
    fig1_path = figures_dir / "fade_yes_edge_by_price.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Net edge by category ---
    fig2, ax2 = plt.subplots(figsize=(10, 6))

    if not cat_df.empty:
        cat_sorted = cat_df.sort_values("net_edge_pp", ascending=True)
        colors = ["#55A868" if e >= 0 else "#C44E52" for e in cat_sorted["net_edge_pp"]]
        y = np.arange(len(cat_sorted))
        ax2.barh(y, cat_sorted["net_edge_pp"], color=colors, alpha=0.85)
        ax2.axvline(x=0, color="black", linewidth=0.5)
        ax2.set_yticks(y)
        ax2.set_yticklabels(cat_sorted["category"], fontsize=10)
        ax2.set_xlabel("Net Edge (pp)", fontsize=12)
        ax2.set_title(
            "Fade YES Strategy: Net Edge by Category",
            fontsize=14,
            fontweight="bold",
        )

    fig2.tight_layout()
    fig2_path = figures_dir / "fade_yes_edge_by_category.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Figure 3: Edge waterfall (overall) ---
    fig3, ax3 = plt.subplots(figsize=(8, 5))

    overall_wr = (price_df["win_rate"] * price_df["total_contracts"]).sum() / price_df[
        "total_contracts"
    ].sum()
    tc = price_df["total_contracts"]
    overall_avg_price = (price_df["avg_price"] * tc).sum() / tc.sum()
    overall_avg_fee_mult = (price_df["avg_fee_mult"] * tc).sum() / tc.sum()
    overall_gross = overall_wr * 100 - overall_avg_price
    overall_fee = kalshi_fee_cents(overall_avg_price, overall_avg_fee_mult)
    overall_net = overall_gross - overall_fee

    labels = ["Gross Edge", "- Fees", "= Net Edge"]
    values = [overall_gross, -overall_fee, overall_net]
    bar_colors = ["#4C72B0", "#C44E52", "#55A868" if overall_net >= 0 else "#C44E52"]

    ax3.bar(labels, values, color=bar_colors, alpha=0.85, edgecolor="white")
    ax3.axhline(y=0, color="black", linewidth=0.5)
    ax3.set_ylabel("Percentage Points", fontsize=12)
    ax3.set_title("Fade YES: Edge Decomposition", fontsize=14, fontweight="bold")

    for i, v in enumerate(values):
        ax3.text(
            i,
            v,
            f"{v:+.2f}",
            ha="center",
            va="bottom" if v >= 0 else "top",
            fontsize=11,
            fontweight="bold",
        )

    fig3.tight_layout()
    fig3_path = figures_dir / "fade_yes_waterfall.png"
    fig3.savefig(fig3_path, dpi=150, bbox_inches="tight")
    plt.close(fig3)

    # --- CSV ---
    csv_path = csv_dir / "fade_yes.csv"
    # Combine price and category breakdowns with a breakdown_type column
    price_out = price_df.copy()
    price_out["breakdown_type"] = "price_bin"
    price_out["breakdown_value"] = price_out["price_bin"].astype(int).astype(str) + "c"

    cat_out = cat_df.copy()
    cat_out["breakdown_type"] = "category"
    cat_out["breakdown_value"] = cat_out["category"]
    cat_out["price_bin"] = np.nan

    cols = [
        "breakdown_type",
        "breakdown_value",
        "gross_edge_pp",
        "fee_cost_pp",
        "net_edge_pp",
        "win_rate_pct",
        "avg_price",
        "avg_fee_mult",
        "total_contracts",
        "daily_cap",
        "kelly",
    ]
    combined = pd.concat([price_out[cols], cat_out[cols]], ignore_index=True)
    combined.to_csv(csv_path, index=False)

    # --- Summary ---
    total_contracts = price_df["total_contracts"].sum()
    overall_daily = daily_capacity(total_contracts, DATASET_DAYS)
    best_bin = price_df.loc[price_df["net_edge_pp"].idxmax()]

    summary = (
        f"Fade YES strategy (NO-taker >= {MIN_PRICE}c): "
        f"overall gross edge {overall_gross:+.2f}pp, fee cost {overall_fee:.2f}pp, "
        f"net edge {overall_net:+.2f}pp. "
        f"Best price bin: {int(best_bin['price_bin'])}c "
        f"(net {best_bin['net_edge_pp']:+.2f}pp, kelly={best_bin['kelly']:.3f}). "
        f"Total capacity: {overall_daily:,.0f} contracts/day across "
        f"{total_contracts:,.0f} historical contracts."
    )

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path, fig3_path],
        csv_path=csv_path,
        summary=summary,
    )
