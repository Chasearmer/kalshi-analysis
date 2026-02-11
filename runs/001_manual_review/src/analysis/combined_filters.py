"""Combined multi-filter strategy analysis.

Stacks multiple bias signals discovered in Round 2 (taker side, fee type,
time bucket, category) and ranks filter combinations by total extractable edge
(net_edge × daily_capacity). Tests whether multiple filters are independent
or interact.
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
MIN_CONTRACTS = 10_000


def _full_trade_outcomes_sql(data_dir: Path) -> str:
    """SQL CTE for trade outcomes with category, fee info, and time bucket.

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
            mf.fee_type,
            mf.fee_multiplier,
            CASE
                WHEN EXTRACT(HOUR FROM CAST(t.created_time AS TIMESTAMPTZ)
                    AT TIME ZONE 'America/New_York') BETWEEN 20 AND 23
                THEN 'evening'
                ELSE 'other'
            END AS time_bucket,
            CASE
                WHEN (CASE WHEN t.taker_side = 'yes'
                     THEN CAST(t.yes_price_dollars AS DOUBLE) * 100
                     ELSE CAST(t.no_price_dollars AS DOUBLE) * 100
                END) >= 60 THEN 'high_price'
                WHEN (CASE WHEN t.taker_side = 'yes'
                     THEN CAST(t.yes_price_dollars AS DOUBLE) * 100
                     ELSE CAST(t.no_price_dollars AS DOUBLE) * 100
                END) <= 30 THEN 'low_price'
                ELSE 'mid_price'
            END AS price_range
        FROM '{data_dir}/trades/*.parquet' t
        INNER JOIN markets_with_fees mf ON t.ticker = mf.ticker
    """


def run(
    data_dir: Path,
    output_dir: Path,
    top_n: int = 10,
    min_contracts: int = MIN_CONTRACTS,
) -> AnalysisResult:
    """Run combined multi-filter strategy analysis.

    Args:
        data_dir: Path to the root data directory containing Parquet files.
        output_dir: Path to the output directory for figures and CSVs.
        top_n: Number of top combinations to report.
        min_contracts: Minimum contracts per combination to include.

    Returns:
        AnalysisResult with figure paths, CSV path, and summary text.
    """
    figures_dir, csv_dir = ensure_output_dirs(output_dir)
    con = get_connection()

    log.info("Running combined filter analysis (top %d, min %d contracts)...", top_n, min_contracts)

    query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("markets_with_fees", with_fee_type_sql(data_dir)),
            ("full_trades", _full_trade_outcomes_sql(data_dir)),
        ],
        select=f"""
            SELECT
                taker_side,
                fee_type,
                time_bucket,
                category,
                price_range,
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) AS win_rate,
                SUM(taker_price * contracts) / SUM(contracts) AS avg_price,
                AVG(fee_multiplier) AS avg_fee_mult,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM full_trades
            WHERE taker_price > 0 AND taker_price < 100
            GROUP BY taker_side, fee_type, time_bucket, category, price_range
            HAVING SUM(contracts) >= {min_contracts}
            ORDER BY SUM(contracts) DESC
        """,
    )
    df = con.execute(query).df()

    # Also query marginal (single-filter) edges for independence test
    marginal_query = build_query(
        ctes=[
            ("resolved_markets", resolved_markets_sql(data_dir)),
            ("markets_with_fees", with_fee_type_sql(data_dir)),
            ("full_trades", _full_trade_outcomes_sql(data_dir)),
        ],
        select="""
            SELECT
                SUM(CASE WHEN taker_won = 1 THEN contracts ELSE 0 END)
                    / SUM(contracts) AS overall_win_rate,
                SUM(taker_price * contracts) / SUM(contracts) AS overall_avg_price,
                AVG(fee_multiplier) AS overall_fee_mult,
                SUM(contracts) AS overall_contracts
            FROM full_trades
            WHERE taker_price > 0 AND taker_price < 100
        """,
    )
    overall = con.execute(marginal_query).df().iloc[0]
    con.close()

    validate_row_count(df, 1, "Combined filter combinations")

    # Compute metrics
    df["win_rate_pct"] = df["win_rate"] * 100
    df["gross_edge_pp"] = df["win_rate_pct"] - df["avg_price"]
    df["fee_cost_pp"] = df.apply(
        lambda r: kalshi_fee_cents(r["avg_price"], r["avg_fee_mult"]), axis=1
    )
    df["net_edge_pp"] = df["gross_edge_pp"] - df["fee_cost_pp"]
    df["daily_cap"] = df["total_contracts"].apply(lambda c: daily_capacity(c, DATASET_DAYS))
    df["total_extractable"] = df["net_edge_pp"] * df["daily_cap"]
    df["kelly"] = df.apply(
        lambda r: kelly_fraction(
            r["win_rate"],
            payout_ratio_from_price(min(max(r["avg_price"], 1), 99)),
        ),
        axis=1,
    )

    # Build filter combination label
    df["filter_combination"] = (
        df["taker_side"]
        + " | "
        + df["fee_type"]
        + " | "
        + df["time_bucket"]
        + " | "
        + df["category"]
        + " | "
        + df["price_range"]
    )

    # Overall baseline
    overall_wr_pct = overall["overall_win_rate"] * 100
    overall_gross = overall_wr_pct - overall["overall_avg_price"]
    overall_fee = kalshi_fee_cents(overall["overall_avg_price"], overall["overall_fee_mult"])
    overall_net = overall_gross - overall_fee

    # Rank by total extractable edge and take top N
    top_df = df.nlargest(top_n, "total_extractable").reset_index(drop=True)
    top_df["rank"] = range(1, len(top_df) + 1)

    log.info(
        "Overall baseline: gross=%.2fpp, fee=%.2fpp, net=%.2fpp",
        overall_gross,
        overall_fee,
        overall_net,
    )
    log.info("Top %d combinations by total extractable edge:", min(top_n, len(top_df)))
    for _, row in top_df.iterrows():
        log.info(
            "  #%d: %s — net=%.2fpp, cap=%.0f/day, extractable=%.0f",
            row["rank"],
            row["filter_combination"],
            row["net_edge_pp"],
            row["daily_cap"],
            row["total_extractable"],
        )

    # --- Figure 1: Top N combinations by total extractable edge ---
    plt.style.use("seaborn-v0_8-whitegrid")
    fig1, ax1 = plt.subplots(figsize=(12, max(4, len(top_df) * 0.5 + 1)))

    if not top_df.empty:
        y = np.arange(len(top_df))
        colors = ["#55A868" if e >= 0 else "#C44E52" for e in top_df["net_edge_pp"]]
        ax1.barh(y, top_df["net_edge_pp"], color=colors, alpha=0.85)
        ax1.set_yticks(y)
        labels = [f"#{r}: {c}" for r, c in zip(top_df["rank"], top_df["filter_combination"])]
        ax1.set_yticklabels(labels, fontsize=8)
        ax1.axvline(x=0, color="black", linewidth=0.5)
        ax1.axvline(
            x=overall_net,
            color="gray",
            linewidth=1,
            linestyle="--",
            label=f"Baseline ({overall_net:+.2f}pp)",
        )
        ax1.set_xlabel("Net Edge (pp)", fontsize=12)
        ax1.set_title(
            f"Top {len(top_df)} Filter Combinations (ranked by extractable edge)",
            fontsize=13,
            fontweight="bold",
        )
        ax1.legend(fontsize=9)

    fig1.tight_layout()
    fig1_path = figures_dir / "combined_filters_ranking.png"
    fig1.savefig(fig1_path, dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Edge vs capacity scatter ---
    fig2, ax2 = plt.subplots(figsize=(10, 7))

    positive = df[df["net_edge_pp"] > 0]
    negative = df[df["net_edge_pp"] <= 0]

    if not positive.empty:
        ax2.scatter(
            positive["daily_cap"],
            positive["net_edge_pp"],
            s=40,
            color="#55A868",
            alpha=0.5,
            label="Positive net edge",
        )
    if not negative.empty:
        ax2.scatter(
            negative["daily_cap"],
            negative["net_edge_pp"],
            s=20,
            color="#C44E52",
            alpha=0.3,
            label="Negative net edge",
        )

    # Highlight top combinations
    if not top_df.empty:
        ax2.scatter(
            top_df["daily_cap"],
            top_df["net_edge_pp"],
            s=100,
            color="#FFD700",
            edgecolors="black",
            zorder=5,
            label=f"Top {len(top_df)}",
        )

    ax2.axhline(y=0, color="black", linewidth=0.5)
    ax2.set_xlabel("Daily Capacity (contracts/day)", fontsize=12)
    ax2.set_ylabel("Net Edge (pp)", fontsize=12)
    ax2.set_title("Edge vs. Capacity: All Filter Combinations", fontsize=14, fontweight="bold")
    ax2.legend(fontsize=10)

    fig2.tight_layout()
    fig2_path = figures_dir / "combined_filters_edge_capacity_scatter.png"
    fig2.savefig(fig2_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Figure 3: Distribution of net edges ---
    fig3, ax3 = plt.subplots(figsize=(10, 5))

    ax3.hist(df["net_edge_pp"], bins=30, color="#4C72B0", alpha=0.8, edgecolor="white")
    ax3.axvline(x=0, color="red", linewidth=1.5, linestyle="--", label="Break-even")
    ax3.axvline(
        x=overall_net,
        color="gray",
        linewidth=1,
        linestyle="--",
        label=f"Baseline ({overall_net:+.2f}pp)",
    )
    ax3.set_xlabel("Net Edge (pp)", fontsize=12)
    ax3.set_ylabel("Number of Combinations", fontsize=12)
    ax3.set_title(
        "Distribution of Net Edge Across Filter Combinations",
        fontsize=14,
        fontweight="bold",
    )
    ax3.legend(fontsize=10)

    fig3.tight_layout()
    fig3_path = figures_dir / "combined_filters_distribution.png"
    fig3.savefig(fig3_path, dpi=150, bbox_inches="tight")
    plt.close(fig3)

    # --- CSV ---
    csv_path = csv_dir / "combined_filters.csv"
    out_cols = [
        "rank",
        "filter_combination",
        "taker_side",
        "fee_type",
        "time_bucket",
        "category",
        "price_range",
        "gross_edge_pp",
        "fee_cost_pp",
        "net_edge_pp",
        "win_rate_pct",
        "avg_price",
        "total_contracts",
        "daily_cap",
        "total_extractable",
        "kelly",
    ]
    # Include all positive-edge combinations plus top N
    positive_edge = df[df["net_edge_pp"] > 0].nlargest(50, "total_extractable").copy()
    positive_edge["rank"] = range(1, len(positive_edge) + 1)
    out = positive_edge[[c for c in out_cols if c in positive_edge.columns]]
    out.to_csv(csv_path, index=False)

    # --- Summary ---
    n_positive = (df["net_edge_pp"] > 0).sum()
    n_total = len(df)

    if not top_df.empty:
        best = top_df.iloc[0]
        summary = (
            f"{n_total} filter combinations evaluated (min {MIN_CONTRACTS:,} contracts). "
            f"{n_positive} ({n_positive / n_total * 100:.0f}%) have positive net edge. "
            f"Baseline net edge: {overall_net:+.2f}pp. "
            f"Best combination: {best['filter_combination']} "
            f"(net {best['net_edge_pp']:+.2f}pp, "
            f"capacity {best['daily_cap']:,.0f}/day, "
            f"extractable={best['total_extractable']:,.0f})."
        )
    else:
        summary = f"{n_total} combinations evaluated. No positive net edge found."

    return AnalysisResult(
        figure_paths=[fig1_path, fig2_path, fig3_path],
        csv_path=csv_path,
        summary=summary,
    )
