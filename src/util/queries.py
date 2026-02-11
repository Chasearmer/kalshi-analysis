"""Reusable DuckDB SQL fragments for Kalshi data analysis.

All queries operate on Parquet files via glob patterns. Prices use the _dollars
fields (0.00-1.00 scale), converted to cents (0-100) for analysis.
"""

from pathlib import Path

import duckdb

from util.categories import category_case_sql


def get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB in-memory connection."""
    return duckdb.connect()


def resolved_markets_sql(data_dir: Path) -> str:
    """SQL for a CTE of finalized binary markets with known outcomes.

    Returns just the CTE body (use with build_query).
    Columns: ticker, event_ticker, result, volume
    """
    return f"""
        SELECT ticker, event_ticker, result, CAST(volume_fp AS DOUBLE) AS volume
        FROM '{data_dir}/markets/*.parquet'
        WHERE status = 'finalized' AND result IN ('yes', 'no')
    """


def trade_outcomes_sql(data_dir: Path) -> str:
    """SQL for a CTE decomposing trades into taker/maker perspectives.

    Depends on: resolved_markets CTE.
    Columns: ticker, taker_price, maker_price, taker_won, maker_won, contracts, created_time
    """
    return f"""
        SELECT
            t.ticker,
            t.taker_side,
            CASE WHEN t.taker_side = 'yes'
                 THEN CAST(t.yes_price_dollars AS DOUBLE) * 100
                 ELSE CAST(t.no_price_dollars AS DOUBLE) * 100
            END AS taker_price,
            CASE WHEN t.taker_side = 'yes'
                 THEN CAST(t.no_price_dollars AS DOUBLE) * 100
                 ELSE CAST(t.yes_price_dollars AS DOUBLE) * 100
            END AS maker_price,
            CASE WHEN t.taker_side = r.result THEN 1 ELSE 0 END AS taker_won,
            CASE WHEN t.taker_side != r.result THEN 1 ELSE 0 END AS maker_won,
            CAST(t.count_fp AS DOUBLE) AS contracts,
            t.created_time
        FROM '{data_dir}/trades/*.parquet' t
        INNER JOIN resolved_markets r ON t.ticker = r.ticker
    """


def with_category_sql(data_dir: Path) -> str:
    """SQL for a CTE adding category to resolved markets.

    Uses events JOIN when available, falls back to prefix-based inference.
    Depends on: resolved_markets CTE.
    Columns: ticker, event_ticker, result, volume, category
    """
    case_sql = category_case_sql()
    return f"""
        SELECT
            r.ticker,
            r.event_ticker,
            r.result,
            r.volume,
            COALESCE(e.category, {case_sql.replace('event_ticker', 'r.event_ticker')}) AS category
        FROM resolved_markets r
        LEFT JOIN '{data_dir}/events/*.parquet' e ON r.event_ticker = e.event_ticker
    """


def categorized_trade_outcomes_sql(data_dir: Path) -> str:
    """SQL for a CTE joining trade outcomes with category.

    Depends on: trade_outcomes CTE, categorized CTE (from with_category_sql).
    Columns: ticker, category, taker_side, taker_price, maker_price,
             taker_won, maker_won, contracts, created_time
    """
    return """
        SELECT
            t.ticker,
            c.category,
            t.taker_side,
            t.taker_price,
            t.maker_price,
            t.taker_won,
            t.maker_won,
            t.contracts,
            t.created_time
        FROM trade_outcomes t
        JOIN categorized c ON t.ticker = c.ticker
    """


def with_fee_type_sql(data_dir: Path) -> str:
    """SQL for a CTE adding fee_type and fee_multiplier to resolved markets.

    Joins resolved_markets → events → series.
    Depends on: resolved_markets CTE.
    Columns: ticker, event_ticker, result, volume, category, fee_type, fee_multiplier
    """
    case_sql = category_case_sql()
    return f"""
        SELECT
            r.ticker,
            r.event_ticker,
            r.result,
            r.volume,
            COALESCE(e.category, {case_sql.replace('event_ticker', 'r.event_ticker')}) AS category,
            COALESCE(s.fee_type, 'unknown') AS fee_type,
            COALESCE(s.fee_multiplier, 1.0) AS fee_multiplier
        FROM resolved_markets r
        LEFT JOIN '{data_dir}/events/*.parquet' e ON r.event_ticker = e.event_ticker
        LEFT JOIN (
            SELECT DISTINCT ticker, fee_type, CAST(fee_multiplier AS DOUBLE) AS fee_multiplier
            FROM '{data_dir}/series/*.parquet'
        ) s ON e.series_ticker = s.ticker
    """


def trade_outcomes_with_timing_sql(data_dir: Path) -> str:
    """SQL for a CTE adding market close_time and hours_to_close to trade outcomes.

    Depends on: resolved_markets CTE.
    Columns: ticker, taker_side, taker_price, maker_price, taker_won, maker_won,
             contracts, created_time, close_time, hours_to_close

    Only includes markets where close_time is populated.
    hours_to_close can be negative for trades after market close but before settlement.
    """
    return f"""
        SELECT
            t.ticker,
            t.taker_side,
            CASE WHEN t.taker_side = 'yes'
                 THEN CAST(t.yes_price_dollars AS DOUBLE) * 100
                 ELSE CAST(t.no_price_dollars AS DOUBLE) * 100
            END AS taker_price,
            CASE WHEN t.taker_side = 'yes'
                 THEN CAST(t.no_price_dollars AS DOUBLE) * 100
                 ELSE CAST(t.yes_price_dollars AS DOUBLE) * 100
            END AS maker_price,
            CASE WHEN t.taker_side = r.result THEN 1 ELSE 0 END AS taker_won,
            CASE WHEN t.taker_side != r.result THEN 1 ELSE 0 END AS maker_won,
            CAST(t.count_fp AS DOUBLE) AS contracts,
            t.created_time,
            m.close_time,
            EXTRACT(EPOCH FROM (
                CAST(m.close_time AS TIMESTAMP) - CAST(t.created_time AS TIMESTAMP)
            )) / 3600.0 AS hours_to_close
        FROM '{data_dir}/trades/*.parquet' t
        INNER JOIN resolved_markets r ON t.ticker = r.ticker
        INNER JOIN '{data_dir}/markets/*.parquet' m ON t.ticker = m.ticker
        WHERE m.close_time IS NOT NULL
    """


def build_query(ctes: list[tuple[str, str]], select: str) -> str:
    """Compose named CTEs with a final SELECT into a complete SQL query.

    Args:
        ctes: List of (name, sql_body) tuples.
        select: The final SELECT statement.

    Returns:
        Complete SQL query string.
    """
    if not ctes:
        return select

    cte_parts = [f"{name} AS ({sql})" for name, sql in ctes]
    return "WITH " + ",\n".join(cte_parts) + "\n" + select
