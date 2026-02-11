"""Tests for DuckDB query utilities using synthetic Parquet fixtures."""

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from util.queries import (
    build_query,
    categorized_trade_outcomes_sql,
    get_connection,
    resolved_markets_sql,
    trade_outcomes_sql,
    with_category_sql,
    with_fee_type_sql,
)


@pytest.fixture()
def fixture_data_dir(tmp_path: Path) -> Path:
    """Create a minimal Parquet dataset for testing."""
    # Markets
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    markets = pa.table({
        "ticker": ["M1", "M2", "M3", "M4", "M5"],
        "event_ticker": ["E1", "E1", "E2", "E1", "E2"],
        "status": ["finalized", "finalized", "finalized", "active", "finalized"],
        "result": ["yes", "no", "yes", "", "42"],
        "volume_fp": ["100.00", "200.00", "150.00", "50.00", "75.00"],
    })
    pq.write_table(markets, markets_dir / "markets_000000.parquet")

    # Trades
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    trades = pa.table({
        "trade_id": ["t1", "t2", "t3"],
        "ticker": ["M1", "M2", "M3"],
        "yes_price_dollars": ["0.6500", "0.3000", "0.8000"],
        "no_price_dollars": ["0.3500", "0.7000", "0.2000"],
        "count_fp": ["10.00", "20.00", "5.00"],
        "taker_side": ["yes", "no", "yes"],
        "created_time": [
            "2024-01-01T00:00:00Z",
            "2024-01-02T00:00:00Z",
            "2024-01-03T00:00:00Z",
        ],
    })
    pq.write_table(trades, trades_dir / "trades_000000.parquet")

    # Events
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    events = pa.table({
        "event_ticker": ["E1", "E2"],
        "category": ["Sports", "Politics"],
        "series_ticker": ["S1", "S2"],
    })
    pq.write_table(events, events_dir / "events_000000.parquet")

    # Series
    series_dir = tmp_path / "series"
    series_dir.mkdir()
    series = pa.table({
        "ticker": ["S1", "S2"],
        "fee_type": ["quadratic", "quadratic_with_maker_fees"],
        "fee_multiplier": [1.0, 0.5],
    })
    pq.write_table(series, series_dir / "series_000000.parquet")

    return tmp_path


class TestGetConnection:
    def test_returns_working_connection(self) -> None:
        con = get_connection()
        result = con.execute("SELECT 1 AS x").fetchone()
        assert result == (1,)
        con.close()


class TestResolvedMarkets:
    def test_filters_finalized_binary_only(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [("resolved_markets", resolved_markets_sql(fixture_data_dir))],
            "SELECT COUNT(*) FROM resolved_markets",
        )
        result = con.execute(query).fetchone()
        # M1 (yes), M2 (no), M3 (yes) pass; M4 (active), M5 (result='42') excluded
        assert result[0] == 3
        con.close()

    def test_includes_correct_tickers(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [("resolved_markets", resolved_markets_sql(fixture_data_dir))],
            "SELECT ticker FROM resolved_markets ORDER BY ticker",
        )
        tickers = [row[0] for row in con.execute(query).fetchall()]
        assert tickers == ["M1", "M2", "M3"]
        con.close()


class TestTradeOutcomes:
    def test_price_calculation(self, fixture_data_dir: Path) -> None:
        """Taker buys YES at 65c → taker_price=65, maker_price=35."""
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT taker_price, maker_price FROM trade_outcomes WHERE ticker = 'M1'",
        )
        row = con.execute(query).fetchone()
        assert row[0] == pytest.approx(65.0)
        assert row[1] == pytest.approx(35.0)
        con.close()

    def test_price_taker_no_side(self, fixture_data_dir: Path) -> None:
        """Taker buys NO at 70c → taker_price=70, maker_price=30."""
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT taker_price, maker_price FROM trade_outcomes WHERE ticker = 'M2'",
        )
        row = con.execute(query).fetchone()
        assert row[0] == pytest.approx(70.0)
        assert row[1] == pytest.approx(30.0)
        con.close()

    def test_win_calculation_taker_wins(self, fixture_data_dir: Path) -> None:
        """M1: taker_side='yes', result='yes' → taker_won=1, maker_won=0."""
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT taker_won, maker_won FROM trade_outcomes WHERE ticker = 'M1'",
        )
        row = con.execute(query).fetchone()
        assert row[0] == 1
        assert row[1] == 0
        con.close()

    def test_win_calculation_taker_loses(self, fixture_data_dir: Path) -> None:
        """M2: taker_side='no', result='no' → taker_won=1, maker_won=0."""
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT taker_won, maker_won FROM trade_outcomes WHERE ticker = 'M2'",
        )
        row = con.execute(query).fetchone()
        assert row[0] == 1  # taker_side='no' and result='no' → taker wins
        assert row[1] == 0
        con.close()

    def test_contracts_parsed(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT contracts FROM trade_outcomes WHERE ticker = 'M1'",
        )
        row = con.execute(query).fetchone()
        assert row[0] == pytest.approx(10.0)
        con.close()


class TestWithCategory:
    def test_matched_events_get_category(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("categorized", with_category_sql(fixture_data_dir)),
            ],
            "SELECT ticker, category FROM categorized ORDER BY ticker",
        )
        results = con.execute(query).fetchall()
        lookup = {row[0]: row[1] for row in results}
        assert lookup["M1"] == "Sports"
        assert lookup["M3"] == "Politics"
        con.close()


class TestBuildQuery:
    def test_no_ctes(self) -> None:
        query = build_query([], "SELECT 1")
        assert query == "SELECT 1"

    def test_single_cte(self) -> None:
        query = build_query([("foo", "SELECT 1 AS x")], "SELECT * FROM foo")
        assert "WITH" in query
        assert "foo AS" in query
        assert "SELECT * FROM foo" in query

    def test_multiple_ctes(self) -> None:
        query = build_query(
            [("a", "SELECT 1"), ("b", "SELECT 2")],
            "SELECT * FROM a, b",
        )
        assert query.count("AS (") == 2


class TestTradeOutcomesTakerSide:
    def test_includes_taker_side(self, fixture_data_dir: Path) -> None:
        """trade_outcomes CTE includes taker_side column."""
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT taker_side FROM trade_outcomes WHERE ticker = 'M1'",
        )
        row = con.execute(query).fetchone()
        assert row[0] == "yes"
        con.close()

    def test_taker_side_no(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT taker_side FROM trade_outcomes WHERE ticker = 'M2'",
        )
        row = con.execute(query).fetchone()
        assert row[0] == "no"
        con.close()


class TestCategorizedTradeOutcomes:
    def test_joins_category_to_trades(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("categorized", with_category_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
                ("cat_trades", categorized_trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT ticker, category FROM cat_trades ORDER BY ticker",
        )
        results = con.execute(query).fetchall()
        lookup = {row[0]: row[1] for row in results}
        assert lookup["M1"] == "Sports"
        assert lookup["M3"] == "Politics"
        con.close()

    def test_preserves_trade_columns(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("categorized", with_category_sql(fixture_data_dir)),
                ("trade_outcomes", trade_outcomes_sql(fixture_data_dir)),
                ("cat_trades", categorized_trade_outcomes_sql(fixture_data_dir)),
            ],
            "SELECT taker_price, taker_won, contracts FROM cat_trades WHERE ticker = 'M1'",
        )
        row = con.execute(query).fetchone()
        assert row[0] == pytest.approx(65.0)
        assert row[1] == 1
        assert row[2] == pytest.approx(10.0)
        con.close()


class TestWithFeeType:
    def test_fee_type_joined(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("markets_with_fees", with_fee_type_sql(fixture_data_dir)),
            ],
            "SELECT ticker, fee_type, fee_multiplier FROM markets_with_fees ORDER BY ticker",
        )
        results = con.execute(query).fetchall()
        lookup = {row[0]: (row[1], row[2]) for row in results}
        assert lookup["M1"] == ("quadratic", pytest.approx(1.0))
        assert lookup["M3"] == ("quadratic_with_maker_fees", pytest.approx(0.5))
        con.close()

    def test_includes_category(self, fixture_data_dir: Path) -> None:
        con = duckdb.connect()
        query = build_query(
            [
                ("resolved_markets", resolved_markets_sql(fixture_data_dir)),
                ("markets_with_fees", with_fee_type_sql(fixture_data_dir)),
            ],
            "SELECT ticker, category FROM markets_with_fees ORDER BY ticker",
        )
        results = con.execute(query).fetchall()
        lookup = {row[0]: row[1] for row in results}
        assert lookup["M1"] == "Sports"
        assert lookup["M3"] == "Politics"
        con.close()
