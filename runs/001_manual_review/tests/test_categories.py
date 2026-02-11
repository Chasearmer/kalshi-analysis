"""Tests for the category taxonomy and inference logic."""

import duckdb
import pytest

from util.categories import (
    EVENT_CATEGORIES,
    PREFIX_TO_CATEGORY,
    category_case_sql,
    infer_category,
)


class TestEventCategories:
    def test_exactly_17_categories(self) -> None:
        assert len(EVENT_CATEGORIES) == 17

    def test_categories_are_unique(self) -> None:
        assert len(EVENT_CATEGORIES) == len(set(EVENT_CATEGORIES))

    def test_known_categories_present(self) -> None:
        for expected in ("Sports", "Crypto", "Financials", "Politics", "Mentions"):
            assert expected in EVENT_CATEGORIES


class TestPrefixToCategory:
    def test_is_nonempty(self) -> None:
        assert len(PREFIX_TO_CATEGORY) > 0

    def test_all_categories_are_known(self) -> None:
        for _prefix, category in PREFIX_TO_CATEGORY:
            assert category in EVENT_CATEGORIES


class TestInferCategory:
    @pytest.mark.parametrize(
        ("ticker", "expected"),
        [
            ("KXMVESPORTSMULTIGAME-ABC", "Sports"),
            ("KXMVENFLSINGLEGAME-26JAN", "Sports"),
            ("KXNBAGAME-26FEB07-GSW-LAL", "Sports"),
            ("KXNCAAMBGAME-26FEB10-DUKE", "Sports"),
        ],
    )
    def test_sports_prefixes(self, ticker: str, expected: str) -> None:
        assert infer_category(ticker) == expected

    @pytest.mark.parametrize(
        ("ticker", "expected"),
        [
            ("KXBTCD-26FEB10-T59500", "Crypto"),
            ("KXMVEMENTIONS-TRUMP", "Mentions"),
            ("KXINXSPX-26FEB10-6000", "Financials"),
        ],
    )
    def test_non_sports_prefixes(self, ticker: str, expected: str) -> None:
        assert infer_category(ticker) == expected

    def test_unknown_prefix(self) -> None:
        assert infer_category("FOOBAR") == "Unknown"

    def test_empty_string(self) -> None:
        assert infer_category("") == "Unknown"

    def test_entertainment_prefix(self) -> None:
        assert infer_category("KXMVEMENTGRAMMYS-26") == "Entertainment"


class TestCategoryCaseSql:
    def test_returns_case_expression(self) -> None:
        sql = category_case_sql()
        assert sql.startswith("CASE ")
        assert sql.endswith(" END")
        assert "WHEN" in sql
        assert "ELSE 'Unknown'" in sql

    def test_contains_all_prefixes(self) -> None:
        sql = category_case_sql()
        for prefix, _category in PREFIX_TO_CATEGORY:
            assert prefix in sql

    @pytest.fixture()
    def duckdb_conn(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        yield conn
        conn.close()

    def test_valid_sql_in_duckdb(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        """The generated CASE expression should be executable SQL in DuckDB."""
        case_sql = category_case_sql()
        duckdb_conn.execute("CREATE TABLE test_events (event_ticker VARCHAR)")
        duckdb_conn.execute(
            "INSERT INTO test_events VALUES ('KXBTCD-26FEB'), ('KXNBAGAME-26'), ('UNKNOWN')"
        )
        query = f"SELECT event_ticker, {case_sql} AS category FROM test_events"
        result = duckdb_conn.execute(query).fetchall()
        lookup = {row[0]: row[1] for row in result}
        assert lookup["KXBTCD-26FEB"] == "Crypto"
        assert lookup["KXNBAGAME-26"] == "Sports"
        assert lookup["UNKNOWN"] == "Unknown"
