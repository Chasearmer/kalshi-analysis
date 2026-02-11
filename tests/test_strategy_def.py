"""Tests for strategy filter definitions."""

from simulation.strategy_def import TIER1_STRATEGIES, StrategyFilter, strategy_where_clause


class TestStrategyFilter:
    def test_frozen_dataclass(self) -> None:
        """StrategyFilter is immutable."""
        s = StrategyFilter(
            name="test", taker_side="yes", category="Elections",
            fee_type="quadratic", time_bucket="other",
            price_min=60.0, price_max=100.0,
        )
        assert s.name == "test"

    def test_all_tier1_strategies_defined(self) -> None:
        """All Tier 1 strategies from Round 3 are registered."""
        assert len(TIER1_STRATEGIES) == 5

    def test_tier1_strategy_names_unique(self) -> None:
        """Strategy names are unique."""
        names = [s.name for s in TIER1_STRATEGIES]
        assert len(names) == len(set(names))

    def test_valid_taker_sides(self) -> None:
        """All strategies have valid taker_side."""
        for s in TIER1_STRATEGIES:
            assert s.taker_side in ("yes", "no")

    def test_valid_price_ranges(self) -> None:
        """All strategies have valid price ranges."""
        for s in TIER1_STRATEGIES:
            assert 0 < s.price_min < s.price_max <= 100


class TestStrategyWhereClause:
    def test_basic_clause(self) -> None:
        """Generates correct WHERE clause for a fully specified strategy."""
        s = StrategyFilter(
            name="test", taker_side="yes", category="Elections",
            fee_type="quadratic", time_bucket="other",
            price_min=60.0, price_max=100.0,
        )
        clause = strategy_where_clause(s)
        assert "taker_side = 'yes'" in clause
        assert "category = 'Elections'" in clause
        assert "fee_type = 'quadratic'" in clause
        assert "time_bucket = 'other'" in clause
        assert "taker_price >= 60.0" in clause
        assert "taker_price < 100.0" in clause

    def test_wildcard_category(self) -> None:
        """Wildcard category is not included in WHERE clause."""
        s = StrategyFilter(
            name="test", taker_side="yes", category="*",
            fee_type="quadratic", time_bucket="*",
            price_min=70.0, price_max=100.0,
        )
        clause = strategy_where_clause(s)
        assert "category" not in clause
        assert "time_bucket" not in clause
        assert "fee_type = 'quadratic'" in clause

    def test_wildcard_fee_type(self) -> None:
        """Wildcard fee_type is not included in WHERE clause."""
        s = StrategyFilter(
            name="test", taker_side="no", category="Economics",
            fee_type="*", time_bucket="*",
            price_min=60.0, price_max=100.0,
        )
        clause = strategy_where_clause(s)
        assert "fee_type" not in clause
        assert "category = 'Economics'" in clause
