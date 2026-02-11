"""Strategy filter definitions for backtesting.

Encodes each Tier 1 strategy from Round 3 as a structured filter that can be
translated into SQL WHERE clauses for trade selection.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyFilter:
    """A filter defining which trades a strategy would take.

    Each field corresponds to a dimension from the combined_filters analysis.
    Use '*' to match any value for that dimension.
    """

    name: str
    taker_side: str  # "yes", "no"
    category: str  # "Elections", "Economics", or "*"
    fee_type: str  # "quadratic", "quadratic_with_maker_fees", or "*"
    time_bucket: str  # "evening", "other", or "*"
    price_min: float  # Minimum taker price in cents (inclusive)
    price_max: float  # Maximum taker price in cents (exclusive)


def strategy_where_clause(strategy: StrategyFilter) -> str:
    """Generate a SQL WHERE clause fragment for a strategy's filters.

    Returns a string like "taker_side = 'yes' AND category = 'Elections' AND ..."
    suitable for appending after WHERE or AND.
    """
    clauses = []

    clauses.append(f"taker_side = '{strategy.taker_side}'")

    if strategy.category != "*":
        clauses.append(f"category = '{strategy.category}'")

    if strategy.fee_type != "*":
        clauses.append(f"fee_type = '{strategy.fee_type}'")

    if strategy.time_bucket != "*":
        clauses.append(f"time_bucket = '{strategy.time_bucket}'")

    clauses.append(f"taker_price >= {strategy.price_min}")
    clauses.append(f"taker_price < {strategy.price_max}")

    return " AND ".join(clauses)


# Tier 1 strategies from Round 3's strategy_comparison.csv
# Only strategies with net_edge >= 2pp AND daily_cap >= 5K/day
TIER1_STRATEGIES: list[StrategyFilter] = [
    StrategyFilter(
        name="Elections YES high (quadratic, non-evening)",
        taker_side="yes",
        category="Elections",
        fee_type="quadratic",
        time_bucket="other",
        price_min=60.0,
        price_max=100.0,
    ),
    StrategyFilter(
        name="Elections NO high (quadratic, non-evening)",
        taker_side="no",
        category="Elections",
        fee_type="quadratic",
        time_bucket="other",
        price_min=60.0,
        price_max=100.0,
    ),
    StrategyFilter(
        name="Economics YES favorites (>=70c)",
        taker_side="yes",
        category="Economics",
        fee_type="*",
        time_bucket="*",
        price_min=70.0,
        price_max=100.0,
    ),
    StrategyFilter(
        name="Elections YES high (quadratic, evening)",
        taker_side="yes",
        category="Elections",
        fee_type="quadratic",
        time_bucket="evening",
        price_min=60.0,
        price_max=100.0,
    ),
    StrategyFilter(
        name="Elections YES favorites (>=70c)",
        taker_side="yes",
        category="Elections",
        fee_type="*",
        time_bucket="*",
        price_min=70.0,
        price_max=100.0,
    ),
]
