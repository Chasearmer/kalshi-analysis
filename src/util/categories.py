"""Category taxonomy for Kalshi markets.

Categories come from two sources:
1. The `category` field on events (authoritative, covers ~18% of markets by count, ~92% by volume)
2. Prefix-based inference from event_ticker (for MVE/orphan markets)
"""

EVENT_CATEGORIES: list[str] = [
    "COVID-19",
    "Climate and Weather",
    "Companies",
    "Crypto",
    "Economics",
    "Education",
    "Elections",
    "Entertainment",
    "Financials",
    "Health",
    "Mentions",
    "Politics",
    "Science and Technology",
    "Social",
    "Sports",
    "Transportation",
    "World",
]

# Maps event_ticker prefixes to categories. Ordered longest-first for correct matching.
# Covers the major orphan prefixes found in the data (MVE sports markets, standalone tickers).
PREFIX_TO_CATEGORY: list[tuple[str, str]] = [
    ("KXMVESPORTS", "Sports"),
    ("KXMVENFL", "Sports"),
    ("KXMVENBA", "Sports"),
    ("KXMVENCAAMB", "Sports"),
    ("KXMVENHL", "Sports"),
    ("KXMVEUFC", "Sports"),
    ("KXMVEMENTIONS", "Mentions"),
    ("KXMVEMENT", "Entertainment"),
    ("KXNCAAMB", "Sports"),
    ("KXNCAAWB", "Sports"),
    ("KXNBAGAME", "Sports"),
    ("KXNBASPREAD", "Sports"),
    ("KXNBATOTAL", "Sports"),
    ("KXNHLGAME", "Sports"),
    ("KXUFCFIGHT", "Sports"),
    ("KXPGATOUR", "Sports"),
    ("KXATPCHALLEN", "Sports"),
    ("KXATPMATCH", "Sports"),
    ("KXWTAMATCH", "Sports"),
    ("KXEPLGAME", "Sports"),
    ("KXLALIGAGAME", "Sports"),
    ("KXBTC", "Crypto"),
    ("KXETH", "Crypto"),
    ("KXINX", "Financials"),
]


def infer_category(event_ticker: str) -> str:
    """Infer category from event_ticker prefix. Returns 'Unknown' if no match."""
    for prefix, category in PREFIX_TO_CATEGORY:
        if event_ticker.startswith(prefix):
            return category
    return "Unknown"


def category_case_sql() -> str:
    """Return a SQL CASE expression for prefix-based category inference.

    Intended for use inside a DuckDB query where the column `event_ticker` is in scope.
    """
    clauses = []
    for prefix, category in PREFIX_TO_CATEGORY:
        clauses.append(f"WHEN event_ticker LIKE '{prefix}%' THEN '{category}'")
    return "CASE " + " ".join(clauses) + " ELSE 'Unknown' END"
