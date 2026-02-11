"""Kalshi fee calculation utilities.

Kalshi uses a quadratic fee structure where the taker fee per contract is:
    fee = BASE_FEE_RATE * fee_multiplier * price * (1 - price)
in dollar terms (price on 0.00-1.00 scale).

The BASE_FEE_RATE is 0.07 (7%). The fee_multiplier from the API is a scaling
factor (1.0 for standard series, 0.5 for reduced-fee series like S&P 500).
Maximum taker fee at 50c with standard multiplier: 0.07 * 0.25 * 100 = 1.75 cents.

This module provides helpers to compute fees and net edge after fees,
using the cents (0-100) scale used throughout this project.
"""

BASE_FEE_RATE = 0.07


def kalshi_fee_cents(price_cents: float, fee_multiplier: float, contracts: float = 1.0) -> float:
    """Compute Kalshi quadratic fee in cents per contract.

    Args:
        price_cents: Contract price on 0-100 scale.
        fee_multiplier: Series fee multiplier from API (1.0 = standard, 0.5 = reduced).
        contracts: Number of contracts (default 1.0).

    Returns:
        Total fee in cents. For a single contract at 50c with fee_mult=1.0:
        0.07 * 0.50 * 0.50 * 100 = 1.75 cents.
    """
    price_dollars = price_cents / 100.0
    fee_per_contract = BASE_FEE_RATE * fee_multiplier * price_dollars * (1.0 - price_dollars)
    return fee_per_contract * contracts * 100.0


def net_edge_pp(gross_edge_pp: float, avg_price_cents: float, fee_multiplier: float) -> float:
    """Convert gross edge to net edge after subtracting fee cost.

    Args:
        gross_edge_pp: Gross excess return in percentage points.
        avg_price_cents: Average taker price on 0-100 scale.
        fee_multiplier: Series fee multiplier from API (1.0 = standard).

    Returns:
        Net edge in percentage points (gross_edge - fee_cost).
    """
    fee_cost = kalshi_fee_cents(avg_price_cents, fee_multiplier, contracts=1.0)
    return gross_edge_pp - fee_cost
