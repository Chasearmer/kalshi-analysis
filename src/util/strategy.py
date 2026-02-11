"""Strategy evaluation metric helpers.

Provides Kelly criterion sizing, payout ratio conversion, capacity estimation,
and a simplified annualized Sharpe ratio proxy for comparing strategy candidates.
"""

import math


def kelly_fraction(win_rate: float, payout_ratio: float) -> float:
    """Compute optimal Kelly fraction for a binary bet.

    Args:
        win_rate: Probability of winning (0.0 to 1.0).
        payout_ratio: Ratio of win amount to bet amount.
            For binary contracts bought at price p: (100 - p) / p.

    Returns:
        Optimal fraction of bankroll to bet (>= 0.0).
        Returns 0.0 if there is no positive edge.
    """
    if payout_ratio <= 0:
        return 0.0
    kelly = (win_rate * payout_ratio - (1.0 - win_rate)) / payout_ratio
    return max(0.0, kelly)


def payout_ratio_from_price(price_cents: float) -> float:
    """Convert contract price to payout ratio.

    Args:
        price_cents: Buy price on 0-100 scale (exclusive of 0 and 100).

    Returns:
        Payout ratio: win_amount / cost.
        Example: 30c → win 70c → payout = 70/30 ≈ 2.33.

    Raises:
        ValueError: If price is not in (0, 100).
    """
    if price_cents <= 0 or price_cents >= 100:
        raise ValueError(f"Price must be in (0, 100), got {price_cents}")
    return (100.0 - price_cents) / price_cents


def daily_capacity(total_contracts: float, days_in_dataset: float) -> float:
    """Compute average daily trading capacity.

    Args:
        total_contracts: Total contracts matching strategy criteria.
        days_in_dataset: Number of calendar days in dataset.

    Returns:
        Average contracts per day.

    Raises:
        ValueError: If days_in_dataset is not positive.
    """
    if days_in_dataset <= 0:
        raise ValueError(f"days_in_dataset must be positive, got {days_in_dataset}")
    return total_contracts / days_in_dataset


def sharpe_proxy(net_edge_pp: float, win_rate: float, avg_price_cents: float) -> float:
    """Compute annualized Sharpe ratio proxy for a binary-outcome strategy.

    Simplified estimate assuming independent bets and constant position sizing.

    Args:
        net_edge_pp: Net excess return in percentage points.
        win_rate: Winning probability (0.0 to 1.0).
        avg_price_cents: Average entry price on 0-100 scale.

    Returns:
        Approximate annualized Sharpe ratio. Returns 0.0 for degenerate inputs.
    """
    if win_rate <= 0 or win_rate >= 1 or avg_price_cents <= 0 or avg_price_cents >= 100:
        return 0.0

    p = avg_price_cents
    win_amount = 100.0 - p
    loss_amount = p

    mean_return = win_rate * win_amount - (1.0 - win_rate) * loss_amount
    variance = (
        win_rate * (win_amount - mean_return) ** 2
        + (1.0 - win_rate) * (-loss_amount - mean_return) ** 2
    )
    std = math.sqrt(variance) if variance > 0 else 0.0

    if std == 0:
        return 0.0

    sharpe_per_bet = net_edge_pp / std
    return sharpe_per_bet * math.sqrt(252)
