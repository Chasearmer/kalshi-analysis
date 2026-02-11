"""Statistical test helpers for Kalshi analysis.

All functions operate on pandas DataFrames or numpy arrays.
"""

import numpy as np
import pandas as pd


def weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    """Compute weighted mean. Weights must be non-negative and sum to > 0."""
    total_weight = np.sum(weights)
    if total_weight == 0:
        raise ValueError("Weights sum to zero")
    return float(np.sum(values * weights) / total_weight)


def excess_return(win_rate: float, price_cents: float) -> float:
    """Compute excess return: actual win rate minus implied probability.

    Args:
        win_rate: Actual win rate as a fraction (0.0 to 1.0).
        price_cents: Price in cents (0 to 100).

    Returns:
        Excess return as a fraction. Positive means profitable.
    """
    return win_rate - (price_cents / 100.0)


def calibration_error(
    bin_df: pd.DataFrame,
    win_rate_col: str,
    midpoint_col: str,
    weight_col: str,
) -> float:
    """Compute weighted mean absolute calibration error across bins.

    Args:
        bin_df: DataFrame with one row per price bin.
        win_rate_col: Column with actual win rate (0-100 scale).
        midpoint_col: Column with bin midpoint / implied probability (0-100 scale).
        weight_col: Column with weights (e.g., total contracts).

    Returns:
        Weighted mean absolute error in percentage points.
    """
    errors = np.abs(bin_df[win_rate_col].values - bin_df[midpoint_col].values)
    weights = bin_df[weight_col].values
    return weighted_mean(errors, weights)


def two_proportion_z_test(
    wins_a: int, n_a: int, wins_b: int, n_b: int
) -> tuple[float, float]:
    """Two-proportion z-test for comparing win rates.

    Tests H0: p_a = p_b against H1: p_a != p_b.

    Returns:
        (z_statistic, p_value) tuple.
    """
    from scipy import stats

    p_a = wins_a / n_a
    p_b = wins_b / n_b
    p_pool = (wins_a + wins_b) / (n_a + n_b)

    se = np.sqrt(p_pool * (1 - p_pool) * (1 / n_a + 1 / n_b))
    if se == 0:
        return 0.0, 1.0

    z = (p_a - p_b) / se
    p_value = 2 * stats.norm.sf(abs(z))
    return float(z), float(p_value)


def chi_squared_independence(observed: np.ndarray) -> tuple[float, float, int]:
    """Chi-squared test of independence on a contingency table.

    Args:
        observed: 2D array of observed counts (e.g., rows=hours, cols=[wins, losses]).

    Returns:
        (chi2_statistic, p_value, degrees_of_freedom) tuple.
    """
    from scipy.stats import chi2_contingency

    chi2, p, dof, _ = chi2_contingency(observed)
    return float(chi2), float(p), int(dof)


def bonferroni_correct(p_values: list[float]) -> list[float]:
    """Apply Bonferroni correction to a list of p-values.

    Returns corrected p-values, each capped at 1.0.
    """
    n = len(p_values)
    return [min(p * n, 1.0) for p in p_values]
