"""Tests for strategy evaluation metric helpers."""

import pytest

from util.strategy import daily_capacity, kelly_fraction, payout_ratio_from_price, sharpe_proxy


class TestKellyFraction:
    def test_positive_edge(self) -> None:
        """Kelly > 0 when there's positive edge."""
        # 55% win rate at even money: f = (0.55 * 1.0 - 0.45) / 1.0 = 0.10
        assert kelly_fraction(0.55, 1.0) == pytest.approx(0.10)

    def test_no_edge_is_zero(self) -> None:
        """Kelly = 0 at fair odds."""
        assert kelly_fraction(0.50, 1.0) == pytest.approx(0.0)

    def test_negative_edge_clamped_to_zero(self) -> None:
        """Kelly clamps to 0 when edge is negative."""
        assert kelly_fraction(0.30, 1.0) == pytest.approx(0.0)

    def test_with_asymmetric_payout(self) -> None:
        """Kelly with non-even payout ratio."""
        # Buy at 25c: payout = 75/25 = 3.0
        # If win rate = 0.30: f = (0.30 * 3.0 - 0.70) / 3.0 = 0.0667
        assert kelly_fraction(0.30, 3.0) == pytest.approx(0.0667, abs=0.001)

    def test_zero_payout_ratio(self) -> None:
        """Zero payout ratio returns 0."""
        assert kelly_fraction(0.50, 0.0) == pytest.approx(0.0)


class TestPayoutRatioFromPrice:
    def test_even_money(self) -> None:
        """50c → payout ratio 1.0 (even money)."""
        assert payout_ratio_from_price(50.0) == pytest.approx(1.0)

    def test_longshot(self) -> None:
        """25c → payout ratio 3.0."""
        assert payout_ratio_from_price(25.0) == pytest.approx(3.0)

    def test_favorite(self) -> None:
        """75c → payout ratio 1/3."""
        assert payout_ratio_from_price(75.0) == pytest.approx(1.0 / 3.0)

    def test_boundary_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="must be in"):
            payout_ratio_from_price(0.0)

    def test_boundary_100_raises(self) -> None:
        with pytest.raises(ValueError, match="must be in"):
            payout_ratio_from_price(100.0)


class TestDailyCapacity:
    def test_simple_division(self) -> None:
        assert daily_capacity(1_000_000, 100) == pytest.approx(10_000)

    def test_zero_days_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            daily_capacity(1000, 0)

    def test_fractional_result(self) -> None:
        assert daily_capacity(10, 3) == pytest.approx(10.0 / 3.0)


class TestSharpeProxy:
    def test_positive_edge_positive_sharpe(self) -> None:
        """Positive net edge should produce positive Sharpe."""
        s = sharpe_proxy(5.0, 0.55, 50.0)
        assert s > 0

    def test_zero_edge_near_zero_sharpe(self) -> None:
        """Zero net edge produces near-zero Sharpe."""
        s = sharpe_proxy(0.0, 0.50, 50.0)
        assert s == pytest.approx(0.0, abs=0.01)

    def test_degenerate_win_rate_returns_zero(self) -> None:
        """Win rate of 0 or 1 returns 0."""
        assert sharpe_proxy(5.0, 0.0, 50.0) == pytest.approx(0.0)
        assert sharpe_proxy(5.0, 1.0, 50.0) == pytest.approx(0.0)

    def test_degenerate_price_returns_zero(self) -> None:
        """Extreme prices return 0."""
        assert sharpe_proxy(5.0, 0.5, 0.0) == pytest.approx(0.0)
        assert sharpe_proxy(5.0, 0.5, 100.0) == pytest.approx(0.0)
