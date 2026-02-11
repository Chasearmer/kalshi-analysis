"""Tests for Kalshi fee calculation utilities."""

import pytest

from util.fees import kalshi_fee_cents, net_edge_pp


class TestKalshiFeeCents:
    def test_fee_symmetric_around_50c(self) -> None:
        """Fee at price p equals fee at price 100-p (quadratic P*(1-P) is symmetric)."""
        fee_mult = 1.0
        assert kalshi_fee_cents(30.0, fee_mult) == pytest.approx(kalshi_fee_cents(70.0, fee_mult))
        assert kalshi_fee_cents(10.0, fee_mult) == pytest.approx(kalshi_fee_cents(90.0, fee_mult))

    def test_fee_maximum_at_50c(self) -> None:
        """Fee is highest at 50c where P*(1-P) = 0.25."""
        fee_mult = 1.0
        fee_50 = kalshi_fee_cents(50.0, fee_mult)
        fee_30 = kalshi_fee_cents(30.0, fee_mult)
        fee_80 = kalshi_fee_cents(80.0, fee_mult)

        assert fee_50 > fee_30
        assert fee_50 > fee_80
        # 0.07 * 1.0 * 0.50 * 0.50 * 100 = 1.75 cents
        assert fee_50 == pytest.approx(1.75)

    def test_fee_at_known_price(self) -> None:
        """Verify fee at 65c with standard fee_mult=1.0."""
        # 0.07 * 1.0 * 0.65 * 0.35 * 100 = 1.5925 cents
        assert kalshi_fee_cents(65.0, 1.0) == pytest.approx(1.5925)

    def test_fee_scales_with_contracts(self) -> None:
        """Fee scales linearly with contract count."""
        fee_1 = kalshi_fee_cents(60.0, 1.0, contracts=1.0)
        fee_10 = kalshi_fee_cents(60.0, 1.0, contracts=10.0)
        assert fee_10 == pytest.approx(fee_1 * 10.0)

    def test_fee_at_extremes_is_small(self) -> None:
        """Fee approaches zero at price extremes."""
        fee_5 = kalshi_fee_cents(5.0, 1.0)
        fee_95 = kalshi_fee_cents(95.0, 1.0)
        # 0.07 * 1.0 * 0.05 * 0.95 * 100 = 0.3325 cents
        assert fee_5 == pytest.approx(0.3325)
        assert fee_95 == pytest.approx(0.3325)

    def test_fee_zero_multiplier(self) -> None:
        """Zero fee multiplier produces zero fee."""
        assert kalshi_fee_cents(50.0, 0.0) == pytest.approx(0.0)

    def test_fee_half_multiplier(self) -> None:
        """Half fee multiplier (e.g. S&P 500 series) halves the fee."""
        fee_full = kalshi_fee_cents(50.0, 1.0)
        fee_half = kalshi_fee_cents(50.0, 0.5)
        assert fee_half == pytest.approx(fee_full / 2.0)
        # 0.07 * 0.5 * 0.25 * 100 = 0.875 cents
        assert fee_half == pytest.approx(0.875)


class TestNetEdgePP:
    def test_net_edge_less_than_gross(self) -> None:
        """Net edge is always less than gross edge when fees > 0."""
        net = net_edge_pp(5.0, 65.0, 1.0)
        assert net < 5.0

    def test_net_edge_equals_gross_minus_fee(self) -> None:
        """Net edge = gross edge - fee cost per contract."""
        gross = 5.0
        price = 65.0
        fee_mult = 1.0
        fee_cost = kalshi_fee_cents(price, fee_mult)

        net = net_edge_pp(gross, price, fee_mult)
        assert net == pytest.approx(gross - fee_cost)

    def test_negative_net_edge(self) -> None:
        """Small gross edge can be overcome by fees."""
        # At 50c, fee = 1.75 cents; gross edge of 1pp is insufficient
        net = net_edge_pp(1.0, 50.0, 1.0)
        assert net < 0

    def test_net_edge_zero_fee_multiplier(self) -> None:
        """With zero fees, net edge equals gross edge."""
        assert net_edge_pp(3.0, 50.0, 0.0) == pytest.approx(3.0)
