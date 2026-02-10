"""Tests for statistical helper functions."""

import numpy as np
import pandas as pd
import pytest

from util.stats import (
    bonferroni_correct,
    calibration_error,
    chi_squared_independence,
    excess_return,
    two_proportion_z_test,
    weighted_mean,
)


class TestWeightedMean:
    def test_uniform_weights(self) -> None:
        values = np.array([10.0, 20.0, 30.0])
        weights = np.array([1.0, 1.0, 1.0])
        assert weighted_mean(values, weights) == pytest.approx(20.0)

    def test_skewed_weights(self) -> None:
        values = np.array([10.0, 20.0])
        weights = np.array([3.0, 1.0])
        assert weighted_mean(values, weights) == pytest.approx(12.5)

    def test_zero_weights_raises(self) -> None:
        values = np.array([10.0, 20.0])
        weights = np.array([0.0, 0.0])
        with pytest.raises(ValueError, match="zero"):
            weighted_mean(values, weights)

    def test_single_element(self) -> None:
        assert weighted_mean(np.array([42.0]), np.array([5.0])) == pytest.approx(42.0)


class TestExcessReturn:
    def test_positive_edge(self) -> None:
        # 60% win rate at 50c implied → +10pp
        assert excess_return(0.60, 50.0) == pytest.approx(0.10)

    def test_negative_edge(self) -> None:
        # 3% win rate at 10c implied → -7pp
        assert excess_return(0.03, 10.0) == pytest.approx(-0.07)

    def test_fair_price(self) -> None:
        # Win rate matches implied probability → 0
        assert excess_return(0.50, 50.0) == pytest.approx(0.0)


class TestCalibrationError:
    def test_perfect_calibration(self) -> None:
        df = pd.DataFrame({
            "win_rate": [5.0, 15.0, 25.0, 35.0, 45.0],
            "midpoint": [5.0, 15.0, 25.0, 35.0, 45.0],
            "contracts": [1000, 1000, 1000, 1000, 1000],
        })
        err = calibration_error(df, "win_rate", "midpoint", "contracts")
        assert err == pytest.approx(0.0)

    def test_known_miscalibration(self) -> None:
        df = pd.DataFrame({
            "win_rate": [3.0, 15.0],
            "midpoint": [5.0, 15.0],
            "contracts": [100, 100],
        })
        # Error = (|3-5| + |15-15|) / 2 = 1.0
        err = calibration_error(df, "win_rate", "midpoint", "contracts")
        assert err == pytest.approx(1.0)


class TestTwoProportionZTest:
    def test_equal_proportions(self) -> None:
        z, p = two_proportion_z_test(500, 1000, 500, 1000)
        assert abs(z) < 0.01
        assert p > 0.99

    def test_different_proportions(self) -> None:
        z, p = two_proportion_z_test(700, 1000, 300, 1000)
        assert abs(z) > 10
        assert p < 0.001

    def test_asymmetric_sample_sizes(self) -> None:
        z, p = two_proportion_z_test(80, 100, 7000, 10000)
        assert abs(z) > 2
        assert p < 0.05


class TestChiSquaredIndependence:
    def test_uniform_table(self) -> None:
        # Equal proportions across rows → not significant
        observed = np.array([[50, 50], [50, 50], [50, 50]])
        chi2, p, dof = chi_squared_independence(observed)
        assert chi2 == pytest.approx(0.0)
        assert p > 0.99
        assert dof == 2

    def test_skewed_table(self) -> None:
        # Very different proportions → significant
        observed = np.array([[90, 10], [10, 90]])
        chi2, p, dof = chi_squared_independence(observed)
        assert chi2 > 100
        assert p < 0.001
        assert dof == 1

    def test_single_row_zero_dof(self) -> None:
        # Single row has 0 degrees of freedom
        observed = np.array([[50, 50]])
        chi2, p, dof = chi_squared_independence(observed)
        assert dof == 0


class TestBonferroniCorrect:
    def test_basic_correction(self) -> None:
        corrected = bonferroni_correct([0.03, 0.03, 0.03])
        assert corrected == [pytest.approx(0.09), pytest.approx(0.09), pytest.approx(0.09)]

    def test_caps_at_one(self) -> None:
        corrected = bonferroni_correct([0.6, 0.01])
        assert corrected == [pytest.approx(1.0), pytest.approx(0.02)]

    def test_single_value(self) -> None:
        corrected = bonferroni_correct([0.05])
        assert corrected == [pytest.approx(0.05)]

    def test_empty_list(self) -> None:
        corrected = bonferroni_correct([])
        assert corrected == []
