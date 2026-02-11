"""Tests for analysis base utilities."""

from pathlib import Path

import pandas as pd
import pytest

from analysis.base import (
    AnalysisResult,
    ensure_output_dirs,
    validate_no_nulls,
    validate_prices,
    validate_row_count,
)


class TestAnalysisResult:
    def test_construction(self, tmp_path: Path) -> None:
        result = AnalysisResult(
            figure_paths=[tmp_path / "fig.png"],
            csv_path=tmp_path / "data.csv",
            summary="Test summary",
        )
        assert result.summary == "Test summary"
        assert len(result.figure_paths) == 1


class TestEnsureOutputDirs:
    def test_creates_directories(self, tmp_path: Path) -> None:
        figures_dir, data_dir = ensure_output_dirs(tmp_path / "output")
        assert figures_dir.exists()
        assert data_dir.exists()
        assert figures_dir.name == "figures"
        assert data_dir.name == "data"

    def test_idempotent(self, tmp_path: Path) -> None:
        output = tmp_path / "output"
        ensure_output_dirs(output)
        ensure_output_dirs(output)  # Should not raise


class TestValidatePrices:
    def test_valid_range(self) -> None:
        df = pd.DataFrame({"price": [0.0, 50.0, 100.0]})
        validate_prices(df, "price")  # Should not raise

    def test_negative_raises(self) -> None:
        df = pd.DataFrame({"price": [50.0, -1.0]})
        with pytest.raises(ValueError, match="out of"):
            validate_prices(df, "price")

    def test_over_100_raises(self) -> None:
        df = pd.DataFrame({"price": [50.0, 101.0]})
        with pytest.raises(ValueError, match="out of"):
            validate_prices(df, "price")


class TestValidateNoNulls:
    def test_clean_data(self) -> None:
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        validate_no_nulls(df, ["a", "b"])  # Should not raise

    def test_with_nulls(self) -> None:
        df = pd.DataFrame({"a": [1, None], "b": ["x", "y"]})
        with pytest.raises(ValueError, match="NULL"):
            validate_no_nulls(df, ["a"])


class TestValidateRowCount:
    def test_sufficient_rows(self) -> None:
        df = pd.DataFrame({"a": range(10)})
        validate_row_count(df, 5, "test")  # Should not raise

    def test_insufficient_rows(self) -> None:
        df = pd.DataFrame({"a": range(3)})
        with pytest.raises(ValueError, match="Expected at least"):
            validate_row_count(df, 10, "test")
