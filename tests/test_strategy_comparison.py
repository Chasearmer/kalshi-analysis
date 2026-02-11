"""Tests for strategy comparison summary analysis."""

from pathlib import Path

import pandas as pd
import pytest

from analysis.strategy_comparison import run


@pytest.fixture()
def fixture_with_csvs(tmp_path: Path) -> tuple[Path, Path]:
    """Create mock CSVs mimicking output from other strategy modules.

    Returns (data_dir, output_dir) where output_dir has pre-populated CSVs.
    """
    # data_dir is unused by strategy_comparison but required by interface
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    output_dir = tmp_path / "output"
    csv_dir = output_dir / "data"
    csv_dir.mkdir(parents=True)
    (output_dir / "figures").mkdir()

    # fade_yes.csv
    pd.DataFrame(
        {
            "breakdown_type": ["price_bin", "price_bin", "category"],
            "breakdown_value": ["60c", "70c", "Sports"],
            "gross_edge_pp": [3.0, 4.5, 2.0],
            "fee_cost_pp": [2.45, 2.1, 2.3],
            "net_edge_pp": [0.55, 2.4, -0.3],
            "win_rate_pct": [63.0, 74.5, 62.0],
            "avg_price": [65.0, 75.0, 65.0],
            "avg_fee_mult": [0.07, 0.07, 0.07],
            "total_contracts": [5_000_000, 3_000_000, 4_000_000],
            "daily_cap": [2976, 1786, 2381],
            "kelly": [0.02, 0.05, 0.01],
        }
    ).to_csv(csv_dir / "fade_yes.csv", index=False)

    # economics_reversal.csv
    pd.DataFrame(
        {
            "category": ["Economics", "Economics", "Elections"],
            "strategy": ["favorites_yes_70c+", "no_all_prices", "no_all_prices"],
            "description": [
                "Economics: YES-taker >=70c",
                "Economics: NO-taker all prices",
                "Elections: NO-taker all prices",
            ],
            "gross_edge_pp": [8.0, 2.5, 5.0],
            "fee_cost_pp": [2.1, 1.5, 3.5],
            "net_edge_pp": [5.9, 1.0, 1.5],
            "win_rate_pct": [83.0, 52.5, 55.0],
            "avg_price": [78.0, 45.0, 50.0],
            "total_contracts": [200_000, 500_000, 800_000],
            "daily_cap": [119, 298, 476],
            "kelly": [0.08, 0.02, 0.03],
        }
    ).to_csv(csv_dir / "economics_reversal.csv", index=False)

    # combined_filters.csv
    pd.DataFrame(
        {
            "rank": [1, 2, 3],
            "filter_combination": [
                "no | quadratic_with_maker_fees | evening | Sports | high_price",
                "no | quadratic_with_maker_fees | other | Sports | high_price",
                "yes | quadratic | evening | Economics | high_price",
            ],
            "taker_side": ["no", "no", "yes"],
            "fee_type": ["quadratic_with_maker_fees", "quadratic_with_maker_fees", "quadratic"],
            "time_bucket": ["evening", "other", "evening"],
            "category": ["Sports", "Sports", "Economics"],
            "price_range": ["high_price", "high_price", "high_price"],
            "gross_edge_pp": [5.0, 4.0, 6.0],
            "fee_cost_pp": [2.0, 2.0, 2.5],
            "net_edge_pp": [3.0, 2.0, 3.5],
            "win_rate_pct": [68.0, 66.0, 71.0],
            "avg_price": [70.0, 70.0, 75.0],
            "total_contracts": [10_000_000, 15_000_000, 500_000],
            "daily_cap": [5952, 8929, 298],
            "total_extractable": [17857, 17857, 1042],
            "kelly": [0.04, 0.03, 0.06],
        }
    ).to_csv(csv_dir / "combined_filters.csv", index=False)

    return data_dir, output_dir


class TestStrategyComparisonRun:
    def test_produces_output(self, fixture_with_csvs: tuple[Path, Path], tmp_path: Path) -> None:
        """run() returns AnalysisResult with 3 figures and a CSV."""
        data_dir, output_dir = fixture_with_csvs
        result = run(data_dir, output_dir)

        assert len(result.figure_paths) == 3
        for fig_path in result.figure_paths:
            assert fig_path.exists()
        assert result.csv_path.exists()
        assert len(result.summary) > 0

    def test_csv_has_tier_and_rank(
        self, fixture_with_csvs: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """CSV includes tier assignment and ranking."""
        data_dir, output_dir = fixture_with_csvs
        result = run(data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        assert "tier" in df.columns
        assert "rank" in df.columns
        assert "sharpe" in df.columns

    def test_aggregates_all_sources(
        self, fixture_with_csvs: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """CSV includes strategies from all three source modules."""
        data_dir, output_dir = fixture_with_csvs
        result = run(data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        sources = set(df["source"].unique())
        assert "fade_yes" in sources
        assert "economics_reversal" in sources
        assert "combined_filters" in sources

    def test_ranking_by_extractable_edge(
        self, fixture_with_csvs: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Strategies are ranked by total_extractable descending."""
        data_dir, output_dir = fixture_with_csvs
        result = run(data_dir, output_dir)

        df = pd.read_csv(result.csv_path)
        extractable = df["total_extractable"].tolist()
        assert extractable == sorted(extractable, reverse=True)

    def test_handles_missing_csvs(self, tmp_path: Path) -> None:
        """Gracefully handles missing input CSVs."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        output_dir = tmp_path / "output"

        result = run(data_dir, output_dir)
        assert result.csv_path.exists()
        assert "No strategy data" in result.summary
