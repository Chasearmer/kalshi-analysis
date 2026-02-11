"""Base patterns for analysis modules.

Each analysis module implements a run() function that produces:
1. A matplotlib figure saved as PNG
2. A CSV summary file
3. A text summary string
"""

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class AnalysisResult:
    """Container for analysis outputs."""

    figure_paths: list[Path]
    csv_path: Path
    summary: str


def ensure_output_dirs(output_dir: Path) -> tuple[Path, Path]:
    """Create figures/ and data/ subdirectories under output_dir.

    Returns:
        (figures_dir, data_dir) tuple.
    """
    figures_dir = output_dir / "figures"
    data_dir = output_dir / "data"
    figures_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    return figures_dir, data_dir


def validate_prices(df: pd.DataFrame, price_col: str) -> None:
    """Assert all values in price_col are between 0 and 100 (inclusive)."""
    vals = df[price_col]
    out_of_range = (vals < 0) | (vals > 100)
    if out_of_range.any():
        bad = vals[out_of_range]
        raise ValueError(
            f"Price column '{price_col}' has {len(bad)} values out of [0, 100] range. "
            f"Min: {bad.min()}, Max: {bad.max()}"
        )


def validate_no_nulls(df: pd.DataFrame, columns: list[str]) -> None:
    """Assert no NULLs in specified columns."""
    for col in columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise ValueError(f"Column '{col}' has {null_count} NULL values")


def validate_row_count(df: pd.DataFrame, min_rows: int, context: str) -> None:
    """Assert df has at least min_rows rows."""
    if len(df) < min_rows:
        raise ValueError(
            f"{context}: Expected at least {min_rows} rows, got {len(df)}"
        )
