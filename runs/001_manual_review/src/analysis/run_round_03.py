"""Run all Round 3 analyses and produce report artifacts.

Round 3 — Strategy Prototyping:
1. Close-proximity efficiency (investigative)
2. Fade YES >=60c (primary strategy)
3. Economics category reversal (category strategy)
4. Combined filters (multi-dimensional)
5. Strategy comparison (summary — must run last)
"""

import logging
import sys
from pathlib import Path

import click

from analysis import (
    close_proximity,
    combined_filters,
    economics_reversal,
    fade_yes,
    strategy_comparison,
)
from analysis.base import AnalysisResult

log = logging.getLogger(__name__)


def run_all(data_dir: Path, output_dir: Path) -> dict[str, AnalysisResult]:
    """Run all Round 3 analyses.

    Args:
        data_dir: Path to the root data directory.
        output_dir: Path to the output directory for report artifacts.

    Returns:
        Dict mapping analysis name to its result.
    """
    results: dict[str, AnalysisResult] = {}

    analyses = [
        ("close_proximity", close_proximity.run),
        ("fade_yes", fade_yes.run),
        ("economics_reversal", economics_reversal.run),
        ("combined_filters", combined_filters.run),
        ("strategy_comparison", strategy_comparison.run),  # must be last
    ]

    for name, run_fn in analyses:
        log.info("=" * 60)
        log.info("Running analysis: %s", name)
        log.info("=" * 60)
        result = run_fn(data_dir, output_dir)
        results[name] = result
        log.info("Summary: %s", result.summary)
        log.info("")

    return results


@click.command()
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path, exists=True),
    default=Path("data"),
    help="Path to the data directory.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("reports/round_03"),
    help="Path to the output directory for report artifacts.",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def main(data_dir: Path, output_dir: Path, verbose: bool) -> None:
    """Run Round 3: Strategy Prototyping analyses."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    results = run_all(data_dir, output_dir)

    click.echo("\n" + "=" * 60)
    click.echo("ROUND 3 RESULTS")
    click.echo("=" * 60)
    for name, result in results.items():
        click.echo(f"\n--- {name} ---")
        click.echo(result.summary)
        click.echo(f"  Figures: {[str(p) for p in result.figure_paths]}")
        click.echo(f"  CSV: {result.csv_path}")


if __name__ == "__main__":
    main()
