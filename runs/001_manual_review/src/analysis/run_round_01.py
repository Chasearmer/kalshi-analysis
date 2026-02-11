"""Run all Round 1 analyses and produce report artifacts.

Round 1 — Landscape & Calibration:
1. Dataset summary statistics
2. Overall calibration curve
3. Volume distribution
4. Maker/taker asymmetry
"""

import logging
import sys
from pathlib import Path

import click

from analysis import calibration, maker_taker, summary, volume
from analysis.base import AnalysisResult

log = logging.getLogger(__name__)


def run_all(data_dir: Path, output_dir: Path) -> dict[str, AnalysisResult]:
    """Run all Round 1 analyses.

    Args:
        data_dir: Path to the root data directory.
        output_dir: Path to the output directory for report artifacts.

    Returns:
        Dict mapping analysis name to its result.
    """
    results: dict[str, AnalysisResult] = {}

    analyses = [
        ("summary", summary.run),
        ("calibration", calibration.run),
        ("volume", volume.run),
        ("maker_taker", maker_taker.run),
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
    "--data-dir", type=click.Path(path_type=Path, exists=True), default=Path("data"),
    help="Path to the data directory.",
)
@click.option(
    "--output-dir", type=click.Path(path_type=Path), default=Path("reports/round_01"),
    help="Path to the output directory for report artifacts.",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def main(data_dir: Path, output_dir: Path, verbose: bool) -> None:
    """Run Round 1: Landscape & Calibration analyses."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    results = run_all(data_dir, output_dir)

    click.echo("\n" + "=" * 60)
    click.echo("ROUND 1 RESULTS")
    click.echo("=" * 60)
    for name, result in results.items():
        click.echo(f"\n--- {name} ---")
        click.echo(result.summary)
        click.echo(f"  Figures: {[str(p) for p in result.figure_paths]}")
        click.echo(f"  CSV: {result.csv_path}")


if __name__ == "__main__":
    main()
