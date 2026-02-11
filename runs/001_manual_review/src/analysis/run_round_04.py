"""Run all Round 4 analyses and produce report artifacts.

Round 4 — Simulation & Backtesting:
1. Per-strategy P&L backtests
2. Walk-forward validation (pre-2025 vs 2025+)
3. Drawdown analysis
4. Strategy decay over time
5. Fill rate sensitivity
6. Multi-strategy portfolio
"""

import logging
import sys
from pathlib import Path

import click

from analysis import (
    backtest_pnl,
    drawdown_analysis,
    fill_rate_sensitivity,
    portfolio_analysis,
    strategy_decay,
    walk_forward_analysis,
)
from analysis.base import AnalysisResult

log = logging.getLogger(__name__)


def run_all(data_dir: Path, output_dir: Path) -> dict[str, AnalysisResult]:
    """Run all Round 4 analyses.

    Args:
        data_dir: Path to the root data directory.
        output_dir: Path to the output directory for report artifacts.

    Returns:
        Dict mapping analysis name to its result.
    """
    results: dict[str, AnalysisResult] = {}

    analyses = [
        ("backtest_pnl", backtest_pnl.run),
        ("walk_forward", walk_forward_analysis.run),
        ("drawdown", drawdown_analysis.run),
        ("strategy_decay", strategy_decay.run),
        ("fill_rate_sensitivity", fill_rate_sensitivity.run),
        ("portfolio", portfolio_analysis.run),
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
    default=Path("reports/round_04"),
    help="Path to the output directory for report artifacts.",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def main(data_dir: Path, output_dir: Path, verbose: bool) -> None:
    """Run Round 4: Simulation & Backtesting analyses."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    results = run_all(data_dir, output_dir)

    click.echo("\n" + "=" * 60)
    click.echo("ROUND 4 RESULTS")
    click.echo("=" * 60)
    for name, result in results.items():
        click.echo(f"\n--- {name} ---")
        click.echo(result.summary)
        click.echo(f"  Figures: {[str(p) for p in result.figure_paths]}")
        click.echo(f"  CSV: {result.csv_path}")


if __name__ == "__main__":
    main()
