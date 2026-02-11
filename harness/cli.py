"""CLI entry point for the experiment lab harness."""

import click


@click.group()
def main():
    """Experiment lab for testing AI research architectures."""


@main.command()
@click.option("--arch", required=True, help="Architecture name (e.g., single_agent)")
@click.option("--problem", required=True, help="Problem name (e.g., kalshi)")
@click.option("--name", required=True, help="Run name (e.g., baseline_test)")
def scaffold(arch: str, problem: str, name: str):
    """Create an isolated run workspace."""
    from harness.scaffold import create_run

    create_run(arch=arch, problem=problem, name=name)


@main.command()
@click.option("--name", required=True, help="Run name to evaluate")
def evaluate(name: str):
    """Evaluate strategies from a completed run."""
    click.echo(f"Evaluating run: {name} (not yet implemented)")


@main.command()
def compare():
    """Compare results across all completed runs."""
    click.echo("Comparison not yet implemented")
