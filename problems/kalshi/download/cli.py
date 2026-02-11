"""CLI entry point for Kalshi data downloads."""

import asyncio
import logging
import sys
from pathlib import Path

import click

from download.client import KalshiClient
from download.events import download_events
from download.markets import download_markets
from download.series import download_series
from download.trades import download_trades

DEFAULT_DATA_DIR = Path("data")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )


@click.group()
@click.option("--data-dir", type=click.Path(path_type=Path), default=DEFAULT_DATA_DIR,
              help="Directory to store downloaded data.")
@click.option("--rate-limit", type=float, default=20.0,
              help="API requests per second (default: 20 for basic tier).")
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
@click.pass_context
def cli(ctx: click.Context, data_dir: Path, rate_limit: float, verbose: bool) -> None:
    """Download Kalshi market data for analysis."""
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["data_dir"] = data_dir
    ctx.obj["rate_limit"] = rate_limit


@cli.command()
@click.option("--no-resume", is_flag=True, help="Start fresh, ignoring saved cursors.")
@click.pass_context
def all(ctx: click.Context, no_resume: bool) -> None:
    """Download all data: series, events, markets, and trades."""
    asyncio.run(_download_all(ctx.obj, resume=not no_resume))


async def _download_all(config: dict, resume: bool = True) -> None:
    data_dir = config["data_dir"]
    rate_limit = config["rate_limit"]

    async with KalshiClient(rate_limit=rate_limit) as client:
        # Small datasets first
        n = await download_series(client, data_dir)
        click.echo(f"Series: {n} records")

        n = await download_events(client, data_dir, resume=resume)
        click.echo(f"Events: {n} records")

        n = await download_markets(client, data_dir, resume=resume)
        click.echo(f"Markets: {n} records")

        # Trades last (largest dataset)
        n = await download_trades(client, data_dir, resume=resume)
        click.echo(f"Trades: {n} records")

    click.echo("All downloads complete.")


@cli.command()
@click.pass_context
def series(ctx: click.Context) -> None:
    """Download series metadata."""
    asyncio.run(_download_one(ctx.obj, "series"))


@cli.command()
@click.option("--no-resume", is_flag=True, help="Start fresh, ignoring saved cursors.")
@click.pass_context
def events(ctx: click.Context, no_resume: bool) -> None:
    """Download events metadata."""
    asyncio.run(_download_one(ctx.obj, "events", resume=not no_resume))


@cli.command()
@click.option("--no-resume", is_flag=True, help="Start fresh, ignoring saved cursors.")
@click.pass_context
def markets(ctx: click.Context, no_resume: bool) -> None:
    """Download market metadata."""
    asyncio.run(_download_one(ctx.obj, "markets", resume=not no_resume))


@cli.command()
@click.option("--no-resume", is_flag=True, help="Start fresh, ignoring saved cursors.")
@click.pass_context
def trades(ctx: click.Context, no_resume: bool) -> None:
    """Download all historical trades."""
    asyncio.run(_download_one(ctx.obj, "trades", resume=not no_resume))


async def _download_one(config: dict, kind: str, resume: bool = True) -> None:
    data_dir = config["data_dir"]
    rate_limit = config["rate_limit"]

    downloaders = {
        "series": lambda c, d: download_series(c, d),
        "events": lambda c, d: download_events(c, d, resume=resume),
        "markets": lambda c, d: download_markets(c, d, resume=resume),
        "trades": lambda c, d: download_trades(c, d, resume=resume),
    }

    async with KalshiClient(rate_limit=rate_limit) as client:
        n = await downloaders[kind](client, data_dir)
        click.echo(f"{kind.capitalize()}: {n} records")


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
