"""Download all Kalshi series metadata."""

import logging
from pathlib import Path

from download.client import KalshiClient
from download.storage import ParquetChunkWriter

logger = logging.getLogger(__name__)


async def download_series(client: KalshiClient, data_dir: Path) -> int:
    """Download all series metadata. Returns the number of records downloaded."""
    output_dir = data_dir / "series"
    writer = ParquetChunkWriter(output_dir, chunk_size=10_000, name_prefix="series")

    logger.info("Downloading series metadata...")
    series = await client.get_series()

    if series:
        writer.add_records(series)
        writer.flush()
        logger.info("Downloaded %d series", len(series))
    else:
        logger.warning("No series data returned from API")

    return writer.total_written
