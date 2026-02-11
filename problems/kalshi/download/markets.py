"""Download all Kalshi market metadata."""

import logging
from pathlib import Path

from download.client import KalshiClient
from download.storage import CursorStore, ParquetChunkWriter

logger = logging.getLogger(__name__)

CURSOR_KEY = "markets"


async def download_markets(client: KalshiClient, data_dir: Path, resume: bool = True) -> int:
    """Download all market metadata with cursor-based pagination.

    ~615K+ markets, ~200-500 MB. Takes ~30 seconds at 20 req/s.
    Returns the number of records downloaded in this run.
    """
    output_dir = data_dir / "markets"
    writer = ParquetChunkWriter(output_dir, chunk_size=10_000, name_prefix="markets")
    cursor_store = CursorStore(data_dir)

    resume_cursor = None
    records_so_far = 0
    if resume:
        resume_cursor, records_so_far = cursor_store.load(CURSOR_KEY)
        if resume_cursor:
            logger.info(
                "Resuming markets download from cursor (previously downloaded %d)",
                records_so_far,
            )

    logger.info("Downloading market metadata...")
    total = records_so_far

    async for batch, cursor in client.get_markets(resume_cursor=resume_cursor):
        writer.add_records(batch)
        total += len(batch)

        if cursor:
            cursor_store.save(CURSOR_KEY, cursor, total)

        if total % 50_000 == 0:
            logger.info("Markets progress: %d records downloaded", total)

    writer.flush()
    cursor_store.clear(CURSOR_KEY)
    logger.info("Markets download complete: %d total records", total)
    return writer.total_written
