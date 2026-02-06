"""Download all Kalshi events metadata."""

import logging
from pathlib import Path

from download.client import KalshiClient
from download.storage import CursorStore, ParquetChunkWriter

logger = logging.getLogger(__name__)

CURSOR_KEY = "events"


async def download_events(client: KalshiClient, data_dir: Path, resume: bool = True) -> int:
    """Download all events metadata with cursor-based pagination.

    Returns the number of records downloaded in this run.
    """
    output_dir = data_dir / "events"
    writer = ParquetChunkWriter(output_dir, chunk_size=10_000, name_prefix="events")
    cursor_store = CursorStore(data_dir)

    resume_cursor = None
    records_so_far = 0
    if resume:
        resume_cursor, records_so_far = cursor_store.load(CURSOR_KEY)
        if resume_cursor:
            logger.info(
                "Resuming events download from cursor (previously downloaded %d)",
                records_so_far,
            )

    logger.info("Downloading events metadata...")
    total = records_so_far

    async for batch, cursor in client.get_events(resume_cursor=resume_cursor):
        writer.add_records(batch)
        total += len(batch)

        if cursor:
            cursor_store.save(CURSOR_KEY, cursor, total)

        if total % 5000 == 0 or total < 100:
            logger.info("Events progress: %d records downloaded", total)

    writer.flush()
    cursor_store.clear(CURSOR_KEY)
    logger.info("Events download complete: %d total records", total)
    return writer.total_written
