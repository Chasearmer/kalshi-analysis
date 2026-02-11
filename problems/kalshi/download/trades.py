"""Download all Kalshi historical trades.

This is the largest download (~100M+ trades, 3-5 GB, ~1.5-2 hours).
Strategy: paginate through all trades globally using the /markets/trades endpoint
without a ticker filter, which returns trades across all markets in chronological order.
"""

import logging
from pathlib import Path

from download.client import KalshiClient
from download.storage import CursorStore, ParquetChunkWriter

logger = logging.getLogger(__name__)

CURSOR_KEY = "trades_global"


async def download_trades(client: KalshiClient, data_dir: Path, resume: bool = True) -> int:
    """Download all historical trades with cursor-based pagination.

    ~100M+ trades. Takes ~1.5-2 hours at 20 req/s basic tier.
    Returns the number of records downloaded in this run.
    """
    output_dir = data_dir / "trades"
    writer = ParquetChunkWriter(output_dir, chunk_size=10_000, name_prefix="trades")
    cursor_store = CursorStore(data_dir)

    resume_cursor = None
    records_so_far = 0
    if resume:
        resume_cursor, records_so_far = cursor_store.load(CURSOR_KEY)
        if resume_cursor:
            logger.info(
                "Resuming trades download from cursor (previously downloaded %d)",
                records_so_far,
            )

    logger.info("Downloading historical trades (this will take a while)...")
    total = records_so_far

    async for batch, cursor in client.get_trades(resume_cursor=resume_cursor):
        writer.add_records(batch)
        total += len(batch)

        if cursor:
            cursor_store.save(CURSOR_KEY, cursor, total)

        if total % 100_000 == 0:
            logger.info("Trades progress: %d records downloaded", total)

    writer.flush()
    cursor_store.clear(CURSOR_KEY)
    logger.info("Trades download complete: %d total records", total)
    return writer.total_written
