"""Download daily candlestick (OHLC) data for all traded markets.

Reads market/event data from existing parquet files to build the
(series_ticker, market_ticker) pairs needed for the candlestick endpoint.

Progress is written to a JSON status file that can be polled by a dashboard.
"""

import asyncio
import base64
import datetime
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com"
API_PREFIX = "/trade-api/v2"
STATUS_FILE = Path("/home/workspace/kalshi-analysis/data/candles/download_status.json")
OUTPUT_DIR = Path("/home/workspace/kalshi-analysis/data/candles")
CHUNK_SIZE = 10_000


def load_private_key() -> Any:
    pk_raw = os.environ["KALSHI_PRIVATE_KEY"]
    if "\n" not in pk_raw and "-----" in pk_raw:
        parts = pk_raw.split()
        header = " ".join(parts[:5])
        footer = " ".join(parts[-5:])
        body = "".join(parts[5:-5])
        lines = [header]
        for i in range(0, len(body), 64):
            lines.append(body[i : i + 64])
        lines.append(footer)
        pk_raw = "\n".join(lines)
    return serialization.load_pem_private_key(
        pk_raw.encode("utf-8"), password=None, backend=default_backend()
    )


class AuthenticatedClient:
    """Async HTTP client with Kalshi RSA-PSS auth and rate limiting."""

    def __init__(self, rate_limit: float = 18.0):
        self.api_key_id = os.environ["KALSHI_API_KEY_ID"]
        self.private_key = load_private_key()
        self.rate_limit = rate_limit
        self._min_interval = 1.0 / rate_limit
        self._last_request = 0.0
        self._lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None
        self.request_count = 0
        self.error_count = 0
        self.rate_limit_count = 0

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    def _sign(self, timestamp: str, method: str, path: str) -> str:
        msg = f"{timestamp}{method}{path.split('?')[0]}".encode("utf-8")
        sig = self.private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    async def get(self, path: str, params: dict | None = None, retries: int = 5) -> dict:
        full_path = API_PREFIX + path
        for attempt in range(retries):
            async with self._lock:
                now = time.monotonic()
                wait = self._min_interval - (now - self._last_request)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_request = time.monotonic()

            ts = str(int(datetime.datetime.now().timestamp() * 1000))
            headers = {
                "KALSHI-ACCESS-KEY": self.api_key_id,
                "KALSHI-ACCESS-SIGNATURE": self._sign(ts, "GET", full_path),
                "KALSHI-ACCESS-TIMESTAMP": ts,
            }
            try:
                resp = await self._client.get(
                    BASE_URL + full_path, headers=headers, params=params
                )
                self.request_count += 1

                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    self.rate_limit_count += 1
                    wait_time = min(2 ** attempt, 30)
                    logger.warning("Rate limited (attempt %d), waiting %ds", attempt + 1, wait_time)
                    await asyncio.sleep(wait_time)
                elif resp.status_code in (500, 502, 503, 504):
                    self.error_count += 1
                    wait_time = min(2 ** attempt, 60)
                    logger.warning("Server error %d (attempt %d), waiting %ds", resp.status_code, attempt + 1, wait_time)
                    await asyncio.sleep(wait_time)
                elif resp.status_code == 404:
                    return {"candlesticks": []}
                else:
                    self.error_count += 1
                    logger.error("Unexpected status %d: %s", resp.status_code, resp.text[:200])
                    return {"candlesticks": []}
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
                self.error_count += 1
                wait_time = min(2 ** attempt, 60)
                logger.warning("Connection error (attempt %d): %s, waiting %ds", attempt + 1, e, wait_time)
                await asyncio.sleep(wait_time)

        logger.error("All retries exhausted for %s", path)
        return {"candlesticks": []}


class ProgressTracker:
    """Tracks download progress and writes status to a JSON file."""

    def __init__(self, total_markets: int, resumed_markets: int = 0):
        self.total_markets = total_markets
        self.completed_markets = resumed_markets
        self.resumed_markets = resumed_markets
        self.total_candles = 0
        self.empty_markets = 0
        self.start_time = time.time()
        self.last_update = time.time()
        self.status = "running"
        self.current_ticker = ""
        self.errors = 0
        self.rate_limits = 0
        self.requests = 0
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        self._write()

    def update(self, ticker: str, candle_count: int, client: AuthenticatedClient):
        self.completed_markets += 1
        self.total_candles += candle_count
        if candle_count == 0:
            self.empty_markets += 1
        self.current_ticker = ticker
        self.errors = client.error_count
        self.rate_limits = client.rate_limit_count
        self.requests = client.request_count
        now = time.time()
        if now - self.last_update > 1.0 or self.completed_markets == self.total_markets:
            self.last_update = now
            self._write()

    def finish(self, status: str = "completed"):
        self.status = status
        self._write()

    def _write(self):
        elapsed = time.time() - self.start_time
        downloaded_this_session = self.completed_markets - self.resumed_markets
        rate = downloaded_this_session / elapsed if elapsed > 0 else 0
        remaining = self.total_markets - self.completed_markets
        eta_seconds = remaining / rate if rate > 0 else 0

        data = {
            "status": self.status,
            "total_markets": self.total_markets,
            "completed_markets": self.completed_markets,
            "total_candles": self.total_candles,
            "empty_markets": self.empty_markets,
            "percent_complete": round(self.completed_markets / self.total_markets * 100, 2) if self.total_markets > 0 else 0,
            "elapsed_seconds": round(elapsed, 1),
            "eta_seconds": round(eta_seconds, 1),
            "markets_per_second": round(rate, 1),
            "requests": self.requests,
            "errors": self.errors,
            "rate_limits": self.rate_limits,
            "current_ticker": self.current_ticker,
            "started_at": datetime.datetime.fromtimestamp(self.start_time).isoformat(),
            "updated_at": datetime.datetime.now().isoformat(),
        }
        STATUS_FILE.write_text(json.dumps(data, indent=2))


class CandleWriter:
    """Buffers and writes candlestick records to chunked parquet files."""

    def __init__(self):
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._buffer: list[dict] = []
        self._file_counter = 0
        self._total = 0

        existing = sorted(self.output_dir.glob("candles_*.parquet"))
        if existing:
            last_num = int(existing[-1].stem.split("_")[-1])
            self._file_counter = last_num + 1

    def add(self, ticker: str, series_ticker: str, records: list[dict]):
        for r in records:
            r["ticker"] = ticker
            r["series_ticker"] = series_ticker
        self._buffer.extend(records)
        while len(self._buffer) >= CHUNK_SIZE:
            self._flush(self._buffer[:CHUNK_SIZE])
            self._buffer = self._buffer[CHUNK_SIZE:]

    def finish(self):
        if self._buffer:
            self._flush(self._buffer)
            self._buffer = []

    def _flush(self, records: list[dict]):
        flat = []
        for r in records:
            row = {
                "ticker": r["ticker"],
                "series_ticker": r["series_ticker"],
                "end_period_ts": r.get("end_period_ts"),
                "volume": r.get("volume"),
                "volume_fp": r.get("volume_fp"),
                "open_interest": r.get("open_interest"),
                "open_interest_fp": r.get("open_interest_fp"),
            }
            for section in ("price", "yes_bid", "yes_ask"):
                s = r.get(section, {}) or {}
                for field in ("open", "high", "low", "close", "open_dollars", "high_dollars", "low_dollars", "close_dollars"):
                    row[f"{section}_{field}"] = s.get(field)
            price = r.get("price", {}) or {}
            for field in ("mean", "mean_dollars", "previous", "previous_dollars", "min", "min_dollars", "max", "max_dollars"):
                row[f"price_{field}"] = price.get(field)
            flat.append(row)

        table = pa.Table.from_pylist(flat)
        path = self.output_dir / f"candles_{self._file_counter:06d}.parquet"
        pq.write_table(table, path, compression="snappy")
        self._file_counter += 1
        self._total += len(flat)
        logger.debug("Wrote %d candle records to %s (total: %d)", len(flat), path, self._total)


def load_market_list(min_volume: int = 0, open_after: str | None = None) -> list[tuple[str, str]]:
    """Load (series_ticker, market_ticker) pairs from parquet files."""
    import duckdb
    con = duckdb.connect()
    where = f"m.volume >= {min_volume}"
    if open_after:
        where += f" AND m.open_time >= '{open_after}'"
    df = con.execute(f"""
        SELECT DISTINCT e.series_ticker, m.ticker
        FROM 'data/markets/*.parquet' m
        JOIN 'data/events/*.parquet' e ON m.event_ticker = e.event_ticker
        WHERE e.series_ticker IS NOT NULL AND e.series_ticker != ''
          AND {where}
        ORDER BY m.volume DESC
    """).df()
    return list(zip(df["series_ticker"], df["ticker"]))


def load_completed_tickers() -> set[str]:
    """Load tickers already downloaded from existing parquet files."""
    completed_file = OUTPUT_DIR / "completed_tickers.txt"
    if completed_file.exists():
        return set(completed_file.read_text().strip().split("\n"))
    return set()


def save_completed_ticker(ticker: str):
    completed_file = OUTPUT_DIR / "completed_tickers.txt"
    with open(completed_file, "a") as f:
        f.write(ticker + "\n")


async def download_candlesticks(
    min_volume: int = 0,
    rate_limit: float = 18.0,
    period_interval: int = 1440,
    open_after: str | None = None,
):
    """Main download loop."""
    os.chdir("/home/workspace/kalshi-analysis")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Loading market list (min_volume=%d, open_after=%s)...", min_volume, open_after)
    all_pairs = load_market_list(min_volume, open_after=open_after)
    logger.info("Found %d markets with series tickers", len(all_pairs))

    completed = load_completed_tickers()
    pairs = [(s, t) for s, t in all_pairs if t not in completed]
    logger.info("Resuming: %d already done, %d remaining", len(completed), len(pairs))

    resumed = len(completed & set(t for _, t in all_pairs))
    tracker = ProgressTracker(total_markets=len(all_pairs), resumed_markets=resumed)
    writer = CandleWriter()

    async with AuthenticatedClient(rate_limit=rate_limit) as client:
        for series_ticker, ticker in pairs:
            try:
                data = await client.get(
                    f"/series/{series_ticker}/markets/{ticker}/candlesticks",
                    params={"start_ts": 1609459200, "end_ts": int(time.time()), "period_interval": period_interval},
                )
                candles = data.get("candlesticks", [])
                if candles:
                    writer.add(ticker, series_ticker, candles)
                tracker.update(ticker, len(candles), client)
                save_completed_ticker(ticker)
            except Exception as e:
                logger.error("Error downloading %s: %s", ticker, e)
                tracker.update(ticker, 0, client)

    writer.finish()
    tracker.finish("completed")
    logger.info(
        "Done! %d markets, %d candles, %d requests",
        tracker.completed_markets,
        tracker.total_candles,
        tracker.requests,
    )


def main():
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Download Kalshi daily candlestick data")
    parser.add_argument("--min-volume", type=int, default=0, help="Minimum market volume to include")
    parser.add_argument("--rate-limit", type=float, default=18.0, help="Requests per second")
    parser.add_argument("--period", type=int, default=1440, choices=[1, 60, 1440], help="Candle period in minutes")
    parser.add_argument("--open-after", type=str, default=None, help="Only include markets opened on or after this date (YYYY-MM-DD)")
    args = parser.parse_args()

    asyncio.run(download_candlesticks(
        min_volume=args.min_volume,
        rate_limit=args.rate_limit,
        period_interval=args.period,
        open_after=args.open_after,
    ))


if __name__ == "__main__":
    main()
