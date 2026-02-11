"""Kalshi API v2 client with rate limiting, retry, and cursor pagination."""

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Basic tier: 20 reads/sec
DEFAULT_RATE_LIMIT = 20


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient errors worth retrying."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    if isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout)):
        return True
    return False


class RateLimiter:
    """Token-bucket rate limiter for API requests."""

    def __init__(self, requests_per_second: float = DEFAULT_RATE_LIMIT):
        self.rps = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self._lock = asyncio.Lock()
        self._last_request: float = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self._last_request = time.monotonic()


class KalshiClient:
    """Async HTTP client for the Kalshi public API v2."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        rate_limit: float = DEFAULT_RATE_LIMIT,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.rate_limiter = RateLimiter(rate_limit)
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "KalshiClient":
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(self.timeout),
            headers={"Accept": "application/json"},
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        reraise=True,
    )
    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a rate-limited GET request with retry."""
        assert self._client is not None, "Client not initialized. Use async with."
        await self.rate_limiter.acquire()
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def paginate(
        self,
        path: str,
        response_key: str,
        params: dict[str, Any] | None = None,
        limit: int = 1000,
        resume_cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """Yield pages of results from a cursor-paginated endpoint.

        Yields (records, cursor) tuples. The cursor can be saved and passed
        as resume_cursor to continue an interrupted download.
        """
        request_params = dict(params or {})
        request_params["limit"] = limit
        if resume_cursor:
            request_params["cursor"] = resume_cursor

        while True:
            data = await self._get(path, params=request_params)
            records = data.get(response_key, [])
            cursor = data.get("cursor", "")

            if records:
                yield records, cursor or None

            if not cursor or not records:
                break

            request_params["cursor"] = cursor

    async def get_markets(
        self,
        limit: int = 1000,
        resume_cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """Paginate through all markets."""
        async for batch, cursor in self.paginate(
            "/markets", "markets", limit=limit, resume_cursor=resume_cursor
        ):
            yield batch, cursor

    async def get_events(
        self,
        limit: int = 200,
        resume_cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """Paginate through all events."""
        async for batch, cursor in self.paginate(
            "/events", "events", limit=limit, resume_cursor=resume_cursor
        ):
            yield batch, cursor

    async def get_series(self) -> list[dict[str, Any]]:
        """Get all series (not paginated — small dataset)."""
        data = await self._get("/series")
        return data.get("series", [])

    async def get_trades(
        self,
        ticker: str | None = None,
        limit: int = 1000,
        resume_cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """Paginate through trades. If ticker is provided, gets trades for that market."""
        params: dict[str, Any] = {}
        if ticker:
            params["ticker"] = ticker
        async for batch, cursor in self.paginate(
            "/markets/trades", "trades", params=params, limit=limit,
            resume_cursor=resume_cursor,
        ):
            yield batch, cursor

    async def get_candlesticks(
        self,
        series_ticker: str,
        ticker: str,
        period_interval: int = 1440,
        limit: int = 1000,
        resume_cursor: str | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """Paginate through candlestick data for a specific market."""
        params = {
            "series_ticker": series_ticker,
            "period_interval": period_interval,
        }
        async for batch, cursor in self.paginate(
            f"/markets/{ticker}/candlesticks",
            "candlesticks",
            params=params,
            limit=limit,
            resume_cursor=resume_cursor,
        ):
            yield batch, cursor
