"""Tests for the Kalshi API client."""

import pytest
from pytest_httpx import HTTPXMock

from download.client import KalshiClient, RateLimiter


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_basic_rate_limiting(self) -> None:
        limiter = RateLimiter(requests_per_second=1000)
        # Should be able to acquire quickly at high rate
        for _ in range(5):
            await limiter.acquire()


class TestKalshiClient:
    @pytest.mark.asyncio
    async def test_get_series(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            json={"series": [{"ticker": "S1", "title": "Series 1"}]},
        )

        async with KalshiClient(rate_limit=1000) as client:
            result = await client.get_series()

        assert len(result) == 1
        assert result[0]["ticker"] == "S1"

    @pytest.mark.asyncio
    async def test_paginate_markets(self, httpx_mock: HTTPXMock) -> None:
        # First page
        httpx_mock.add_response(
            json={
                "markets": [{"ticker": f"M{i}"} for i in range(3)],
                "cursor": "page2",
            },
        )
        # Second page (last)
        httpx_mock.add_response(
            json={
                "markets": [{"ticker": f"M{i}"} for i in range(3, 5)],
                "cursor": "",
            },
        )

        async with KalshiClient(rate_limit=1000) as client:
            all_markets = []
            async for batch, cursor in client.get_markets(limit=3):
                all_markets.extend(batch)

        assert len(all_markets) == 5

    @pytest.mark.asyncio
    async def test_paginate_with_resume_cursor(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            json={
                "events": [{"ticker": "E1"}],
                "cursor": "",
            },
        )

        async with KalshiClient(rate_limit=1000) as client:
            all_events = []
            async for batch, cursor in client.get_events(resume_cursor="saved_cursor"):
                all_events.extend(batch)

        assert len(all_events) == 1
        # Verify the cursor was passed in the request
        request = httpx_mock.get_requests()[0]
        assert "cursor=saved_cursor" in str(request.url)

    @pytest.mark.asyncio
    async def test_empty_response(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            json={"markets": [], "cursor": ""},
        )

        async with KalshiClient(rate_limit=1000) as client:
            batches = []
            async for batch, cursor in client.get_markets():
                batches.append(batch)

        assert len(batches) == 0

    @pytest.mark.asyncio
    async def test_trades_with_ticker(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            json={
                "trades": [{"trade_id": "t1", "ticker": "AAPL"}],
                "cursor": "",
            },
        )

        async with KalshiClient(rate_limit=1000) as client:
            all_trades = []
            async for batch, cursor in client.get_trades(ticker="AAPL"):
                all_trades.extend(batch)

        assert len(all_trades) == 1
        request = httpx_mock.get_requests()[0]
        assert "ticker=AAPL" in str(request.url)
