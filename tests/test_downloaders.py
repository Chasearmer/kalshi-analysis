"""Integration tests for downloader modules."""

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pytest_httpx import HTTPXMock

from download.client import KalshiClient
from download.events import download_events
from download.markets import download_markets
from download.series import download_series
from download.trades import download_trades


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


class TestSeriesDownloader:
    @pytest.mark.asyncio
    async def test_downloads_series(self, httpx_mock: HTTPXMock, data_dir: Path) -> None:
        httpx_mock.add_response(
            json={"series": [
                {"ticker": "S1", "title": "Sports", "category": "sports"},
                {"ticker": "S2", "title": "Politics", "category": "politics"},
            ]},
        )

        async with KalshiClient(rate_limit=1000) as client:
            count = await download_series(client, data_dir)

        assert count == 2
        files = list((data_dir / "series").glob("*.parquet"))
        assert len(files) == 1
        table = pq.read_table(files[0])
        assert len(table) == 2


class TestEventsDownloader:
    @pytest.mark.asyncio
    async def test_downloads_events(self, httpx_mock: HTTPXMock, data_dir: Path) -> None:
        httpx_mock.add_response(
            json={
                "events": [{"event_ticker": f"E{i}"} for i in range(3)],
                "cursor": "page2",
            },
        )
        httpx_mock.add_response(
            json={
                "events": [{"event_ticker": "E3"}],
                "cursor": "",
            },
        )

        async with KalshiClient(rate_limit=1000) as client:
            count = await download_events(client, data_dir, resume=False)

        assert count == 4


class TestMarketsDownloader:
    @pytest.mark.asyncio
    async def test_downloads_markets(self, httpx_mock: HTTPXMock, data_dir: Path) -> None:
        httpx_mock.add_response(
            json={
                "markets": [{"ticker": f"M{i}", "title": f"Market {i}"} for i in range(5)],
                "cursor": "",
            },
        )

        async with KalshiClient(rate_limit=1000) as client:
            count = await download_markets(client, data_dir, resume=False)

        assert count == 5


class TestTradesDownloader:
    @pytest.mark.asyncio
    async def test_downloads_trades(self, httpx_mock: HTTPXMock, data_dir: Path) -> None:
        httpx_mock.add_response(
            json={
                "trades": [
                    {"trade_id": f"t{i}", "ticker": "M1", "yes_price_dollars": "0.65"}
                    for i in range(3)
                ],
                "cursor": "",
            },
        )

        async with KalshiClient(rate_limit=1000) as client:
            count = await download_trades(client, data_dir, resume=False)

        assert count == 3
