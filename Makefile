.PHONY: install download download-series download-events download-markets download-trades test lint

install:
	uv sync --all-extras

download:
	uv run kalshi-download all

download-series:
	uv run kalshi-download series

download-events:
	uv run kalshi-download events

download-markets:
	uv run kalshi-download markets

download-trades:
	uv run kalshi-download trades

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

format:
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/
