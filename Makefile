.PHONY: install download download-series download-events download-markets download-trades test lint analysis analysis-r2 analysis-r3 report report-r2 report-r3

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

analysis:
	uv run python -m analysis.run_round_01

analysis-r2:
	uv run python -m analysis.run_round_02

analysis-r3:
	uv run python -m analysis.run_round_03

report:
	cd reports/round_01 && quarto render report.qmd

report-r2:
	cd reports/round_02 && quarto render report.qmd

report-r3:
	cd reports/round_03 && quarto render report.qmd
