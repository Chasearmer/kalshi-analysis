.PHONY: install test lint format analysis analysis-r2 analysis-r3 analysis-r4 report report-r2 report-r3 report-r4

install:
	uv sync --all-extras

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

analysis-r4:
	uv run python -m analysis.run_round_04

report:
	cd reports/round_01 && quarto render report.qmd

report-r2:
	cd reports/round_02 && quarto render report.qmd

report-r3:
	cd reports/round_03 && quarto render report.qmd

report-r4:
	cd reports/round_04 && quarto render report.qmd
