"""Scaffold isolated run workspaces for experiment runs."""

import shutil
from pathlib import Path

import click
import yaml

LAB_ROOT = Path(__file__).resolve().parent.parent
PROBLEMS_DIR = LAB_ROOT / "problems"
ARCHITECTURES_DIR = LAB_ROOT / "architectures"
RUNS_DIR = LAB_ROOT / "runs"


def _next_run_number() -> int:
    """Find the next available run number."""
    existing = [
        int(d.name.split("_")[0])
        for d in RUNS_DIR.iterdir()
        if d.is_dir() and d.name[0].isdigit()
    ]
    return max(existing, default=0) + 1


def _generate_pyproject(run_dir: Path, run_name: str) -> None:
    """Generate a minimal pyproject.toml for the run workspace."""
    content = f"""\
[project]
name = "kalshi-run-{run_name}"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "pyarrow>=18.0",
    "duckdb>=1.1",
    "pandas>=2.2",
    "numpy>=2.0",
    "scipy>=1.14",
    "matplotlib>=3.9",
    "click>=8.1",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "ruff>=0.8",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/analysis", "src/simulation", "src/util"]

[tool.ruff]
target-version = "py312"
line-length = 100

[tool.pytest.ini_options]
testpaths = ["tests"]
"""
    (run_dir / "pyproject.toml").write_text(content)


def _generate_claude_md(run_dir: Path, problem_name: str) -> None:
    """Generate workspace-level CLAUDE.md instructions."""
    content = f"""\
# {problem_name.title()} Analysis Workspace

You are analyzing prediction market data to discover profitable trading strategies.

## Data Access
- All data is in `data/` as Parquet files, queryable via DuckDB glob patterns
- See `brief.md` for dataset schema and analysis guidelines
- A `queries.py` scaffold is available in `src/util/` if present

## Where to Write
- Analysis code: `src/analysis/`
- Utility code: `src/util/`
- Simulation code: `src/simulation/`
- Tests: `tests/`
- Results: `results/strategies.csv` (required output format)

## Expected Output
Your final output must include `results/strategies.csv` with columns:
strategy_name, taker_side, category, fee_type, time_bucket, price_min, price_max, confidence, rationale
"""
    claude_dir = run_dir / ".claude"
    claude_dir.mkdir(parents=True, exist_ok=True)
    (claude_dir / "CLAUDE.md").write_text(content)


def create_run(arch: str, problem: str, name: str) -> Path:
    """Create an isolated run workspace.

    Args:
        arch: Architecture name (must exist in architectures/).
        problem: Problem name (must exist in problems/).
        name: Human-readable run name.

    Returns:
        Path to the created run directory.
    """
    problem_dir = PROBLEMS_DIR / problem
    if not problem_dir.exists():
        raise click.ClickException(f"Problem not found: {problem_dir}")

    arch_dir = ARCHITECTURES_DIR / arch
    arch_file = arch_dir / "arch.yaml" if arch_dir.is_dir() else ARCHITECTURES_DIR / f"{arch}.yaml"
    if not arch_file.exists():
        raise click.ClickException(f"Architecture not found: {arch}")

    run_num = _next_run_number()
    run_id = f"{run_num:03d}_{name}"
    run_dir = RUNS_DIR / run_id

    click.echo(f"Creating run workspace: {run_dir}")
    run_dir.mkdir(parents=True)

    # Symlink data
    data_link = run_dir / "data"
    data_target = problem_dir / "data"
    data_link.symlink_to(data_target.resolve())
    click.echo(f"  Linked data -> {data_target}")

    # Copy problem brief
    problem_md = problem_dir / "problem.md"
    if problem_md.exists():
        shutil.copy2(problem_md, run_dir / "brief.md")
        click.echo("  Copied problem brief -> brief.md")

    # Copy queries scaffold if available
    queries_file = problem_dir / "queries.py"
    if queries_file.exists():
        util_dir = run_dir / "src" / "util"
        util_dir.mkdir(parents=True)
        shutil.copy2(queries_file, util_dir / "queries.py")
        (util_dir / "__init__.py").touch()
        click.echo("  Copied queries scaffold -> src/util/queries.py")

    # Create standard directories
    for d in ["src/analysis", "src/simulation", "tests", "results"]:
        (run_dir / d).mkdir(parents=True, exist_ok=True)
        init = run_dir / d.split("/")[0] / "__init__.py"
        if not init.exists():
            init.touch()

    # Generate pyproject.toml
    _generate_pyproject(run_dir, name)
    click.echo("  Generated pyproject.toml")

    # Generate CLAUDE.md
    _generate_claude_md(run_dir, problem)
    click.echo("  Generated .claude/CLAUDE.md")

    # Create empty strategies.csv with header
    (run_dir / "results" / "strategies.csv").write_text(
        "strategy_name,taker_side,category,fee_type,time_bucket,price_min,price_max,confidence,rationale\n"
    )

    click.echo(f"\nRun workspace ready: {run_dir}")
    click.echo(f"  cd {run_dir} && uv sync --all-extras")
    return run_dir
