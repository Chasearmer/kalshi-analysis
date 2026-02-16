"""Shared filesystem paths for the experiment harness."""

from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent
PROBLEMS_DIR = LAB_ROOT / "problems"
ARCHITECTURES_DIR = LAB_ROOT / "architectures"
RUNS_DIR = LAB_ROOT / "runs"
