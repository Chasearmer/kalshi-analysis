# Experiment Lab

Testing different meta-architectures for AI-driven scientific research, starting with Kalshi prediction market analysis.

## Structure

- `problems/` — Problem definitions and datasets. Each problem has data, a download pipeline, and a problem brief.
- `architectures/` — Meta-architecture definitions (manual review, single agent, attractor graph, etc.)
- `harness/` — Scaffolding, evaluation, and comparison tooling
- `runs/` — Isolated workspaces for each experiment run
- `comparison/` — Cross-run analysis output

## Runs

| Run | Architecture | Problem | Status |
|-----|-------------|---------|--------|
| 001 | Manual review | Kalshi | 4 rounds completed |

## Quick Start

```bash
# Continue manual review (run 001)
cd runs/001_manual_review
make install
make test

# Scaffold a new automated run
uv run kalshi-lab scaffold --arch single_agent --problem kalshi --name my_experiment
```
