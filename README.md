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

# Scaffold and run Ralph loop (Claude-first)
uv run kalshi-lab scaffold --arch ralph_loop --problem kalshi --name ralph_loop_trial

# Run (defaults to execution.mode=container from architecture config).
# The runner image auto-builds when missing or stale.
uv run kalshi-lab run --name 002_ralph_loop_trial

# Resume an interrupted/stopped run and extend budget
uv run kalshi-lab resume --name 002_ralph_loop_trial --extend-cost-usd 10

# Optional: prebuild image manually (usually not needed)
docker build -f docker/claude-runner.Dockerfile -t kalshi-lab-claude-runner:latest .

# Optional: force host mode (no container isolation)
uv run kalshi-lab run --name 002_ralph_loop_trial --execution-mode host

# Optional: use Podman instead of Docker (if installed)
uv run kalshi-lab run --name 002_ralph_loop_trial --container-runtime podman

# Optional: disable container network explicitly (advanced)
uv run kalshi-lab run --name 002_ralph_loop_trial --container-network none
```

Authentication environment:

- Supported API key variables:
  - `ANTHROPIC_API_KEY`
  - `AGENT_LAB_ANTHROPIC_API_KEY` (automatically mapped to `ANTHROPIC_API_KEY` for runner containers)
