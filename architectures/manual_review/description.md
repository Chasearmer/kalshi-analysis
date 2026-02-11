# Manual Review Architecture

The baseline approach: a human reviews each round of AI-generated research
before steering the next round.

## Workflow

1. Agent reads PROJECT_SEED.md and previous round reports
2. Agent enters plan mode, designs experiments for the next round
3. Agent implements analysis scripts, runs them, produces figures + CSV
4. Agent compiles a Quarto report
5. Human reviews the report, provides feedback
6. Repeat from step 1 with accumulated findings

## Characteristics

- High quality per round (human catches errors, steers direction)
- Low throughput (limited by human review time)
- Sequential rounds (no parallelism)
- Strong iterative refinement (human can redirect completely between rounds)

## Run 001

The first and primary instance of this architecture. Completed 4 rounds of
research covering landscape analysis, bias mapping, strategy prototyping,
and backtesting simulation.
