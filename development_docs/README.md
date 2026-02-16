# Development Docs Workflow

This directory tracks feature development decisions, research, and implementation summaries.

## Folder Naming

Create one subfolder per feature using:

`YYYYMMDD_HHMMSS_<concise_feature_name>`

Example:

`20260212_125554_ralph_loop_claude`

## Required Files Per Feature Folder

- `research.md`: External + internal research findings that informed design.
- `plan.md`: Approved implementation plan and sequencing.
- `summary.md`: Final implementation summary, written when the feature is complete.

## Process

1. At feature start, create the timestamped folder and write `research.md` + `plan.md`.
2. During development, update `plan.md` if the scope or decisions change.
3. At feature completion, write `summary.md` at the top level of that feature folder.
4. Keep entries concise, factual, and reproducible (include dates, assumptions, and source links where relevant).
