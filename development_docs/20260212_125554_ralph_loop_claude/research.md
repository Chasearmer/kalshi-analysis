# Research: Claude Ralph Loop (Initial)

Date: 2026-02-12

## Objective

Design a first fully automated "Ralph Wiggums" style loop for this repo, starting with Claude, with stop conditions for cost/time/tokens and iterative research over Kalshi data.

## Internal Repo Findings

- Current architecture definitions exist in `architectures/manual_review/arch.yaml`, `architectures/single_agent/arch.yaml`, and `architectures/attractor_loop/arch.yaml`.
- `single_agent` already conceptually matches autonomous iteration but is not wired to a concrete runtime yet.
- The main research process and standards are defined in `runs/001_manual_review/PROJECT_SEED.md`.
- `harness/scaffold.py` scaffolds runs and checks architecture existence, but does not currently persist a full run manifest (selected architecture + resolved hyperparameters + limits) into each run directory.
- `harness/cli.py` currently has `scaffold`, and placeholder `evaluate`/`compare`.

## Claude Agent SDK Findings

- Anthropic’s current SDK naming/docs are under "Agent SDK" (Python + TypeScript).
- Session continuation is supported (`resume`/session ID patterns), which fits long-running iterative loops.
- Budget and turn controls exist at SDK level (`max_budget_usd`, `max_turns`).
- Usage/cost data is exposed in results, enabling outer-loop stop logic for cumulative time/cost/tokens.
- Hooks include `PreCompact`, allowing pre-compaction checkpoints and summary updates before context is compacted.
- Compaction/context management is first-class in the SDK and can be combined with file-based persistent memory for long loops.

## External Ralph-Loop / Autonomous Loop Repo Findings

- `vercel-labs/ralph-loop-agent`: strong conceptual reference for outer-loop control and stop predicates, but tied to Vercel AI SDK patterns.
- `frankbria/ralph-claude-code`, `AnandChowdhary/continuous-claude`, `disler/infinite-agentic-loop`, `context-machine-lab/sleepless-agent`: useful for patterns, but not direct drop-in fits for this repo’s Python + DuckDB + analysis pipeline.
- Recommendation: re-implement a lightweight loop in this repo using Claude Agent SDK directly, borrowing only high-level patterns.

## Architecture Organization Findings

Question considered: separate architectures (`claude_loop`, `claude_ralph_loop`, `codex_ralph_loop`) vs one generic `ralph_loop` with model/provider hyperparameters.

Conclusion:

- Better long-term structure is one generic `ralph_loop` architecture with provider/model as hyperparameters.
- This keeps experiment comparison cleaner and avoids duplicating loop logic across provider-specific architecture folders.
- Provider-specific differences should live in runner adapters/config fields, not in duplicated architecture definitions.

## Clarification: "Experiment Registry Format"

This does not require a new backend database.

It means: where per-iteration experiment metadata lives (question, hypothesis, script path, output artifacts, metrics, decision).

For this repo, a file-based approach is sufficient:

- Run-level state in `runs/<run_id>/...`
- Structured records in CSV/JSONL (or lightweight SQLite only if needed later)

Given current direction, prefer run-local files over introducing a new DB.

## Clarification: Memory, Continuity, and Pre-Compact Checkpointing

- Research memory/continuity artifacts: persistent markdown/csv files the loop updates each iteration (`findings.md`, `open_questions.md`, `strategies.md`, etc.).
- Standardized docs: fixed file names and schemas so each iteration can reliably load prior state.
- Compaction-aware checkpointing: before context compaction, write an explicit checkpoint file summarizing current round status and unresolved threads.
- `PreCompact` hook usage: trigger that checkpoint write right before compaction to reduce loss of active context.

## Sources

- Anthropic Agent SDK overview: https://platform.claude.com/docs/en/agent-sdk/overview
- Anthropic Agent SDK (Python): https://platform.claude.com/docs/en/agent-sdk/python
- Anthropic Agent SDK sessions: https://platform.claude.com/docs/en/agent-sdk/sessions
- Anthropic Agent SDK hooks: https://platform.claude.com/docs/en/agent-sdk/hooks
- Anthropic Agent SDK cost tracking: https://platform.claude.com/docs/en/agent-sdk/cost-tracking
- Anthropic compaction docs: https://platform.claude.com/docs/en/build-with-claude/compaction
- OpenAI Codex AGENTS docs page: https://developers.openai.com/codex/guides/agents-md
- OpenAI Codex docs file (`docs/agents_md.md`): https://raw.githubusercontent.com/openai/codex/main/docs/agents_md.md
- Ralph loop references:
  - https://github.com/vercel-labs/ralph-loop-agent
  - https://github.com/frankbria/ralph-claude-code
  - https://github.com/AnandChowdhary/continuous-claude
  - https://github.com/disler/infinite-agentic-loop
  - https://github.com/context-machine-lab/sleepless-agent
