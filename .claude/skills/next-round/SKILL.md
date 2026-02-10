---
name: next-round
description: Plan and execute the next round of iterative research analysis
disable-model-invocation: true
---

# Next Research Round

You are starting the next round of iterative research for the Kalshi trading strategy discovery project.

## Step 1: Read the Project Seed

Read `PROJECT_SEED.md` to understand the project vision, research roadmap (Section 5.5), engineering standards, and strategy taxonomy.

## Step 2: Review Previous Research

1. List all existing report directories under `reports/` to determine which rounds have been completed.
2. For each completed round, read the Quarto report source (`reports/round_XX/report.qmd`) to understand:
   - What questions were investigated
   - Key findings and results
   - Strategy candidates identified
   - Next steps recommended
   - Do NOT read `report.html` — only read `report.qmd`.
3. Read the analysis source code under `src/analysis/` to understand what infrastructure already exists.

## Step 3: Identify the Next Round Number

Based on completed rounds, determine which round comes next (e.g., if `round_01/` exists, plan `round_02`).

## Step 4: Write a Plan

Enter plan mode and write a detailed plan for the next round that includes:

- **Round number and title**
- **Research questions** — specific, measurable questions informed by previous findings and the research roadmap. Frame these as open inquiries ("Does X vary by Y?", "How large is the effect of Z?") rather than claims to prove. Let the data answer the question.
- **Analyses to implement** — concrete analysis modules with expected inputs/outputs, designed to answer each research question
- **Infrastructure needed** — any new shared utilities, query helpers, or stat functions required
- **Subagent strategy** — how many subagents to spawn, what work each one is assigned, and how the work is divided across them
- **Report structure** — what figures, tables, and sections the Quarto report should contain
- **Expected outputs** — what CSV data, figures, and findings each analysis should produce

The Quarto report for each round must begin with a concise **"Research Plan"** section that explains:

1. The research questions this round sets out to answer
2. The experiments and analyses designed to investigate each question
3. Any new infrastructure built to support the analyses
4. The number of subagents used and how work was divided/assigned among them

This section serves as a reader's roadmap before diving into results. Present findings objectively — report what the data shows, whether or not it matches prior expectations.

Prioritize analyses that build on previous findings and follow the roadmap order in the project seed. Focus on systematic/structural opportunities before predictive approaches.

## Step 5: Review Subagent Work

After subagents complete their work, rigorously review their output before assembling the report. For each subagent's work, check:

- **Correctness** — Are SQL queries joining on the right keys? Are filters applied correctly? Are win rates and excess returns computed properly?
- **Data integrity** — Do row counts, value ranges, and NULL handling look right? Are `_dollars` fields (not integer cent fields) being used?
- **Statistical soundness** — Are the right tests applied? Are sample sizes sufficient? Are conclusions supported by the evidence, or overstated?
- **Code quality** — Does it follow existing patterns in `src/`? Are there bugs, off-by-one errors, or silent failures?
- **Figures and outputs** — Do charts accurately represent the underlying data? Are axes labeled and scaled correctly?

If you find mistakes, unclear reasoning, or analysis gaps, send the subagent specific feedback and have it iterate until the work meets the project's engineering standards. Do not accept work that is merely "good enough" — this is quantitative research where subtle errors lead to wrong conclusions and real financial risk.

## Step 6: Render the Report

After all analyses are complete and the Quarto `.qmd` file is written, render it to HTML:

```bash
quarto render reports/round_XX/report.qmd
```

In your final response, include the full path to the rendered HTML file so the user can click to open it:

```
Report: reports/round_XX/report.html
```
