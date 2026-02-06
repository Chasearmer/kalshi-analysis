# Kalshi Trading Strategy Discovery — Project Seed Document

## 1. Vision

Build an automated, iterative research system that:

1. Downloads all available Kalshi historical data (trades, markets, metadata)
2. Spawns parallel analysis agents that explore systematic opportunities
3. Produces human-readable Quarto reports summarizing findings
4. Iterates: each round of findings informs the next round of hypotheses
5. Graduates promising strategies into backtested simulations with realistic P&L tracking
6. Eventually explores predictive models (weather, sports, etc.) for markets where forecasting adds edge

The goal is to discover a **profitable, automatable trading protocol** — ideally one that exploits structural or systematic biases rather than requiring sophisticated prediction.

---

## 2. Data Acquisition Plan

### 2.1 What to Download

| Data Type | Endpoint | Records | Size (compressed) | Download Time | Priority |
|-----------|----------|---------|-------------------|---------------|----------|
| **All market metadata** | `GET /markets` (limit=1000, cursor-paginated) | ~615K+ markets | ~200-500 MB | ~30 seconds | **P0** |
| **All historical trades** | `GET /markets/trades` (per-ticker, cursor-paginated) | ~100M+ trades | ~3-5 GB | ~1.5-2 hours | **P0** |
| **Series metadata** | `GET /series` | ~hundreds | ~10 MB | ~instant | **P0** |
| **Events metadata** | `GET /events` (limit=200, cursor-paginated) | ~tens of thousands | ~50 MB | ~10 seconds | **P0** |
| **Daily OHLC candles** | `GET /markets/candlesticks` (batch, period=1440) | ~tens of millions | ~500 MB-1 GB | ~5 minutes | **P1** |

**Total P0 download: ~4-6 GB compressed, ~2 hours at basic rate tier (20 req/s).**

This is entirely feasible to run locally and store locally.

### 2.2 What NOT to Download (and Why)

**Candlestick data at 1-minute or 1-hour resolution** — skipped because:

- **Volume problem:** Candles exist for every time interval whether or not a trade occurred. A market open for 30 days generates 43,200 one-minute candles even if it only had 500 trades. Across 615K+ markets, this produces billions of mostly-empty rows.
- **Redundancy:** Trade-based OHLC can be reconstructed from tick data at any resolution on demand.
- **Size:** Estimated 50-200 GB for 1-min across all markets — orders of magnitude larger than the trade data itself.

**What candles uniquely provide that trades don't:** Each candle includes `yes_bid` and `yes_ask` OHLC — sampled orderbook state at each interval. This is the only way to get **historical spread/liquidity information** since Kalshi doesn't offer historical orderbook snapshots. If spread analysis becomes important for a specific strategy, we can selectively download hourly or daily candles for targeted markets rather than doing a bulk download.

**Historical orderbook data** — not available via the Kalshi API. Only point-in-time snapshots exist. Third-party services like PredictionData.dev claim 10B+ orderbook updates if needed. For forward-looking orderbook data, a WebSocket recorder could be built.

### 2.3 API Details

- **Base URL:** `https://api.elections.kalshi.com/trade-api/v2`
- **Auth for public data:** Not required. All market data, trades, events, series, and candlesticks are public endpoints.
- **Auth for trading/portfolio:** RSA key pair, generated at `https://kalshi.com/account/profile`. Headers: `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP`, `KALSHI-ACCESS-SIGNATURE` (RSA-PSS SHA-256 of `timestamp + method + path`).
- **Rate limits (Basic tier):** 20 reads/sec, 10 writes/sec. Higher tiers (Advanced: 30/30, Premier: 100/100, Prime: 400/400) require application + volume thresholds.
- **Pagination:** Cursor-based. Pass `cursor` param to get next page; empty cursor means end of results.
- **Deprecation notice (Feb 19, 2026):** Integer cent fields (`yes_price`, `no_price`) are deprecated in favor of `_dollars`/`_fp` fixed-point string equivalents. The new project should use the `_fp`/`_dollars` fields from the start.

### 2.4 Key Market Object Fields (60+)

**Identifiers:** `ticker`, `event_ticker`, `series_ticker`, `market_type` (binary/scalar)

**Pricing (dollars):** `yes_bid_dollars`, `yes_ask_dollars`, `no_bid_dollars`, `no_ask_dollars`, `last_price_dollars`, `previous_yes_bid_dollars`, `previous_yes_ask_dollars`, `previous_price_dollars`, `notional_value_dollars`, `liquidity_dollars`

**Volume:** `volume_fp`, `volume_24h_fp`, `open_interest_fp`

**Timestamps:** `created_time`, `updated_time`, `open_time`, `close_time`, `expected_expiration_time`, `latest_expiration_time`, `settlement_ts`, `fee_waiver_expiration_time`

**Settlement:** `result` (yes/no/scalar/empty), `settlement_value_dollars`, `expiration_value`, `status` (initialized/inactive/active/closed/determined/disputed/amended/finalized)

**Structure:** `strike_type`, `floor_strike`, `cap_strike`, `functional_strike`, `custom_strike`, `price_level_structure`, `price_ranges`

**Rules:** `rules_primary`, `rules_secondary`, `title`, `yes_sub_title`, `no_sub_title`

**Series metadata** includes: `category`, `tags`, `frequency`, `settlement_sources`, `fee_type` (quadratic/flat/quadratic_with_maker_fees), `fee_multiplier`

### 2.5 Key Trade Object Fields

`trade_id`, `ticker`, `yes_price_dollars` / `no_price_dollars` (fixed-point strings), `count_fp`, `taker_side` ("yes"/"no"), `created_time`

### 2.6 Forecast Percentile History (Future Enhancement)

Available at 5-sec, 1-min, 1-hour, 1-day resolution per event. Could be valuable for calibration studies. Available via `GET /series/{series}/events/{event}/forecast_percentile_history`.

---

## 3. Insights from Existing Research

A similar codebase (by jbecker, published as "The Microstructure of Wealth Transfer in Prediction Markets") contains 20+ analysis scripts and reveals several confirmed market phenomena worth reassessing building on:

### 3.1 Confirmed Phenomena

**Longshot Bias:** YES contracts at low prices (longshots) have negative expected value. Win rates are systematically below implied probabilities at price extremes. This is the single most robust finding across prediction markets.

**Maker-Taker Asymmetry:** Makers (passive liquidity providers) outperform takers (aggressive order initiators) by ~2-4 percentage points on average. This gap is larger in retail-dominated categories (Sports) and smaller in sophisticated categories (Finance).

**NO-Side Advantage:** NO contracts consistently outperform YES at equivalent prices, especially in extreme ranges. Makers show selective positioning toward NO (the less "exciting" side).

**Trade Size → Skill Signal:** Larger trades correlate with positive excess returns. Mann-Whitney U tests confirm makers trade significantly larger across all price deciles.

**Category Variation:** The maker/taker gap varies significantly by market category. Sports markets (90% of volume) show larger spreads; Finance markets attract more sophisticated participants.

**Temporal Patterns:** Returns vary by hour of day (Eastern Time), suggesting different participant composition at different times.

**Improving Calibration:** Cumulative calibration deviation has decreased over time as the platform has matured.

### 3.2 Useful Analytical Patterns

**Core metric — Excess Return:** `excess_return = actual_win_rate - (price / 100)` — used universally across analyses. Positive = profitable edge.

**Taker/Maker Decomposition:** Every trade has two sides. Taker position: `price = yes_price if taker_side='yes' else no_price`, `won = (taker_side == result)`. Maker position: the inverse.

**Counterparty Price Reflection:** When comparing maker vs taker returns on the same chart, the maker's return at price P should be plotted at price 100-P (since if a taker buys YES at 60c, the maker holds NO at 40c).

**Resolved Markets CTE:** Standard pattern for joining trades to outcomes:
```sql
WITH resolved_markets AS (
    SELECT ticker, result
    FROM markets WHERE status = 'finalized' AND result IN ('yes', 'no')
)
SELECT t.*, m.result
FROM trades t INNER JOIN resolved_markets m ON t.ticker = m.ticker
```

**Volume-Weighted Aggregation:** Category-level and group-level statistics should be contract-weighted, not trade-count-weighted, to prevent small trades from distorting averages.

**Contract-Weighted Win Rates:** `SUM(CASE WHEN won THEN count ELSE 0 END) / SUM(count)` — not just `AVG(won)`.

### 3.3 Statistical Methods Used

- **Mann-Whitney U tests** (non-parametric) for trade size comparisons
- **Two-proportion z-tests** for YES/NO asymmetry with contract weighting
- **Welch's t-tests** (unequal variances) for cross-category comparisons
- **Cohen's d** effect sizes for practical significance
- **Pearson + Spearman correlations** for trade-size-to-performance relationships
- **Weighted linear regression** on binned data for noisy large datasets
- **Subsampling** (cap at 1M for correlations, 100K for t-tests) for computational feasibility

---

## 4. Technical Architecture for New Project

### 4.1 Stack

- **Language:** Python 3.12+
- **Package Manager:** uv
- **Data Storage:** Parquet files (chunked, local)
- **Query Engine:** DuckDB (SQL queries directly on Parquet files via glob patterns)
- **Analysis:** pandas, numpy, scipy
- **Visualization:** matplotlib (for Quarto-embedded figures)
- **Reporting:** Quarto (`.qmd` files → HTML reports)
- **Simulation/Backtesting:** Custom Python (event-driven, time-ordered trade replay)
- **API Client:** httpx with tenacity retry/backoff

### 4.2 Project Structure

```
kalshi-research/
├── pyproject.toml
├── Makefile
├── README.md
│
├── src/
│   ├── download/                  # Data acquisition
│   │   ├── client.py              # Kalshi API client (v2, _dollars/_fp fields)
│   │   ├── markets.py             # Market metadata downloader
│   │   ├── trades.py              # Trade history downloader
│   │   ├── events.py              # Events downloader
│   │   ├── series.py              # Series downloader
│   │   └── storage.py             # Chunked Parquet storage
│   │
│   ├── analysis/                  # Analysis modules
│   │   ├── base.py                # Analysis base class
│   │   ├── calibration/           # Market calibration studies
│   │   ├── microstructure/        # Maker/taker, spreads, trade size
│   │   ├── temporal/              # Time-of-day, day-of-week, seasonality
│   │   ├── categories/            # Category-level analysis
│   │   ├── strategies/            # Strategy-specific analysis
│   │   └── predictive/            # Predictive model explorations
│   │
│   ├── simulation/                # Backtesting engine
│   │   ├── engine.py              # Event-driven trade replay
│   │   ├── portfolio.py           # Portfolio/position tracking
│   │   ├── strategies/            # Strategy implementations
│   │   └── metrics.py             # Sharpe, drawdown, PnL, etc.
│   │
│   ├── util/                      # Shared utilities
│   │   ├── categories.py          # Category taxonomy
│   │   ├── queries.py             # Common SQL patterns/CTEs
│   │   └── stats.py               # Statistical test helpers
│   │
│   └── reporting/                 # Quarto report generation
│       ├── templates/             # .qmd templates
│       └── render.py              # Report assembly and rendering
│
├── reports/                       # Generated Quarto reports (HTML output)
│   ├── round_01/
│   ├── round_02/
│   └── ...
│
├── data/                          # Downloaded data (gitignored)
│   ├── markets/
│   ├── trades/
│   ├── events/
│   ├── series/
│   └── candles/                   # Optional, for targeted downloads
│
└── .context/                      # Agent collaboration (gitignored)
```

### 4.3 Data Access Pattern

All analysis uses DuckDB SQL on Parquet with glob patterns:
```python
import duckdb
con = duckdb.connect()
df = con.execute("""
    SELECT t.*, m.result
    FROM 'data/trades/*.parquet' t
    INNER JOIN (
        SELECT ticker, result FROM 'data/markets/*.parquet'
        WHERE status = 'finalized' AND result IN ('yes', 'no')
    ) m ON t.ticker = m.ticker
""").df()
```

This is zero-copy, requires no database server, and handles multi-GB datasets efficiently.

---

## 5. Iterative Research Workflow

### 5.1 Each Research Round

```
┌─────────────────────────────────────────────────┐
│  1. PLAN — Define hypotheses and experiments     │
│     What systematic opportunities might exist?   │
│     What specific tests would confirm/deny them? │
├─────────────────────────────────────────────────┤
│  2. EXECUTE — Spawn parallel analysis agents     │
│     Each agent: writes analysis code, runs it,   │
│     produces figures + CSV + summary text         │
├─────────────────────────────────────────────────┤
│  3. SYNTHESIZE — Aggregate results               │
│     Combine all agent outputs into a Quarto      │
│     report with findings, figures, and next steps │
├─────────────────────────────────────────────────┤
│  4. REVIEW — Human reviews the Quarto report     │
│     Approve/reject/redirect findings             │
│     Identify promising threads to pursue deeper  │
├─────────────────────────────────────────────────┤
│  5. ITERATE — Feed findings into next round      │
│     Refine hypotheses, spawn new experiments     │
│     Graduate promising findings to simulation    │
└─────────────────────────────────────────────────┘
```

### 5.2 Quarto Reports

Each round produces a self-contained Quarto HTML report:
```
reports/round_XX/
├── report.qmd        # Main Quarto document
├── figures/           # PNG/SVG figures from analyses
├── data/              # CSV summaries for tables
└── report.html        # Rendered output
```

The report includes:
- **Executive summary** — key findings in 3-5 bullet points
- **Hypotheses tested** — what we set out to investigate
- **Results** — figures, tables, statistical tests with interpretation
- **Strategy candidates** — any opportunities identified, with estimated edge
- **Next steps** — what to explore in the next round
- **Appendix** — methodology details, code references

### 5.3 Research Roadmap (Suggested Order)

**Round 1 — Landscape & Calibration:**
- Dataset summary statistics (market count, trade count, volume by category, time range)
- Overall calibration curve (win rate vs. price)
- Volume distribution across categories and over time
- Basic maker/taker asymmetry replication

**Round 2 — Systematic Bias Mapping:**
- Longshot bias by category (is it stronger in Sports? Weather? Politics?)
- YES/NO asymmetry decomposition — where is the NO edge strongest?
- Time-of-day effects — are there exploitable intraday patterns?
- Day-of-week and seasonality effects
- Fee structure analysis — do fee differences across series create exploitable distortions?

**Round 3 — Strategy Prototyping:**
- "Always bet NO on longshots" — estimate edge and volume capacity
- Category-specific contrarian strategies
- Time-based entry strategies (if temporal patterns are robust)
- Market-close-proximity strategies (do prices converge more accurately near settlement?)
- Mean reversion in correlated markets (e.g., over/under sports totals)

**Round 4 — Simulation & Backtesting:**
- Build event-driven backtester that replays historical trades
- Implement realistic execution: fees, spread crossing, partial fills
- Run top strategies from Round 3 through backtester
- Track: cumulative P&L, Sharpe ratio, max drawdown, win rate, average edge
- Kelly criterion sizing analysis

**Round 5 — Refinement & Robustness:**
- Out-of-sample testing (train on pre-2025, test on 2025+)
- Strategy decay analysis — does edge diminish over time as market matures?
- Correlation between strategies — can multiple be combined?
- Capacity analysis — at what volume does the edge get arbed away?

**Round 6+ — Predictive Models (if systematic approaches plateau):**
- Weather markets: Can we run a weather model (e.g., GFS/ECMWF data) to generate forecasts and compare against market prices? Backtest what it would have predicted historically.
- Sports markets: Can public statistical models (Elo, win probability models) identify consistent mispricings?
- Economic indicators: Can we use leading indicators or nowcasting models to front-run settlement?
- AI/LLM-based prediction: Can language models process news/context to price events better than the market?

---

## 6. Simulation / Backtesting Design

### 6.1 Core Concept

Replay historical trades in chronological order. At each trade timestamp, the strategy "sees" the current market state (latest price, volume, time to expiry, etc.) and decides whether to enter a position. The simulator tracks:

- **Portfolio state:** Cash balance, open positions by market, realized P&L
- **Position lifecycle:** Entry → hold → settlement (or exit via opposing trade)
- **Execution model:** Strategy places orders at a specified price; execution occurs if a historical trade happened at or better than that price within the time window
- **Fee model:** Kalshi's quadratic fee structure: `fee = count * fee_rate * min(price, 100-price) / 100`

### 6.2 Key Metrics

- Cumulative P&L (absolute and %)
- Sharpe ratio (annualized)
- Max drawdown (peak-to-trough)
- Win rate (% of positions that profit)
- Average edge (mean excess return per contract)
- Kelly optimal fraction
- Profit factor (gross wins / gross losses)
- Volume capacity (how much capital can the strategy deploy?)
- Time-weighted return

### 6.3 Realistic Execution Assumptions

For backtesting to be credible:
- Assume taker execution (crossing the spread) unless the strategy explicitly posts limit orders
- Include Kalshi fees in all P&L calculations
- Account for the fact that our orders would change the market (impact modeling for larger sizes)
- Separate in-sample and out-of-sample periods

---

## 7. Strategy Taxonomy

### 7.1 Systematic / Structural (Priority — No Prediction Required)

These exploit known biases in how prediction markets work:

| Strategy Type | Description | Key Data Needed |
|---------------|-------------|-----------------|
| **Longshot Fade** | Systematically sell overpriced longshots (bet NO on low-probability YES contracts) | Trades, outcomes, calibration curve |
| **Favorite-Longshot Arbitrage** | Buy underpriced favorites where win rate > implied probability | Trades, outcomes |
| **YES/NO Asymmetry** | Exploit the systematic NO-side advantage at certain price levels | Trades, taker_side, outcomes |
| **Maker Mimicry** | Replicate maker positioning patterns (post limit orders on the winning side) | Trades, taker_side, maker behavior analysis |
| **Category Rotation** | Overweight categories where retail participation is highest (largest maker edge) | Trades, categories, outcomes |
| **Time-of-Day** | Trade during hours when the edge is largest | Trades, timestamps, outcomes |
| **Close-Proximity** | Trade markets approaching settlement where prices should converge to fundamentals | Trades, market close_time, outcomes |
| **Correlated Market Spread** | Trade mispricings between correlated markets (e.g., overlapping sports events) | Markets, event structure, trades |

### 7.2 Predictive (Later Rounds)

| Strategy Type | Description | External Data Needed |
|---------------|-------------|---------------------|
| **Weather Forecasting** | Run weather models (GFS/ECMWF/HRRR) against weather markets | Weather model data, historical forecasts |
| **Sports Modeling** | Apply Elo/win-probability models to sports markets | Team statistics, game logs |
| **Economic Nowcasting** | Use leading indicators to predict economic releases | FRED, BLS, economic data feeds |
| **News/Sentiment** | Use LLMs to process news and predict outcomes | News APIs, LLM inference |

---

## 8. Implementation Notes

### 8.1 API Client Best Practices (from existing codebase)

- **Retry with exponential backoff:** 5 attempts, 1-60 second wait, on 429/5xx/timeout/connection errors
- **Cursor-based pagination:** Always paginate to completion; save cursor to file for resumable downloads
- **Chunked storage:** Write 10K records per Parquet file to avoid memory issues and enable partial reads
- **Use `_dollars`/`_fp` fields:** The new API fields use fixed-point string representation. Parse as `Decimal` or `float` carefully.
- **Rate limit awareness:** At 20 req/s basic tier, throttle requests. Use `time.sleep()` or token bucket.

### 8.2 DuckDB Query Patterns

**Glob Parquet reads:**
```sql
SELECT * FROM 'data/trades/*.parquet' WHERE ticker = 'XYZ'
```

**Window functions for time-series:**
```sql
DATE_TRUNC('quarter', created_time) AS period
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
```

**Statistical aggregations:**
```sql
VAR_POP(excess_return) AS variance
SUM(CASE WHEN won THEN count ELSE 0 END)::DOUBLE / SUM(count) AS weighted_win_rate
```

### 8.3 Category Taxonomy

Preserve the existing 568-pattern taxonomy from the original project. It maps event_ticker prefixes (e.g., "NFLGAME", "PRES", "INXSPX") to a 3-level hierarchy. The taxonomy should be extended as new market types appear.

### 8.4 Quarto Integration

Each analysis script should output:
1. A figure (PNG/SVG) saved to `reports/round_XX/figures/`
2. A CSV summary saved to `reports/round_XX/data/`
3. A text summary (key finding in 1-2 sentences)

A Quarto `.qmd` template assembles these into the final report. Quarto can embed Python code cells that reference the analysis outputs, or the report can be purely assembled from pre-generated artifacts.

---

## 9. Open Questions / Decisions

1. **Trade filtering threshold:** The original project only downloaded trades for markets with volume >= 100. Should we lower this or remove the filter entirely? Low-volume markets may have interesting pricing anomalies but also more noise.

2. **Data freshness:** The existing dataset may be stale (the platform has grown dramatically — 2025 saw $23.8B volume, up 1,108% YoY). We should do a full re-download rather than trying to incrementally update the old data.

3. **Scalar markets:** The original research focused on binary (yes/no) markets. Kalshi also has scalar markets (e.g., "How many seats will party X win?"). These have different pricing dynamics and may offer distinct opportunities.

4. **Fee structure complexity:** Kalshi uses different fee types across series (quadratic, flat, quadratic_with_maker_fees). Fee differences may create category-specific edge calculations.

5. **Real-time component:** Eventually, a live trading bot needs WebSocket integration for real-time data. This is a separate concern from historical analysis but should be considered in the architecture.

6. **Multivariate / combo markets:** Kalshi has multivariate event collections. These create potential cross-market arbitrage opportunities.
