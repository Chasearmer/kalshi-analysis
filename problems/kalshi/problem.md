# Kalshi Trading Strategy Discovery

## Goal

Discover profitable, automatable trading strategies for Kalshi prediction markets that exploit structural or systematic biases rather than requiring sophisticated prediction.

The output should be a ranked list of simulated and stress-tested investment strategies with realistic P&L, Sharpe ratio, max drawdown, win rate, and capacity estimates.

## Dataset

All data is in `data/` as Parquet files, queryable via DuckDB glob patterns:

| Directory | Contents | Records |
|-----------|----------|---------|
| `data/trades/*.parquet` | Every historical trade | ~100M+ |
| `data/markets/*.parquet` | Market metadata (ticker, result, status, timestamps, volume) | ~615K+ |
| `data/events/*.parquet` | Event metadata (category, series_ticker) | ~tens of thousands |
| `data/series/*.parquet` | Series metadata (fee_type, fee_multiplier, category) | ~hundreds |

**Time range:** 2021-06-30 to 2026-02-06 (~1,680 calendar days)
**Total volume:** ~36.6 billion contracts
**Category split:** ~82% Sports, ~18% other (Elections, Financials, Economics, Weather, etc.)

### Key Fields

**Markets:** `ticker`, `event_ticker`, `series_ticker`, `status` (finalized = settled), `result` (yes/no), `volume_fp`, `close_time`, `created_time`

**Trades:** `trade_id`, `ticker`, `yes_price_dollars` / `no_price_dollars` (0.00-1.00 scale), `count_fp` (contracts), `taker_side` (yes/no), `created_time`

**Events:** `event_ticker`, `category`, `series_ticker`

**Series:** `ticker`, `fee_type` (quadratic/flat/quadratic_with_maker_fees), `fee_multiplier`, `category`

### Data Access Pattern

```python
import duckdb
con = duckdb.connect()
df = con.execute("""
    WITH resolved_markets AS (
        SELECT ticker, result
        FROM 'data/markets/*.parquet'
        WHERE status = 'finalized' AND result IN ('yes', 'no')
    )
    SELECT t.*, m.result
    FROM 'data/trades/*.parquet' t
    INNER JOIN resolved_markets m ON t.ticker = m.ticker
""").df()
```

### Fee Model

Kalshi uses a quadratic fee structure: `fee = contracts * base_rate * fee_multiplier * price * (1 - price)`
where `base_rate = 0.07`, `price` is in 0-1 scale, and `fee_multiplier` varies by series.

## Known Phenomena (from prior research)

- **Longshot Bias:** YES contracts at low prices have negative expected value. Win rates are systematically below implied probabilities at price extremes.
- **Maker-Taker Asymmetry:** Makers outperform takers by ~2-4pp on average. Larger gap in retail-dominated categories (Sports).
- **NO-Side Advantage:** NO contracts outperform YES at equivalent prices, especially at extremes.
- **Category Variation:** Edge varies significantly by market category.
- **Temporal Patterns:** Returns vary by hour of day and day of week.

## Core Metrics

- **Excess Return:** `actual_win_rate - implied_probability` (positive = profitable edge)
- **Contract-Weighted Win Rate:** `SUM(won * contracts) / SUM(contracts)` (not simple average)
- **Sharpe Ratio:** Annualized risk-adjusted return
- **Max Drawdown:** Largest peak-to-trough decline
- **Profit Factor:** Gross wins / gross losses

## Expected Output

A `strategies.csv` file with columns:
- `strategy_name` — Human-readable name
- `taker_side` — "yes" or "no"
- `category` — Market category filter (or "*" for all)
- `fee_type` — Fee type filter (or "*" for all)
- `time_bucket` — Time filter (or "*" for all)
- `price_min` — Minimum price in cents (0-100)
- `price_max` — Maximum price in cents (0-100)
- `confidence` — "high", "medium", or "low"
- `rationale` — Why this strategy works

## Technical Stack

- Python 3.12+, uv package manager
- DuckDB on Parquet (no database server)
- pandas, numpy, scipy for analysis
- matplotlib for visualization

## Scaffold

A `queries.py` file is available with reusable DuckDB SQL builders (resolved markets CTE, trade outcomes decomposition, category joins, fee type joins). Use it or build your own.
