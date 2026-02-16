# Simulation Engine Design: Research

## Problem Statement

We need a standardized way to compare the performance of different architectures (ralph_loop, single_agent, attractor_loop, manual_review) that are independently discovering Kalshi trading strategies. The core question: which architecture finds the most profitable strategies?

This requires:
1. A standardized strategy interface that all architectures output
2. A simulation engine that evaluates strategies against historical data
3. Temporal train/validation/test splits to prevent overfitting
4. Realistic modeling of capital constraints, fees, and order execution

## Key Metrics

### P&L (Profit and Loss)

Total money made or lost. On Kalshi, contracts settle at $1.00 (event happened) or $0.00 (didn't happen).

- Buy YES at $0.60, event happens: profit = $0.40 per contract
- Buy YES at $0.60, event doesn't happen: loss = $0.60 per contract
- Net P&L subtracts Kalshi's fee from gross P&L

Primary metric for comparison. A strategy's total P&L is the sum across all trades, net of fees.

### Sharpe Ratio

Risk-adjusted return: how much return per unit of volatility.

```
sharpe = (mean daily P&L / std dev of daily P&L) * sqrt(252)
```

The sqrt(252) annualizes the ratio (252 trading days/year). Sharpe > 1.0 is good, > 2.0 is very good. A strategy making $500 with steady $5/day gains has a higher Sharpe than one making $500 with wild swings. Higher Sharpe = more consistently profitable.

### Max Drawdown

The worst peak-to-trough decline. If a strategy is up $500 at its peak then drops to only up $200, that is a $300 drawdown (60% of peak).

```
running_max = cumulative_pnl.cummax()
drawdown = running_max - cumulative_pnl
max_drawdown = drawdown.max()
```

Matters because even a profitable strategy is unusable if it regularly wipes out 80% of your account before recovering.

## Decision: Independent Strategy Evaluation

**Decision**: Evaluate each strategy independently with its own $1,000 bankroll. Up to 10 strategies per architecture.

**Reasoning**: The question is "which architecture found better trading opportunities?" not "which architecture is the best portfolio manager?" Independent evaluation is simpler, more interpretable, and harder to game.

**Future work**: Portfolio correlation analysis across strategies can happen as a separate layer. This could combine high-performing strategies from different architectures and is a distinct problem from strategy discovery evaluation.

## Temporal Splits and Data Analysis

### Trade Volume Growth

The Kalshi dataset shows explosive growth. Analysis of 173M total trades:

| Period | Trades | % of Dataset |
|--------|--------|-------------|
| Pre-2024 (2.5 years) | 2.9M | 1.7% |
| Jan 2024 -- Aug 2025 (20 months) | 27.1M | 15.7% |
| Sep 2025 -- Feb 2026 (6 months) | 143.2M | 82.7% |
| January 2026 alone | 53.9M | 31.1% |

Contract volume tells the same story: 82% of all contracts traded are from Sep 2025 onward.

Charts saved to `development_docs/trade_volume_by_month.png` and `development_docs/trade_volume_by_quarter.png`.

**Key insight**: The market pre-2024 is fundamentally different from the current market -- different liquidity, different participant mix, different category mix, probably different biases. Strategies discovered from 2022 data may be irrelevant today.

Average contracts per trade also shifted: 93 pre-2024, 236 in 2024-mid 2025, 215 in recent months. November 2024 (US elections) had an anomalous 471 contracts/trade average.

### Decision: Temporal Split Design

**Decision**: Data starts at January 2025. One month validation, one month test.

| Period | Date Range | Who Sees It | Purpose |
|--------|-----------|-------------|---------|
| Train | 2025-01-01 to ~1 month before data end | Agents (full access) | Strategy discovery and backtesting |
| Validation | Train end to ~1 month before data end | Agents (can submit strategies for evaluation) | Self-evaluation, iteration |
| Test | Last month of data | Harness only | True held-out evaluation |

**Reasoning**:
- Three-month splits would consume 67% of the dataset (too much held out)
- Volume is doubling every 2-3 months, so market structure changes fast
- One-month windows provide enough trading activity to be meaningful
- Data before 2025 represents a fundamentally different, much smaller market
- Agents get access to train data and can run simulations on validation data
- Test data is never accessible to agents, preventing overfitting

### Decision: Include Sports Markets

**Decision**: Sports markets are included in the strategy universe.

**Reasoning**: Sports represents 82% of volume and attracts many non-sophisticated retail participants. This is exactly where systematic biases are most likely to exist. Excluding it would eliminate the largest opportunity set.

## Data Infrastructure

### Current Data (as of 2026-02-13)

- Latest trade: 2026-02-06T21:07:12Z (7 days stale)
- Total trades: 173,227,171
- Total contracts: 37.5 billion
- Storage: 17,323 parquet files for trades, 3,023 for markets, 15 for events, 2 for series

### Incremental Download Capability

**Finding**: The current download pipeline (`problems/kalshi/download/`) does NOT support incremental downloads. It uses cursor-based resume for interrupted downloads, but not date-based incremental updates.

**However**: The Kalshi API supports `min_ts`/`max_ts` filters on the trades endpoint. Adding incremental download is a straightforward modification to `download/client.py` -- query the latest trade timestamp from existing parquet, then fetch only trades after that timestamp.

**API authentication**: Environment variables `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH` are configured. Basic tier (20 reads/sec, 10 writes/sec) is automatic with any API key. Advanced tier (30/30) requires a free qualification form. Previous download attempts at 20 req/sec had intermittent failures (likely burst behavior triggering 429 errors), causing the download agent to reduce to 5 req/sec.

### Decision: Enforce Data Splits via Pre-Filtered Data

**Decision**: Use pre-filtered data directories (Option 2 from original discussion).

Create separate data directories with parquet files filtered to only contain rows before the cutoff dates. The scaffold symlinks only train (and optionally validation) data into run workspaces. The harness retains access to the full data for test evaluation.

**Reasoning**: Honor-system enforcement (just documenting cutoffs) is insufficient because agents could accidentally or subtly use future data. Pre-filtered directories are robust against accidental leakage and the preprocessing is straightforward with DuckDB.

## Simulation Engine Design

### Decision: Minute-Tick Simulation (Not Trade-by-Trade)

**Decision**: The simulation advances in 1-minute increments, presenting market state snapshots to the strategy. Not replaying individual trades.

**Reasoning**:

1. **Deployability**: A trade-by-trade model gives the strategy information it would never have in production (seeing individual trades and deciding whether to participate). In the real world, you see market state and decide what to do before knowing what trades will happen next. The minute-tick model mirrors how a real trading bot works: poll market state, make decisions, place orders.

2. **Performance**: 112M trades in the test period vs 130K minute-steps. The minute-tick approach is ~860x fewer iterations and trivially fast in Python.

3. **API alignment**: The Kalshi API provides 1-minute candlesticks natively. A strategy built for the minute-tick interface can run live with minimal changes -- swap the historical data source for live API calls.

### Why Not Sub-Minute?

The Kalshi API provides candlesticks at 1-minute, 1-hour, and 1-day intervals. With WebSocket streaming, sub-second updates are possible. However:

- The strategies being discovered exploit structural biases (e.g., "Elections YES at 60+ cents"), not microsecond patterns
- These strategies hold positions for days/weeks/months
- 1-minute is granular enough and aligns with the finest candlestick the API provides
- Sub-minute simulation can be added later if needed

### Simulation Loop

```
Pre-compute: build per-(market, minute) candlestick table from raw trades.

Initialize:
  available_cash = $1,000
  open_positions = {}
  pending_limit_orders = []
  realized_pnl = 0

For each minute T in the evaluation period:

  1. SETTLE: Check if any markets with open positions have
     finalized (close_time <= T). For each:
     - result = "yes": YES holders receive $1.00/contract
     - result = "no": YES holders receive $0.00
     - Return proceeds to available_cash
     - Update realized_pnl

  2. CHECK FILLS on pending limit orders:
     Look at trades that happened during minute T.
     Apply the calibrated fill model (see Fill Model section).

  3. PRESENT market state to strategy:
     - Candlestick data for active markets
     - Market metadata (category, fee_type, expected settlement)
     - Portfolio state (available_cash, open_positions, pending_orders)

  4. STRATEGY DECIDES: returns a list of orders
     - Market orders: fill immediately at minute T's close price
     - Limit orders: added to pending, checked in future minutes
     - Cancellations: remove pending orders

  5. EXECUTE market orders:
     - Fill at close price of minute T
     - Deduct cost + taker fee from available_cash
     - Add to open_positions

  6. RECORD: daily P&L snapshot, available_cash, open exposure
```

## What a Kalshi Trading Bot Has Access To

Research into the Kalshi API (docs.kalshi.com) reveals the full surface area of information available to a trading bot. The strategy interface should replicate this as closely as possible.

### Real-Time Market Data

| Data Source | What You Get | Method |
|-------------|-------------|--------|
| Order book | Full bid/ask at every price level with depth | REST or WebSocket |
| Trade feed | Every trade as it happens (price, side, volume) | WebSocket `trade` channel |
| Ticker updates | Best bid/ask price changes | WebSocket `ticker` channel |
| Candlesticks | OHLC for bids, asks, AND trade prices + volume + OI | REST endpoint |
| Market metadata | Category, rules, settlement time, strike, fee structure | REST endpoint |
| Market lifecycle | Status changes (open, close, settle) | WebSocket `market_lifecycle_v2` |

### Candlestick Detail

Each candlestick (1-min, 1-hour, or 1-day intervals) contains:
- `yes_bid` OHLC: movement of the best bid price throughout the interval
- `yes_ask` OHLC: movement of the best ask price throughout the interval
- `price` OHLC + mean: actual trade execution prices
- `volume_fp`: contracts traded in the period
- `open_interest_fp`: total contracts outstanding at period end

The bid/ask OHLC represents resting limit order dynamics:
- **Open**: best bid (or ask) at the start of the minute
- **High**: highest the best bid (or ask) reached during the minute
- **Low**: lowest it dropped to
- **Close**: best bid (or ask) at the end of the minute

Open interest is the total number of contracts currently held by participants (not limit orders). When someone buys YES and someone else takes the NO side, one contract is created. Open interest increases when new contracts are created, decreases when positions are closed, and stays the same when positions transfer between participants.

### Portfolio and Account

| Data | Fields |
|------|--------|
| Balance | Available cash, portfolio value |
| Positions | Per-market: contracts held, exposure, realized P&L, fees paid |
| Pending orders | Status, price, remaining quantity, queue position |
| Fills | Fill history with prices, fees, taker/maker flag |
| Settlements | How each market resolved |

### Order Types

| Type | Description |
|------|-------------|
| Limit | Rests on the book at specified price (default) |
| Market | Fills immediately at best available price |
| GTC | Good-till-canceled |
| FOK | Fill-or-kill (fill entirely or reject) |
| IOC | Immediate-or-cancel (fill what you can, cancel rest) |
| Post-only | Rejected if it would cross the spread (guarantees maker fee) |
| Reduce-only | Only reduces existing position |

### Latency

| Method | Latency |
|--------|---------|
| REST API | 50-200ms round trip |
| WebSocket | Sub-50ms (pushed data) |
| FIX Protocol | Less than 1ms (institutional, requires application) |
| Colocated VPS | ~1ms to Kalshi servers |

### Rate Limits

| Tier | Reads/sec | Writes/sec | How to Get |
|------|-----------|------------|------------|
| Basic | 20 | 10 | Automatic with API key |
| Advanced | 30 | 30 | Free qualification form |
| Premier | 100 | 100 | 3.75% monthly volume + review |
| Prime | 400 | 400 | 7.5% monthly volume + review |

### Fee Structure

Kalshi uses a quadratic fee: `fee = contracts * 0.07 * fee_multiplier * price * (1 - price)`

- Taker fee: 7% base rate (peaks at 1.75 cents per contract at 50c)
- Maker fee: 1.75% base rate (~4x cheaper than taker)
- Fee multiplier varies by series (1.0 standard, 0.5 for some reduced-fee series)

The maker/taker fee asymmetry is significant: maker strategies have a structural cost advantage. From Becker's analysis of 72.1M Kalshi trades, makers earn +1.12% excess return on average while takers lose -1.12%.

## Candlestick Data

### What Candlesticks Add vs Trade-Reconstructed Data

| Data Point | From Trades | From Candlestick API |
|-----------|:-----------:|:-------------------:|
| Trade price OHLC | Exact | Yes |
| Trade volume | Exact | Yes |
| Bid OHLC | Rough (from NO-taker trades, only when trades happen) | Every minute, even without trades |
| Ask OHLC | Rough (from YES-taker trades, only when trades happen) | Every minute, even without trades |
| Spread | Only minutes with both-sided trades | Every active minute |
| Open interest | Not available | Yes, per minute |
| Activity during no-trade minutes | Invisible | Visible |

**Decision**: Download candlestick data. The bid/ask information is genuinely valuable for spread estimation, liquidity filtering, and more accurate limit order simulation.

### Candlestick Download Estimate

For non-sports markets from 2025 onward:
- 354,512 markets with trades
- ~13.9M trade-active (market, minute) pairs, estimated ~28-42M total candlestick records
- Storage: 2-4 GB compressed
- Download time: ~5 hours at 20 req/sec

For ALL markets including sports:
- 4.3M markets
- Download time: ~60 hours at 20 req/sec

**Approach**: Download non-sports first (5 hours), start building the simulation, download sports in parallel over a couple of days.

## Historical Order Book Data

Full tick-level order book reconstruction (depth at every price level over time) is NOT available from our historical data and would require a third-party provider.

| Provider | Data | Price |
|----------|------|-------|
| PredictionData.dev | 10B+ OB updates, full L2 reconstruction | $1,100/month (annual) |
| FinFeedAPI | Order books, OHLCV via unified API | Usage-based tiers |
| DeltaBase | Trade data only (not order book) | Free (7 days) / less than $100/month |
| DIY collection | Record WebSocket orderbook_delta going forward | Free (API access) |

**Decision**: Do not purchase order book data now. Use the calibrated fill model (below) to handle the queue depth uncertainty. Revisit if limit order strategies become critical and the fill model proves insufficient.

## Strategy Interface

### Decision: Class-Based Strategy Interface

**Decision**: Strategies are Python classes implementing a standardized interface, not CSV filter rows.

**Reasoning**:
- CSV filters can only express static rules ("buy YES on Elections at 60+ cents")
- Cannot express: dynamic sizing, capital management, trend-following, concentration limits, limit orders, profit-taking
- A class interface is deployable: swap the data source from historical to live API and the same strategy runs in production
- CSV strategies remain supported via a wrapper class that translates filter rows into the class interface

### Strategy Base Class

```python
class Strategy:
    name: str

    def initialize(self, metadata: SimulationMetadata) -> None:
        """Called once before simulation starts.
        metadata: start/end dates, starting capital,
        available market categories."""

    def on_tick(self,
                timestamp: datetime,
                markets: dict[str, MarketSnapshot],
                portfolio: PortfolioState
                ) -> list[Order]:
        """Called each minute. Returns orders to place."""

    def on_fill(self, fill: FillEvent) -> None:
        """A limit order was filled."""

    def on_settlement(self, ticker: str,
                      result: str, pnl: float) -> None:
        """A market in the portfolio settled."""
```

### MarketSnapshot (What the Strategy Sees Each Tick)

```python
@dataclass
class MarketSnapshot:
    ticker: str
    event_ticker: str
    series_ticker: str
    category: str
    fee_type: str
    fee_multiplier: float
    status: str
    expected_settlement: datetime

    # Latest candlestick (1-minute)
    candle: Candle | None  # None if no trades this minute

    # Approximate orderbook (derived from trades or candlestick data)
    approx_yes_bid: float | None
    approx_yes_ask: float | None
    last_trade_price: float | None

    # Volume context
    volume_today: float
    volume_total: float

@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float
    buy_volume: float   # YES-taker volume
    sell_volume: float  # NO-taker volume
    trade_count: int
```

### Order Types

```python
@dataclass
class Order:
    ticker: str
    side: str           # "yes" or "no"
    contracts: int
    order_type: str     # "market" or "limit"
    limit_price: float  # only for limit orders
    action: str         # "buy" or "sell" (sell = close position)
```

### Strategy Complexity Spectrum

Strategies range from trivial CSV-equivalent filters to sophisticated multi-signal systems:

**Level 1 -- Static filter**: Buy YES on Elections at 60+ cents. Uses only category and price.

**Level 2 -- Spread-aware**: Same filter but skip illiquid markets (spread > 5 cents) and use limit orders 1 cent above the bid.

**Level 3 -- Trend-following**: Track price history internally, enter on moving average crossovers.

**Level 4 -- Kelly-sized with limits**: Compute half-Kelly fraction from tracked win rates, cap at 5 open positions and 20% per position.

**Level 5 -- Active order management**: Monitor existing positions for profit-taking, cancel stale limit orders, manage order lifecycle.

Each level uses more of the available information. The simulation engine exposes everything; strategies use what they need.

## Fill Model

### The Problem

When a strategy places a limit order, we need to determine whether and how many contracts would have been filled, given that we don't have historical order book data and therefore don't know the queue depth at any price level.

### Empirical Analysis

Analyzed 117M trades from Nov 2025 -- Feb 2026 (43.5M price episodes). Key findings:

**Queue depth distribution** (observed at trade-through events, where the queue was exhausted):

| Percentile | Queue Depth (contracts) |
|-----------|------------------------|
| p10 | 5 |
| p25 | 16 |
| Median | 69 |
| p75 | 291 |
| p90 | 1,050 |
| p95 | 2,250 |

Follows a log-normal distribution: LogNormal(mu=4.23, sigma=2.09).

**By category**:

| Category | Median Queue Depth |
|----------|-------------------|
| Sports | 100 contracts |
| Politics | 53 contracts |
| Financial | 27 contracts |
| Other | 30 contracts |

**Round number effect**: None. Prices at multiples of 10 cents have ~8% more trading episodes but the same queue depth per episode. No round-number adjustment needed.

**Trade-through rate**: 44% of price episodes trade through, 56% bounce. Stable across price levels.

### The Fill Model

Limit order fills are decomposed into two components:

**1. Certain fills (traded through)**: Any volume that traded below the limit buy price (or above the limit sell price) represents contracts that would have hit our order first. These are 100% filled, capped by order size. No modeling required -- this is pure logic.

Example: Limit buy at 60c. If 50 contracts traded at 58c, we definitely fill 50 contracts (capped by our order size). Those sellers would have taken our 60c offer before selling at 58c.

**2. Uncertain fills (traded at limit price)**: Volume that traded at exactly our limit price. We were somewhere in the queue alongside other limit orders. The fraction we receive is modeled by the calibrated fill rate function.

### Calibrated Fill Rate Function

```python
import math

def uncertain_fill_rate(V_traded, order_size, category='all'):
    """
    Estimate fill rate for uncertain fills.

    Args:
        V_traded: contracts that traded at our limit price
        order_size: our remaining unfilled contracts
        category: 'Sports', 'Financial', 'Politics', 'Other', or 'all'

    Returns:
        float: estimated fraction of order filled (0 to 1)
    """
    base_rate = 0.00722 * math.log(V_traded + 1) ** 1.794
    base_rate = min(base_rate, 1.0)

    category_mult = {
        'Sports': 0.93,
        'Financial': 1.38,
        'Politics': 1.06,
        'Other': 1.44,
        'all': 1.0
    }.get(category, 1.0)

    fill_rate = min(1.0, (V_traded / order_size) * base_rate * category_mult)
    return max(0.0, fill_rate)
```

**Parameters calibrated from data**:
- Base rate uses p25 of queue position distribution (conservative -- assumes we are in the back 75% of the queue)
- Category multipliers derived from median queue depths, dampened 50% toward 1.0 for additional conservatism

**Base rate by volume**:

| V (contracts traded at price) | Base Rate |
|------------------------------|-----------|
| 5 | 2.1% |
| 10 | 3.5% |
| 50 | 8.4% |
| 100 | 11.2% |
| 500 | 19.2% |
| 1,000 | 23.1% |
| 5,000 | 33.7% |

**Model behavior**:
- Small orders fill much more easily than large orders at the same price level
- More volume at the price = higher fill probability
- Sports (deeper queues) = slightly harder to fill
- Financial/Other (shallower queues) = easier to fill

### Example Walkthrough

Limit buy at 60c for 75 contracts. Next minute: 100 contracts trade at 60c, then 50 at 58c.

1. Certain fills: 50 (traded below 60c). Remaining unfilled: 75 - 50 = 25.
2. Uncertain fills: V=100 (at 60c), Q=25 (remaining).
   - base_rate(100) = 0.00722 * ln(101)^1.794 = ~12.3%
   - fill_rate = min(1.0, (100/25) * 0.123) = 49%
   - Fills: 25 * 0.49 = ~12 contracts
3. Total: 50 + 12 = 62 contracts filled out of 75.

### Fees

- Market orders: taker fee (`0.07 * fee_multiplier * price * (1 - price)`)
- Limit order fills: maker fee (`0.0175 * fee_multiplier * price * (1 - price)`)
- Maker fee is ~4x cheaper, correctly rewarding limit order strategies for providing liquidity

### Relevant Research

Key references from the queue depth modeling research:

- **Avellaneda & Stoikov (2008)**: Foundational market-making framework with Poisson fill model. Fill intensity decays exponentially with distance from mid-price.
- **Moallemi & Yuan (2016)**: Queue position valuation. Decomposes value into static (spread vs adverse selection) and dynamic (optionality) components.
- **Cont & Kukanov (2013)**: Optimal limit/market order split as convex optimization.
- **hftbacktest**: Open-source Rust/Python framework with probabilistic queue models (linear, power, logarithmic). Gold standard for limit order backtesting with L2 data.
- **Becker**: Analysis of 72.1M Kalshi trades showing makers earn +1.12% excess return vs takers losing -1.12%.

Commercial backtesting platforms (Backtrader, Zipline, QuantConnect) default to naive fill-on-touch or simple volume-capped models. Our calibrated model is significantly more realistic, especially for prediction markets with thin order books.

### Known Limitations

1. **No queue priority modeling**: We assume a random queue position (actually worse -- p25). Real priority depends on order timing.
2. **No adverse selection**: The model does not account for the fact that limit fills often happen when the price is moving against you.
3. **No intra-minute timing**: Orders placed at the tick boundary are assumed to be in the book for the entire next minute.
4. **Category multipliers are coarse**: Only 4 categories. Could be refined with more granular segmentation.
5. **Stationarity assumption**: The model uses recent data (Nov 2025 -- Feb 2026) for calibration. Queue dynamics may change as the market grows.

## Position Sizing

### Kelly Criterion

The Kelly criterion determines the optimal fraction of bankroll to bet for maximum long-term growth:

```
f* = (p(b + 1) - 1) / b
```

Where:
- f* = optimal fraction of bankroll to bet
- p = estimated probability of winning (from historical win rate)
- b = net payout ratio = (1 - price) / price for a YES bet

Example: YES at 60c, strategy wins 70% of the time.
b = 0.40/0.60 = 0.667. f* = (0.70 * 1.667 - 1) / 0.667 = 0.25 (bet 25% of bankroll).

**Fractional Kelly**: Full Kelly assumes perfect knowledge of win probability, which we don't have. Half-Kelly (f*/2) gives 75% of the growth rate with dramatically less variance. Quarter-Kelly is very conservative.

### Decision: Strategy Controls Position Sizing

**Decision**: Position sizing is part of the strategy class, not the evaluation engine.

**Reasoning**: Sizing is half the game in practice. A strategy that finds a good edge but sizes incorrectly will underperform. The class interface naturally supports dynamic sizing -- the strategy sees its portfolio state and decides how many contracts to buy. This tests both discovery and conviction.

The simulation engine enforces hard constraints: you cannot spend more than `available_cash`, and capital is locked until settlement.

## Resolved Questions

1. **Candlestick download**: Download candlestick data first, before building the simulation. This will be done in a separate cloud instance. Non-sports markets first (~5 hours), sports in parallel over a couple of days.
2. **Incremental data update**: Need to modify `download/client.py` to support date-based incremental downloads before defining exact split dates.
3. **Strategy sandboxing**: How to prevent strategy classes from accessing future data or the filesystem? Docker isolation handles security, but we also need to ensure the simulation engine only feeds data up to the current tick.
4. **Backward compatibility**: Not needed. The `strategies.csv` format was a prototype from the first manual review run. No architectures have been run against it at scale. The class-based strategy interface replaces it entirely. The CSV format and related scaffolding (`results/strategies.csv` header in scaffold, references in arch.yaml files) can be removed.
5. **Market filtering**: Strategies filter internally within `on_tick`. The simulation presents all active markets each tick. This is simpler and avoids adding a subscription mechanism to the interface. If performance becomes an issue (unlikely at 130K ticks), the strategy can declare interest in specific categories in `initialize` and the engine can optimize, but this is a future optimization, not a design requirement.
6. **Limit order lifetime**: Managed by the strategy, not the engine. Limit orders persist until filled, the market settles, or the strategy explicitly cancels them via a `CancelOrder` in its `on_tick` return. If a strategy wants auto-cancel after N minutes, it tracks order timestamps internally and cancels them. This keeps the engine simple and gives strategies full control over their order lifecycle.
