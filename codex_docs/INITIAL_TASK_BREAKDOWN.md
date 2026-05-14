# INITIAL_TASK_BREAKDOWN.md

## Phase 1 — Repository Bootstrap

Tasks:
- create repository structure
- configure Docker Compose
- configure environments
- configure logging
- configure configs

---

## Phase 2 — Infrastructure

Tasks:
- Redis setup
- ClickHouse setup
- persistence adapters
- event bus abstraction

---

## Phase 3 — Market Data

Tasks:
- Binance websocket collector
- tick normalization
- orderbook snapshots
- reconnect logic

---

## Phase 4 — Feature Engine

Tasks:
- order flow imbalance
- microprice
- liquidity shifts
- volatility metrics

---

## Phase 5 — ML Engine

Tasks:
- LightGBM inference
- XGBoost inference
- ensemble logic
- confidence thresholds

---

## Phase 6 — Risk Engine

Tasks:
- position sizing
- exposure limits
- daily drawdown protection
- kill switch

---

## Phase 7 — Execution Engine

Tasks:
- order routing
- maker/taker logic
- retries
- partial fills
- reconciliation

---

## Phase 8 — Paper Trading

Tasks:
- simulated fills
- slippage simulation
- latency simulation

---

## Phase 9 — Replay Engine

Tasks:
- deterministic replay
- accelerated replay
- step replay

---

## Phase 10 — Dashboard

Tasks:
- pnl dashboard
- latency dashboard
- risk dashboard
- drift dashboard
