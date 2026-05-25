# MICROTRADE_CRYPTO_IA Monolith Implementation Plan

## Goal Description

The goal of the `MICROTRADE_CRYPTO_IA` project is to implement a local-first, CPU-optimized, modular monolith crypto micro-trading system. The system runs on a single-node workstation with hardware constraints (8GB RAM, Intel i7 CPU) and relies on Redis Streams for internal event routing and ClickHouse for persistence.

This plan details the implementation phases of the project. Phase 1 (Core Infrastructure) is already fully implemented, tested, and operational. We outline the detailed strategy for the remaining phases (Phase 2 to 10) to deliver a robust, type-safe, and high-performance workstation trading bot.

---

## User Review Required

> [!IMPORTANT]
> The next step is to initiate Phase 2 (Market Data Engine). The code structure, schemas, and queues for the Market Data Engine are already partially laid out in `src/market_data/`. We will integrate and build upon this structure.
>
> **Core Architecture Constraints to Keep in Mind:**
> - System optimized for 8GB RAM and CPU execution.
> - High reliability: automated reconnects with exponential backoff and jitter, persistent queues with backpressure, and validation of sequence numbers.
> - No complex cloud infrastructure (e.g. Kubernetes, Kafka).

---

## Open Questions

> [!NOTE]
> 1. Do we want to start Binance Web Socket in production mode with real streams or should we continue using mocks/simulations for safety until Paper Trading is enabled in Phase 6?
> 2. Are there specific crypto currency pairs other than `BTCUSDT` that you would like to ingest during the test execution of Phase 2?

---

## Proposed Changes and Roadmap

We list the implementation breakdown by phases. Since Phase 1 is done, we focus on what each future phase will implement and modify.

### Phase 1: Core Infrastructure [COMPLETED]

Provides the basic bootstrap, directory mapping, config parsing (YAML/Env), structured logging, healthchecks, and the abstraction for Redis Streams event bus.

- **Status**: Completed & Verified. 23 unit tests pass successfully.
- **Key Files**:
  - [config.py](file:///d:/Projetos/microtrade_crypto_ia/src/core/config.py) (handles environment & YAML overrides)
  - [logging.py](file:///d:/Projetos/microtrade_crypto_ia/src/core/logging.py) (structured JSON logs)
  - [event_bus.py](file:///d:/Projetos/microtrade_crypto_ia/src/core/event_bus.py) (event bus protocol)
  - [redis_event_bus.py](file:///d:/Projetos/microtrade_crypto_ia/src/storage/redis_event_bus.py) (Redis Streams adapter with poison message DLQ handling)
  - [clickhouse.py](file:///d:/Projetos/microtrade_crypto_ia/src/storage/clickhouse.py) (ClickHouse connection client)
  - [main.py](file:///d:/Projetos/microtrade_crypto_ia/src/api/main.py) (health check API endpoints)

---

### Phase 2: Market Data Engine

Builds the Binance websocket and HTTP snapshot clients, normalizing raw JSON payloads into tick trades and L2 depth updates, storing snapshots under 10-20 levels, and managing websocket reconnections.

- **Actions**:
  - Integrate websocket collector loop in [engine.py](file:///d:/Projetos/microtrade_crypto_ia/src/market_data/engine.py).
  - Activate data writers to ClickHouse (`ticks` and `orderbook_snapshots` tables).
  - Verify backpressure queues and queue lag monitoring.
- **Files to Verify/Polish**:
  - [binance_client.py](file:///d:/Projetos/microtrade_crypto_ia/src/market_data/binance_client.py)
  - [orderbook.py](file:///d:/Projetos/microtrade_crypto_ia/src/market_data/orderbook.py)
  - [market_data_writer.py](file:///d:/Projetos/microtrade_crypto_ia/src/storage/market_data_writer.py)

---

### Phase 3: Feature Engine

Implements calculation of quantitative features from raw market feeds (trades and orderbooks).

- **Actions**:
  - Create `src/features/engine.py` to listen to `market:ticks` and `market:orderbook` streams.
  - Implement real-time calculations:
    - **Order Flow Imbalance (OFI)**: tracking supply/demand changes.
    - **Microprice**: weighted mid-price based on bid/ask volume.
    - **Queue Pressure**: imbalance of limit orders at top depth levels.
    - **Liquidity Shift**: shifts in volume distribution.
  - Publish features to the `features:updates` stream and persist them to the ClickHouse `features` table.

---

### Phase 4: Machine Learning Engine

Loads ML models and handles real-time inference on CPU.

- **Actions**:
  - Support LightGBM and XGBoost model loading (using static pickle or booster formats).
  - Create `src/ml/inference.py` subscribing to `features:updates`.
  - Implement confidence scoring and ensemble logic.
  - Publish signals with expected price movements to `signals:generated` stream and ClickHouse `signals` table.

---

### Phase 5: Risk Engine

Acts as the absolute gateway for orders, validating sizing and exposure constraints before execution.

- **Actions**:
  - Create `src/risk/engine.py` subscribing to `signals:generated`.
  - Validate against strict rules:
    - Maximum daily drawdown limit: 2% of equity.
    - Maximum simultaneous positions: 5.
    - Max position size/exposure rules.
    - Active Kill Switch listening to system alerts or manual intervention.
  - Publish risk-approved orders to `risk:approved` stream.

---

### Phase 6: Execution Engine

Manages sending orders to the exchange and handles lifecycle changes.

- **Actions**:
  - Create `src/execution/engine.py` subscribing to `risk:approved`.
  - Support Maker/Taker hybrid logic, order routing, partial fills, and retries.
  - Listen for fill events, reconciling them with open positions, publishing updates to `positions:updates` and storing in ClickHouse `orders`/`fills`.

---

### Phase 7: Paper Trading

High-fidelity simulator running on the live pipeline.

- **Actions**:
  - Create `src/execution/paper.py` intercepting orders when mode is configured to `paper_trading`.
  - Simulate fill probability, slippage models, and latency penalties using orderbook snapshots.

---

### Phase 8: Replay Engine

Enables historical backtesting and system verification using stored database ticks.

- **Actions**:
  - Create `src/replay/engine.py` to pull ticks and orderbook snapshots from ClickHouse.
  - Replay events in: `realtime` speed, `accelerated` mode, or `step` event-by-event mode.
  - Inject replayed data back into the modular monolith to validate model drift, risk control, and execution logic.

---

### Phase 9: Monitoring & Dashboards

Streamlit dashboard showing PnL, performance metrics, and system status.

- **Actions**:
  - Create dashboard pages in `src/dashboard/` for:
    - PnL and active positions tracker.
    - Risk engine limits & warning indicators.
    - Execution latency tracking.
    - Model drift analysis (comparing prediction confidence vs. realized returns).

---

### Phase 10: Performance Optimization

Rust-based extensions and final optimizations.

- **Actions**:
  - Move performance-critical collectors or orderbook parsing logic to Rust where profiling shows CPU/RAM bottlenecks.
  - Perform stress testing under high volatility feeds (e.g. >10,000 updates/sec).

---

## Verification Plan

### Phase 1 & 2 Automated Tests
- Run `python -m pytest` to execute existing test suite (23 passing tests).
- Implement new integration tests in `tests/integration/` simulating full cycle of Market Data receiving, normalization, queues, stream publishing, and ClickHouse persistence.
