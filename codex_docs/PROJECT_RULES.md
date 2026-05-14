# PROJECT_RULES.md

## Core Invariants

These rules MUST NEVER be violated.

## Risk

- Risk Engine has absolute priority.
- No trade can bypass Risk Engine.
- No model can directly place orders.
- Kill switch must always remain active.

## Architecture

- Modular monolith only.
- Internal event-driven communication.
- Redis Streams for lightweight messaging.
- Avoid unnecessary infrastructure complexity.

## Persistence

Persist:
- trades
- reduced orderbook snapshots
- features
- orders
- fills
- pnl

Do NOT persist:
- full depth indefinitely

## Performance

Target latency:
- ideal <100ms
- acceptable <250ms

## ML

Primary edge:
- microstructure
- orderflow
- execution quality
- risk control

NOT:
- price prediction fantasies

## Execution

Execution engine must:
- support partial fills
- support maker/taker hybrid
- support retries
- support reconciliation

## Reliability

Mandatory:
- retry policies
- recovery logic
- state reconstruction
- replay determinism
- logging
- observability

## Operational Scope

Project is:
- single-user
- local-first
- workstation-oriented
- non-distributed
