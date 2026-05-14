# REPOSITORY_BOOTSTRAP.md

## Initial Stack

| Component | Technology |
|---|---|
| Core | Python |
| Collector | Rust |
| Streaming | Redis Streams |
| Database | ClickHouse |
| API | FastAPI |
| Dashboard | Streamlit |
| Deployment | Docker Compose |

## Initial Services

- redis
- clickhouse
- api
- collector
- trading-engine
- dashboard

## First Milestone

Goal:
- receive Binance data
- persist ticks
- calculate features
- generate mock signals
- validate risk
- paper trade

## Constraints

System MUST run comfortably on:
- 8GB RAM
- CPU-first environment
