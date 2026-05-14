# DIRECTORY_MAP.md

## Repository Structure

```text
src/
  core/
  market_data/
  features/
  ml/
  risk/
  execution/
  replay/
  storage/
  monitoring/
  dashboard/
  api/

config/
tests/
docker/
scripts/
docs/

```

## Module Responsibilities

### core/
Shared contracts:
- events
- models
- enums
- config loaders

### market_data/
Responsible for:
- websocket ingestion
- normalization
- parsing
- market event publishing

### features/
Responsible for:
- orderflow metrics
- microprice
- queue pressure
- liquidity metrics

### ml/
Responsible for:
- inference
- ensemble logic
- confidence scoring

### risk/
Responsible for:
- exposure
- limits
- sizing
- kill switch

### execution/
Responsible for:
- order handling
- routing
- reconciliation
- retries

### replay/
Responsible for:
- deterministic replay
- accelerated replay
- debugging

### storage/
Responsible for:
- ClickHouse
- Redis
- persistence

### monitoring/
Responsible for:
- metrics
- alerts
- latency tracking

### dashboard/
Responsible for:
- Streamlit dashboards

### api/
Responsible for:
- FastAPI endpoints
- operational APIs
