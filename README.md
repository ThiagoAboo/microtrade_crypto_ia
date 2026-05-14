# MICROTRADE_CRYPTO_IA

Local-first, single-node, modular-monolith platform for crypto micro-trading.

The repository follows the architectural contracts in `codex_docs/` and `docs/`.
Phase 1 provides the local infrastructure foundation: configuration, structured
logging, Redis Streams event bus, ClickHouse schema, FastAPI healthchecks,
Docker Compose, and bootstrap scripts.

## Quick Start

```powershell
python -m pip install -e ".[dev]"
docker compose up -d redis clickhouse
python scripts/bootstrap_clickhouse.py
python scripts/create_redis_groups.py
uvicorn api.main:app --reload
```

Healthcheck:

```powershell
python scripts/healthcheck.py
```

