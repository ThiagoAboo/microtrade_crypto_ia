# PHASE 2 — MARKET DATA ENGINE

Implemente a Fase 2 — Market Data Engine.

Objetivos:
- Binance websocket collector
- reconnect logic
- tick normalization
- orderbook normalization
- multi-symbol support
- Redis Streams publishing
- persistência em ClickHouse
- snapshots reduzidos L2

Requisitos:
- baixo consumo de memória
- async-first
- tolerante a falhas
- reconnect automático
- rate-limit awareness
- logging estruturado

Restrições:
- máximo 5 ativos
- salvar apenas top 10~20 níveis do L2

Obrigatório:
- testes básicos
- retries
- healthcheck
- backpressure protection

Antes de implementar:
explique o plano arquitetural.
aguarde a minha decisão.