# PHASE 3 — FEATURE ENGINE

Implemente a Fase 3 — Feature Engine.

Objetivos:
- order flow imbalance
- microprice
- queue pressure
- liquidity shifts
- spread dynamics
- volatility score

Requisitos:
- processamento incremental
- baixa latência
- streaming contínuo
- Redis Streams integration
- persistência opcional em ClickHouse

Evitar:
- pandas pesado realtime
- processamento bloqueante
- uso excessivo de RAM

Obrigatório:
- testes unitários
- benchmarks básicos
- logging estruturado

Antes de implementar:
explique a arquitetura da pipeline de features.
aguarde a minha decisão.