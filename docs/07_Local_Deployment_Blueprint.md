# Local Deployment Blueprint

## Objetivo

Executar o sistema integralmente local.

## Orquestração

Docker Compose

## Containers

### redis
Streams/event bus

### clickhouse
Persistência

### api
FastAPI

### dashboard
Streamlit

### collector
Rust collector

### trading-engine
Core quantitativo

## Estratégia de Recursos

Otimizado para:
- 8GB RAM
- baixo consumo
- baixo overhead

## Limites

### Redis
máx memória limitada

### ClickHouse
retenção curta

### replay
processamento controlado

## Recomendações

### Operação
- máximo 5 ativos
- máximo 30 dias de retenção

### Modelos
- LightGBM
- XGBoost

Evitar:
- transformers pesados
- RL pesado
