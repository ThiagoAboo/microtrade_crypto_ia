# Production Engineering Specification

## Arquitetura Final Aprovada

### Tipo de Arquitetura
Modular Monolith Event-Driven

### Objetivo
Sistema de micro-trading quantitativo para criptomoedas com:
- microestrutura
- order flow
- baixo drawdown
- baixa latência
- execução local

## Stack Final

| Camada | Tecnologia |
|---|---|
| Collector | Rust |
| Core | Python |
| ML | LightGBM/XGBoost |
| Streaming | Redis Streams |
| DB | ClickHouse |
| API | FastAPI |
| Dashboard | Streamlit |
| Deployment | Docker Compose |

## Restrições do Ambiente

### Hardware
- 8GB RAM
- CPU-centric
- sem GPU dedicada

### Consequências Arquiteturais
- evitar transformers pesados
- evitar RL pesado
- evitar Kafka
- evitar microservices distribuídos

## Estratégia Operacional

### Ativos
Máximo 5 simultâneos

### Mercado
Crypto perpetual futures inicialmente

### Alavancagem
Não utilizar

### Overnight
Permitido

### Timeframe
1m como núcleo operacional

## Estratégia Quantitativa

Baseada em:
- order flow imbalance
- liquidity shifts
- microprice
- queue pressure
- signed trade flow

Indicadores clássicos NÃO serão o núcleo da estratégia.

## Persistência

Persistir:
- trades
- snapshots reduzidos do L2
- features
- ordens
- fills
- pnl

Não persistir:
- full depth contínuo

## L2 Policy

Salvar:
- top 10~20 níveis do orderbook

## Retenção

Máximo:
30 dias

## Latência Alvo

Excelente:
<100ms

Aceitável:
<250ms

## Trading Modes

### Paper Trading
Obrigatório

### Real Trading
Obrigatório

Mesmo pipeline para ambos.

## Risk Engine

### Máximo risco diário
2%

### Máximo posições simultâneas
5

### Drawdown máximo
10%

## Conclusão

Arquitetura otimizada para:
- workstation local
- eficiência
- baixo overhead
- desenvolvimento iterativo
- robustez operacional
