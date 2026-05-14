# Redis Streams Topology

## Objetivo

Substituir Kafka por Redis Streams para:
- menor consumo
- menor complexidade
- menor overhead

## Streams

### market:ticks

Responsável por:
- trades
- agressões
- fluxo

---

### market:orderbook

Responsável por:
- snapshots L2
- spreads
- liquidity changes

---

### features:updates

Responsável por:
- features calculadas
- embeddings
- sinais quantitativos

---

### signals:generated

Responsável por:
- sinais dos modelos

---

### risk:approved

Responsável por:
- aprovações de risco

---

### orders:execution

Responsável por:
- envio de ordens

---

### positions:updates

Responsável por:
- posições abertas
- pnl
- updates

---

### system:alerts

Responsável por:
- drift
- kill switch
- erros

## Consumer Groups

Cada módulo deverá possuir:
- consumer group dedicado
- retry policy
- idempotência

## Retenção

Streams curtos:
- memória controlada
- trimming automático

## Estratégia

Redis será utilizado para:
- eventos internos
- filas leves
- sincronização
- baixo overhead
