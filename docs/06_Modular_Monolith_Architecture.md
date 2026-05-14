# Modular Monolith Architecture

## Arquitetura Oficial

Modular Monolith Event-Driven

## Motivação

Ideal para:
- workstation local
- baixo overhead
- debugging simples
- baixa RAM
- evolução futura

## Estrutura

```text
src/
  core/
  market_data/
  features/
  ml/
  risk/
  execution/
  replay/
  dashboard/
  storage/
  monitoring/
```

## Módulos

### market_data
- websocket
- parsing
- normalization

### features
- orderflow
- microprice
- liquidity

### ml
- inferência
- ensemble
- confidence

### risk
- exposure
- sizing
- limits

### execution
- smart routing
- order handling
- fills

### replay
- tick replay
- deterministic simulation

### storage
- clickhouse
- redis

## Comunicação

Interna via:
- Redis Streams
- eventos

## Regras

### Obrigatório
- desacoplamento
- tipagem
- retry
- observabilidade

### Proibido
- lógica monolítica gigante
- dependências cíclicas
- hardcoded configs
