# Replay Engine Specification

## Objetivo

Permitir:
- replay determinístico
- backtesting realista
- validação de modelos
- debugging

## Fontes

Replay deverá suportar:
- ticks
- orderbook
- features
- ordens
- fills

## Modos

### realtime
Velocidade real

### accelerated
Velocidade acelerada

### step
Evento por evento

### paused
Pausado

## Controles

Obrigatórios:
- pause
- resume
- speed multiplier
- jump timestamp

## Requisitos

### Determinístico
Mesmo input → mesmo resultado

### Reprodutível
Replay consistente

## Simulações

Incluir:
- slippage
- spread
- partial fills
- latency

## Objetivo Principal

Validar:
- estratégia
- execução
- risco
- estabilidade
