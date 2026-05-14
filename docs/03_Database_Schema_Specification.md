# Database Schema Specification

## Banco
ClickHouse

## Estratégia

Otimizado para:
- baixo espaço
- replay rápido
- queries analíticas
- retenção curta

## Tabelas

### ticks

Campos:
- timestamp
- symbol
- price
- quantity
- side
- trade_id

TTL:
30 dias

---

### orderbook_snapshots

Campos:
- timestamp
- symbol
- best_bid
- best_ask
- bid_levels_json
- ask_levels_json
- spread

Guardar apenas top 10~20 níveis.

TTL:
15 dias

---

### features

Campos:
- timestamp
- symbol
- order_flow_imbalance
- microprice
- volatility_score
- liquidity_shift
- queue_pressure

TTL:
30 dias

---

### signals

Campos:
- timestamp
- symbol
- direction
- confidence
- expected_move

TTL:
90 dias

---

### orders

Campos:
- order_id
- symbol
- side
- quantity
- price
- status
- exchange_order_id

TTL:
180 dias

---

### fills

Campos:
- fill_id
- order_id
- symbol
- fill_price
- fill_quantity
- fees

TTL:
180 dias

---

### positions

Campos:
- position_id
- symbol
- entry_price
- exit_price
- pnl
- duration

TTL:
365 dias

## Compressão

Utilizar:
- ZSTD

## Particionamento

Particionar por:
- dia
- símbolo

## Índices

Prioridade:
- timestamp
- symbol
