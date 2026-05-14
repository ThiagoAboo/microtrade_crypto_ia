# System Contracts & Operational Specs

## Event Pattern

Todos eventos seguem:

```json
{
  "event_id": "uuid",
  "event_type": "EVENT",
  "timestamp": 1740000000,
  "source": "service",
  "payload": {}
}
```

## Eventos Principais

### MARKET_TICK
### ORDERBOOK_UPDATE
### FEATURE_UPDATED
### SIGNAL_GENERATED
### RISK_APPROVED
### ORDER_SENT
### ORDER_FILLED
### POSITION_OPENED
### POSITION_CLOSED
### KILL_SWITCH_TRIGGERED

## Order Lifecycle

market_event
→ feature_generation
→ signal_generation
→ risk_validation
→ execution
→ fill
→ position_management
→ reconciliation

## Risk Policy

### Hard Stop Diário
2%

### Warning Threshold
1%

### Max Positions
5

## Execution Rules

### Timeout
3~10 segundos

### Partial Fills
Obrigatórios

### Smart Execution
maker + taker híbrido

## Logging

Categorias:
- TRADE
- RISK
- MODEL
- EXECUTION
- ERROR

## Recovery

O sistema deve:
- reconstruir estado
- reconciliar posições
- reprocessar streams
- persistir snapshots
