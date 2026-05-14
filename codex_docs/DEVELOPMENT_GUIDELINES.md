# DEVELOPMENT_GUIDELINES.md

## Development Philosophy

Prioritize:
- correctness
- observability
- modularity
- determinism

Avoid:
- premature optimization
- overengineering
- unnecessary abstractions

## Code Quality

Mandatory:
- typing
- tests
- logging
- retries
- error handling

## Async Policy

Prefer async for:
- websocket
- execution
- replay
- ingestion

## Configuration

All configs must be externalized.

Use:
- YAML
- ENV

Never:
- hardcode secrets

## Testing

Mandatory:
- unit tests
- integration tests
- replay validation

## Logging

Every critical operation must be logged.

Especially:
- trades
- risk
- fills
- errors
- reconnects
