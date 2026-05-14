# CODING_STANDARDS.md

## Python

Mandatory:
- type hints
- dataclasses or pydantic
- async where appropriate
- structured logging

## Rust

Use Rust only for:
- latency-sensitive modules
- collectors
- execution-critical paths

## Naming

Prefer:
- explicit names
- deterministic semantics

Avoid:
- abbreviations
- hidden side effects

## Error Handling

Mandatory:
- retries
- explicit exceptions
- recovery paths

## Observability

All modules must expose:
- metrics
- logs
- health states

## Security

Never commit:
- API keys
- secrets
- credentials
