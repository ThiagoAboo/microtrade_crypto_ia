# AGENTS.md

## Objective

This repository contains a local-first quantitative crypto micro-trading platform.

Primary goals:
- stability
- controlled risk
- low overhead
- execution quality
- probabilistic edge exploitation

## Architecture Rules

Mandatory:
- modular monolith
- event-driven internal architecture
- Redis Streams
- ClickHouse
- Docker Compose
- strong typing
- async-first architecture

Forbidden:
- distributed microservices
- Kubernetes
- heavy transformers
- RL-heavy systems
- Kafka
- hardcoded secrets
- direct model-to-exchange execution

## Hardware Constraints

Target machine:
- Intel i7-1165G7
- 8GB RAM
- Intel Iris Xe

All implementations MUST optimize:
- RAM usage
- CPU efficiency
- low disk usage

## ML Rules

Primary models:
- LightGBM
- XGBoost

Do NOT prioritize:
- massive transformers
- GPU-first pipelines
- massive RL systems

## Trading Rules

Maximum simultaneous positions:
5

Maximum daily risk:
2%

Maximum drawdown:
10%

## Mandatory Systems

- Risk Engine
- Replay Engine
- Paper Trading
- Logging
- State Recovery
- Drift Detection

## Coding Principles

- simple > clever
- deterministic > magical
- observable > opaque
- stable > experimental
- modular > monolithic chaos

## Final Objective

Build a sustainable quantitative workstation optimized for:
- local execution
- robustness
- iterative evolution
- operational efficiency
