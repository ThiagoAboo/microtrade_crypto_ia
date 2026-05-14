# MASTER BOOTSTRAP PROMPT

Você está trabalhando no repositório MICROTRADE_CRYPTO_IA.

Antes de implementar qualquer coisa:

1. Leia TODOS os arquivos dentro de:
- /codex_docs
- /docs

2. Considere esses documentos como a fonte oficial de verdade arquitetural do projeto.

3. Respeite obrigatoriamente:
- AGENTS.md
- PROJECT_RULES.md
- DIRECTORY_MAP.md
- CODING_STANDARDS.md

4. O projeto é:
- local-first
- single-node
- modular monolith
- event-driven internally
- otimizado para 8GB RAM
- CPU-first
- sem overengineering

5. NÃO utilize:
- Kubernetes
- Kafka
- microservices distribuídos
- transformers pesados
- RL pesado
- infraestrutura cloud complexa

6. Stack obrigatória:
- Python
- Rust apenas para collectors/execution-critical
- Redis Streams
- ClickHouse
- FastAPI
- Streamlit
- Docker Compose
- LightGBM/XGBoost

7. Siga obrigatoriamente:
- INITIAL_TASK_BREAKDOWN.md
- IMPLEMENTATION_PHASES.md

8. Trabalhe incrementalmente.
NÃO tente implementar o sistema inteiro de uma vez.

9. Primeiro objetivo:
Criar a Fase 1 completa:
- bootstrap do repositório
- estrutura de diretórios
- Docker Compose
- configs
- logging
- Redis setup
- ClickHouse setup
- abstração do event bus
- healthcheck básico
- scripts iniciais

10. Gere:
- código production-ready
- tipagem forte
- logging estruturado
- tratamento de erro
- retries
- documentação inline mínima
- testes básicos

11. Antes de codar:
explique o plano de implementação da Fase 1.

12. Após explicar o plano:
aguarde a minha decisão.