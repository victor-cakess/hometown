# Makefile para Projeto Hometown
.PHONY: help build up down logs shell test lint format clean install

# Variáveis
DOCKER_COMPOSE = docker-compose
SERVICE_NAME = hometown-app
TEST_SERVICE = hometown-test

# Cores para output
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

## Ajuda
help: ## Mostra esta mensagem de ajuda
	@echo "$(GREEN)Comandos disponíveis para o projeto Hometown:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## Setup e Build
install: ## Instala dependências localmente com Poetry
	@echo "$(GREEN)Instalando dependências...$(NC)"
	poetry install

build: ## Constrói as imagens Docker
	@echo "$(GREEN)Construindo imagens Docker...$(NC)"
	$(DOCKER_COMPOSE) build

## Execução
up: ## Sobe os containers em background
	@echo "$(GREEN)Subindo containers...$(NC)"
	$(DOCKER_COMPOSE) up -d

up-logs: ## Sobe os containers com logs visíveis
	@echo "$(GREEN)Subindo containers com logs...$(NC)"
	$(DOCKER_COMPOSE) up

down: ## Para e remove os containers
	@echo "$(YELLOW)Parando containers...$(NC)"
	$(DOCKER_COMPOSE) down

restart: down up ## Reinicia os containers

## Logs e Debug
logs: ## Mostra logs do container principal
	$(DOCKER_COMPOSE) logs -f $(SERVICE_NAME)

shell: ## Acessa shell do container principal
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) bash

jupyter: ## Abre Jupyter notebook
	@echo "$(GREEN)Jupyter disponível em: http://localhost:8888$(NC)"
	$(DOCKER_COMPOSE) up $(SERVICE_NAME)

## Testes
test: ## Executa todos os testes
	@echo "$(GREEN)Executando testes...$(NC)"
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE)

test-unit: ## Executa apenas testes unitários
	@echo "$(GREEN)Executando testes unitários...$(NC)"
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run pytest tests/unit/ -v

test-integration: ## Executa testes de integração
	@echo "$(GREEN)Executando testes de integração...$(NC)"
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run pytest tests/integration/ -v

test-cov: ## Executa testes com coverage
	@echo "$(GREEN)Executando testes com coverage...$(NC)"
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run pytest --cov=src --cov-report=html

## Qualidade de Código
lint: ## Executa linting (flake8, mypy)
	@echo "$(GREEN)Executando linting...$(NC)"
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run flake8 src/
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run mypy src/

format: ## Formata código (black, isort)
	@echo "$(GREEN)Formatando código...$(NC)"
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run black src/ tests/
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run isort src/ tests/

format-check: ## Verifica formatação sem alterar
	@echo "$(GREEN)Verificando formatação...$(NC)"
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run black --check src/ tests/
	$(DOCKER_COMPOSE) run --rm $(TEST_SERVICE) poetry run isort --check-only src/ tests/

quality: format lint test ## Executa todas as verificações de qualidade

## Pipeline
extract: ## Executa apenas extração de dados
	@echo "$(GREEN)Executando extração...$(NC)"
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run python -c "from src.extract.web_scraper import main; main()"

load: ## Executa carregamento (parquet + csv)
	@echo "$(GREEN)Executando carregamento...$(NC)"
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run python -c "from src.load.data_loader import main; main()"

transform: ## Executa transformação dos dados
	@echo "$(GREEN)Executando transformação...$(NC)"
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run python -c "from src.transform.geo_processor import main; main()"

pipeline: extract load transform ## Executa pipeline completo ELT

## Limpeza
clean: ## Remove containers, volumes e imagens
	@echo "$(YELLOW)Limpando ambiente Docker...$(NC)"
	$(DOCKER_COMPOSE) down -v --rmi all --remove-orphans

clean-data: ## Limpa diretórios de dados
	@echo "$(YELLOW)Limpando dados...$(NC)"
	rm -rf data/raw/* data/parquet/* data/consolidated/* data/outputs/*

clean-cache: ## Limpa cache Python
	@echo "$(YELLOW)Limpando cache...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-all: clean clean-data clean-cache ## Limpeza completa

## Desenvolvimento
dev-setup: install build up ## Setup completo para desenvolvimento
	@echo "$(GREEN)Setup de desenvolvimento concluído!$(NC)"
	@echo "$(GREEN)Jupyter disponível em: http://localhost:8888$(NC)"

## Status
status: ## Mostra status dos containers
	$(DOCKER_COMPOSE) ps

health: ## Verifica saúde dos containers
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run python -c "print('Container OK!')"