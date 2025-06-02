# Hometown - Analytics Case Makefile

.PHONY: help build up down test clean extract logs shell format lint

# Variáveis
DOCKER_COMPOSE = docker-compose
SERVICE_NAME = hometown

help: ## Mostrar ajuda
	@echo "Hometown - Analytics Case"
	@echo "========================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Construir containers
	$(DOCKER_COMPOSE) build

up: ## Subir containers
	$(DOCKER_COMPOSE) up -d
	@echo "Jupyter disponível em: http://localhost:8888"
	@echo "Para ver logs: make logs"

down: ## Parar containers
	$(DOCKER_COMPOSE) down

test: ## Executar testes
	$(DOCKER_COMPOSE) --profile test run --rm hometown-test

extract: ## Executar apenas extração
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run python -c "
	import sys; sys.path.insert(0, '/app/src')
	from extraction.extractors import SigelExtractor
	from extraction.validators import validate_api_connection
	from config.settings import SIGEL_CONFIG
	
	print('🔍 Validando conexão...')
	validate_api_connection(SIGEL_CONFIG['url'])
	
	print('Iniciando extração...')
	extractor = SigelExtractor()
	files = extractor.extract_all_data()
	print(f'✅ Concluído! {len(files)} arquivos salvos')
	"

logs: ## Ver logs do container
	$(DOCKER_COMPOSE) logs -f $(SERVICE_NAME)

shell: ## Acessar shell do container
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) bash

clean: ## Limpar dados e containers
	$(DOCKER_COMPOSE) down --volumes --rmi all
	docker system prune -f
	rm -rf data/raw/* data/processed/* data/output/* logs/*

format: ## Formatar código
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run black src/ tests/
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run isort src/ tests/

lint: ## Verificar código
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run black --check src/ tests/
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run isort --check-only src/ tests/
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) poetry run mypy src/

setup: ## Setup inicial do projeto
	@echo "Configurando projeto Hometown..."
	mkdir -p data/raw data/processed data/output logs tests
	cp .env.example .env || echo "⚠️ Crie o arquivo .env baseado no .env.example"
	@echo "✅ Setup concluído!"

restart: down up ## Reiniciar containers

status: ## Status dos containers
	$(DOCKER_COMPOSE) ps