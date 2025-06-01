# Dockerfile
FROM python:3.11-slim

# Configuração de variáveis de ambiente
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VENV_IN_PROJECT=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Instalar dependências do sistema necessárias para geopandas
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    gcc \
    g++ \
    libproj-dev \
    proj-data \
    proj-bin \
    libgeos-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar Poetry
RUN pip install poetry==1.6.1

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivos de configuração do Poetry
COPY pyproject.toml poetry.lock* ./

# Instalar dependências
RUN poetry install --no-dev && rm -rf $POETRY_CACHE_DIR

# Copiar código fonte
COPY . .

# Criar diretórios necessários
RUN mkdir -p data/raw data/parquet data/consolidated data/outputs logs

# Definir usuário não-root para segurança
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

# Expor porta para Jupyter (se necessário)
EXPOSE 8888

# Comando padrão
CMD ["poetry", "run", "jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]