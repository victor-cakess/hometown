"""
Configurações centralizadas do projeto
"""

SIGEL_CONFIG = {
    "url": "https://sigel.aneel.gov.br/arcgis/rest/services/PORTAL/WFS/MapServer/0/query",
    "params": {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "true",
        "spatialRel": "esriSpatialRelIntersects"
    },
    "page_size": 1000,  # Registros por página
    "timeout": 30,      # Timeout das requisições
    "max_retries": 3    # Tentativas de retry
}

# Configurações de logging
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S"
}

# Paths dos dados
DATA_PATHS = {
    "raw": "data/raw",
    "processed": "data/processed", 
    "output": "data/output"
}