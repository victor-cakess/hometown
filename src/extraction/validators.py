"""
Validações para extração de dados
"""
import requests
import logging
from typing import Dict, List, Optional

from config.settings import SIGEL_CONFIG
from utils.exceptions import APIConnectionError, ValidationError
from utils.logger import setup_logger

logger = setup_logger(__name__)

def validate_api_connection(url: str, timeout: int = 30) -> bool:
    """
    Verifica se a API está respondendo
    """
    try:
        params = {"where": "1=1", "returnCountOnly": "true", "f": "json"}
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        if "count" in data:
            logger.info(f"API conectada com sucesso. Total de registros: {data['count']}")
            return True
        else:
            raise ValidationError("Resposta da API não contém campo 'count'")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de conexão com API: {e}")
        raise APIConnectionError(f"Não foi possível conectar com a API: {e}")

def validate_response_structure(response_data: Dict) -> bool:
    """
    Valida estrutura da resposta da API
    """
    required_fields = ["features"]
    
    for field in required_fields:
        if field not in response_data:
            raise ValidationError(f"Campo obrigatório '{field}' não encontrado na resposta")
    
    features = response_data["features"]
    if not isinstance(features, list):
        raise ValidationError("Campo 'features' deve ser uma lista")
    
    if len(features) > 0:
        feature = features[0]
        if "geometry" not in feature or "attributes" not in feature:
            raise ValidationError("Features devem conter 'geometry' e 'attributes'")
    
    logger.info(f"Estrutura da resposta validada. {len(features)} features encontradas")
    return True

def validate_extraction_results(saved_files: List[str], expected_records: int) -> bool:
    """
    Valida resultados da extração
    """
    if not saved_files:
        raise ValidationError("Nenhum arquivo foi salvo durante a extração")
    
    logger.info(f"Validação da extração concluída. {len(saved_files)} arquivos salvos")
    return True