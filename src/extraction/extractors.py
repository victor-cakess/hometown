"""
Extratores de dados da API SIGEL/ANEEL
"""
import json
import logging
import requests
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import SIGEL_CONFIG
from utils.logger import setup_logger

logger = setup_logger(__name__)

class SigelExtractor:
    def __init__(self):
        self.base_url = SIGEL_CONFIG["url"]
        self.params = SIGEL_CONFIG["params"]
        self.page_size = SIGEL_CONFIG["page_size"]
        self.max_retries = SIGEL_CONFIG["max_retries"]
        current_path = Path.cwd()
        if current_path.name == "notebooks":
            project_root = current_path.parent
        else:
            project_root = current_path
        
        self.raw_data_path = project_root / "data/raw"
        
    def _make_request(self, params: Dict) -> Optional[Dict]:
        """Faz request HTTP com retry manual"""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Tentativa {attempt + 1} falhou: {e}. Tentando novamente em {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Erro na requisição após {self.max_retries} tentativas: {e}")
                    raise

    def _get_total_count(self) -> int:
        """Descobre total de registros na API"""
        params = {**self.params, "returnCountOnly": "true"}
        response = self._make_request(params)
        return response.get("count", 0)

    def _save_page_data(self, data: Dict, page: int, timestamp: str) -> str:
        """Salva dados de uma página"""
        filename = f"aerogeradores_raw_{timestamp}_page_{page:04d}.json"
        filepath = self.raw_data_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return str(filepath)

    def extract_all_data(self) -> List[str]:
        """Extrai todos os dados da API"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        total_records = self._get_total_count()
        total_pages = (total_records + self.page_size - 1) // self.page_size
        
        logger.info(f"Total de registros: {total_records}")
        logger.info(f"Total de páginas: {total_pages}")
        
        saved_files = []
        processed_records = 0
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_page = {}
            
            for page in range(total_pages):
                offset = page * self.page_size
                params = {
                    **self.params,
                    "resultOffset": offset,
                    "resultRecordCount": self.page_size
                }
                
                logger.info(f"Extraindo página {page + 1}/{total_pages}")
                page_data = self._make_request(params)
                
                if page_data and page_data.get("features"):
                    future = executor.submit(
                        self._save_page_data, 
                        page_data, 
                        page + 1, 
                        timestamp
                    )
                    future_to_page[future] = (page + 1, len(page_data["features"]))
            
            for future in as_completed(future_to_page):
                page_num, records_count = future_to_page[future]
                try:
                    filepath = future.result()
                    saved_files.append(filepath)
                    processed_records += records_count
                    logger.info(f"Página {page_num} salva: {records_count} registros")
                except Exception as e:
                    logger.error(f"Erro ao processar página {page_num}: {e}")
        
        logger.info(f"Extração concluída!")
        logger.info(f"Arquivos salvos: {len(saved_files)}")
        logger.info(f"Registros processados: {processed_records}/{total_records}")
        
        return saved_files