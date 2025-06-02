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
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
        
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

    def _get_sample_data(self) -> Dict:
        """Pega amostra da API (primeira página) para verificar freshness"""
        try:
            params = {**self.params, "resultRecordCount": 100}  # Só 100 registros
            return self._make_request(params)
        except Exception as e:
            logger.warning(f"Erro ao obter amostra da API: {e}")
            return {}

    def _extract_latest_update_date(self, api_data: Dict) -> int:
        """Extrai a data de atualização mais recente dos dados da API"""
        features = api_data.get('features', [])
        if not features:
            return 0
        
        # Pegar todas as datas de atualização
        update_dates = []
        for feature in features:
            attrs = feature.get('attributes', {})
            if 'DATA_ATUALIZACAO' in attrs and attrs['DATA_ATUALIZACAO']:
                try:
                    update_dates.append(int(attrs['DATA_ATUALIZACAO']))
                except (ValueError, TypeError):
                    continue
        
        # Retornar a mais recente
        return max(update_dates) if update_dates else 0

    def _get_last_extraction_info(self) -> Dict:
        """Carrega informações da última extração"""
        metadata_file = self.raw_data_path / "extraction_metadata.json"
        
        if not metadata_file.exists():
            return {}
        
        try:
            with open(metadata_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Erro ao ler metadata: {e}")
            return {}

    def _save_extraction_metadata(self, latest_update: int, total_records: int, saved_files: List[str]):
        """Salva metadados da extração para próximas execuções"""
        metadata = {
            'extraction_timestamp': datetime.now().isoformat(),
            'api_latest_update': latest_update,
            'total_records': total_records,
            'files_created': len(saved_files),
            'file_pattern': 'aerogeradores_raw_*.json'
        }
        
        metadata_file = self.raw_data_path / "extraction_metadata.json"
        try:
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Metadata salvo: última atualização {latest_update}")
        except Exception as e:
            logger.error(f"Erro ao salvar metadata: {e}")

    def check_data_freshness(self) -> Dict:
        """Verifica se dados da API são mais novos que nossa extração"""
        logger.info("Verificando freshness dos dados...")
        
        # 1. Pegar amostra da API
        sample_data = self._get_sample_data()
        latest_update = self._extract_latest_update_date(sample_data)
        
        # 2. Verificar nossa última extração
        last_extraction_info = self._get_last_extraction_info()
        last_update = last_extraction_info.get('api_latest_update', 0)
        
        needs_refresh = latest_update != last_update or latest_update == 0
        
        logger.info(f"API última atualização: {latest_update}")
        logger.info(f"Nossa última extração: {last_update}")
        logger.info(f"Precisa atualizar: {needs_refresh}")
        
        return {
            'api_latest_update': latest_update,
            'our_last_extraction': last_update,
            'needs_refresh': needs_refresh,
            'last_extraction_time': last_extraction_info.get('extraction_timestamp'),
            'last_total_records': last_extraction_info.get('total_records', 0)
        }

    def discover_existing_files(self) -> List[str]:
        """Descobre arquivos já extraídos"""
        existing_files = list(self.raw_data_path.glob("aerogeradores_raw_*.json"))
        existing_files.sort()
        return [str(f) for f in existing_files]

    def extract_all_data(self, force_refresh: bool = False) -> List[str]:
        """Extrai todos os dados da API (com verificação de idempotência)"""
        
        # Verificar se precisa extrair
        if not force_refresh:
            freshness = self.check_data_freshness()
            
            if not freshness['needs_refresh']:
                existing_files = self.discover_existing_files()
                if len(existing_files) > 0:
                    logger.info(f"Dados já atualizados! Usando {len(existing_files)} arquivos existentes.")
                    logger.info(f"Última extração: {freshness['last_extraction_time']}")
                    logger.info(f"Total de registros: {freshness['last_total_records']}")
                    return existing_files
            else:
                logger.info("Dados da API foram atualizados, executando nova extração...")
                # Limpar dados antigos quando API mudou
                self._cleanup_old_extractions()
        else:
            logger.info("Forçando nova extração...")
            # Limpar dados antigos no force refresh
            self._cleanup_old_extractions()
        
        # Executar extração normal
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 1. Descobrir total de registros
        total_records = self._get_total_count()
        total_pages = (total_records + self.page_size - 1) // self.page_size
        
        # 2. Obter data de atualização da API
        sample_data = self._get_sample_data()
        latest_update = self._extract_latest_update_date(sample_data)
        
        logger.info(f"Total de registros: {total_records}")
        logger.info(f"Total de páginas: {total_pages}")
        logger.info(f"Data de atualização da API: {latest_update}")
        
        # 3. Extrair dados página por página
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
            
            # Coletar resultados
            for future in as_completed(future_to_page):
                page_num, records_count = future_to_page[future]
                try:
                    filepath = future.result()
                    saved_files.append(filepath)
                    processed_records += records_count
                    logger.info(f"Página {page_num} salva: {records_count} registros")
                except Exception as e:
                    logger.error(f"Erro ao processar página {page_num}: {e}")
        
        # 4. Salvar metadata da extração
        self._save_extraction_metadata(latest_update, total_records, saved_files)
        
        # 5. Log final
        logger.info(f"✅ Extração concluída!")
        logger.info(f"Arquivos salvos: {len(saved_files)}")
        logger.info(f"Registros processados: {processed_records}/{total_records}")
        
        return saved_files
    
    def _cleanup_old_extractions(self):
        """Remove arquivos de extrações anteriores"""
        try:
            # Encontrar arquivos antigos
            old_json_files = list(self.raw_data_path.glob("aerogeradores_raw_*.json"))
            old_metadata = self.raw_data_path / "extraction_metadata.json"
            
            # Remover arquivos JSON antigos
            removed_count = 0
            for file in old_json_files:
                file.unlink()
                removed_count += 1
                logger.debug(f"Removido arquivo antigo: {file.name}")
            
            # Remover metadata antigo
            if old_metadata.exists():
                old_metadata.unlink()
                
            if removed_count > 0:
                logger.info(f"Removidos {removed_count} arquivos JSON antigos")
                
        except Exception as e:
            logger.warning(f"Erro ao limpar arquivos antigos: {e}")

    def cleanup_all_raw_data(self):
        """Remove todos os dados raw (método público para limpeza manual)"""
        try:
            old_json_files = list(self.raw_data_path.glob("aerogeradores_raw_*.json"))
            old_metadata = self.raw_data_path / "extraction_metadata.json"
            
            removed_count = 0
            for file in old_json_files:
                file.unlink()
                removed_count += 1
            
            if old_metadata.exists():
                old_metadata.unlink()
                
            logger.info(f"Limpeza manual: {removed_count} arquivos removidos")
            return removed_count
            
        except Exception as e:
            logger.error(f"Erro na limpeza manual: {e}")
            return 0