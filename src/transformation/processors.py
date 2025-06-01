"""
Processadores de transformação de dados
"""
import json
import pandas as pd
import geopandas as gpd
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import DATA_PATHS, SIGEL_CONFIG
from utils.logger import setup_logger
from utils.exceptions import DataProcessingError
from .geo_utils import extract_coordinates, validate_geometry

logger = setup_logger(__name__)

class DataProcessor:
    def __init__(self):
        self.raw_data_path = self._get_project_root() / "data/raw"
        self.processed_data_path = self._get_project_root() / "data/processed"
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        
    def _get_project_root(self) -> Path:
        """Detecta raiz do projeto independente de onde está executando"""
        current_path = Path.cwd()
        if current_path.name == "notebooks":
            return current_path.parent
        return current_path
    
    def discover_raw_files(self) -> List[Path]:
        """Descobre arquivos JSON na pasta raw"""
        json_files = list(self.raw_data_path.glob("aerogeradores_raw_*.json"))
        json_files.sort()  # Ordenar por nome
        logger.info(f"Descobertos {len(json_files)} arquivos JSON para processar")
        return json_files
    
    def check_transformation_needed(self) -> Dict:
        """Verifica se transformação é necessária"""
        json_files = self.discover_raw_files()
        parquet_files = list(self.processed_data_path.glob("aerogeradores_processed_*.parquet"))
        
        if not json_files:
            return {
                'needs_transformation': False, 
                'reason': 'Nenhum arquivo JSON encontrado'
            }
        
        if len(parquet_files) >= len(json_files):
            latest_json = max(f.stat().st_mtime for f in json_files)
            latest_parquet = max(f.stat().st_mtime for f in parquet_files)
            
            if latest_parquet > latest_json:
                return {
                    'needs_transformation': False, 
                    'reason': f'Parquets já atualizados ({len(parquet_files)} arquivos)',
                    'json_count': len(json_files),
                    'parquet_count': len(parquet_files)
                }
        
        return {
            'needs_transformation': True, 
            'reason': 'Novos JSONs encontrados - transformação necessária',
            'json_count': len(json_files), 
            'parquet_count': len(parquet_files)
        }
    
    def _json_to_geodataframe(self, json_file: Path) -> gpd.GeoDataFrame:
        """Converte um arquivo JSON em GeoDataFrame"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extrair features
            features = data.get('features', [])
            if not features:
                logger.warning(f"Arquivo {json_file.name} não contém features")
                return gpd.GeoDataFrame()
            
            # Criar DataFrame dos attributes
            attributes_list = []
            geometries = []
            
            for feature in features:
                attrs = feature.get('attributes', {})
                geom = feature.get('geometry', {})
                
                attributes_list.append(attrs)
                geometries.append(geom)
            
            # Criar DataFrame
            df = pd.DataFrame(attributes_list)
            
            # Criar GeoDataFrame
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(
                [g.get('x', 0) for g in geometries],
                [g.get('y', 0) for g in geometries]
            ))
            
            # Extrair coordenadas como colunas separadas
            gdf = extract_coordinates(gdf)
            
            logger.info(f"Processado {json_file.name}: {len(gdf)} registros")
            return gdf
            
        except Exception as e:
            logger.error(f"Erro ao processar {json_file.name}: {e}")
            raise DataProcessingError(f"Falha ao processar {json_file.name}: {e}")
    
    def _save_geodataframe(self, gdf: gpd.GeoDataFrame, output_name: str) -> str:
        """Salva GeoDataFrame como Parquet"""
        try:
            output_path = self.processed_data_path / f"{output_name}.parquet"
            
            # Converter geometry para WKT antes de salvar (Parquet não suporta geometry nativo)
            gdf_copy = gdf.copy()
            gdf_copy['geometry_wkt'] = gdf_copy.geometry.to_wkt()
            
            # Remover coluna geometry original
            df_to_save = gdf_copy.drop(columns=['geometry'])
            
            # Salvar como Parquet
            df_to_save.to_parquet(output_path, index=False, engine='pyarrow')
            
            logger.info(f"Salvo {output_name}.parquet: {len(df_to_save)} registros, {output_path.stat().st_size / 1024 / 1024:.2f} MB")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Erro ao salvar {output_name}: {e}")
            raise DataProcessingError(f"Falha ao salvar {output_name}: {e}")
    
    def process_single_file(self, json_file: Path) -> str:
        """Processa um único arquivo JSON → Parquet"""
        # Extrair timestamp do nome do arquivo
        file_name = json_file.stem  # Remove .json
        output_name = file_name.replace('raw', 'processed')
        
        # Converter JSON → GeoDataFrame
        gdf = self._json_to_geodataframe(json_file)
        
        if gdf.empty:
            logger.warning(f"GeoDataFrame vazio para {json_file.name}, pulando...")
            return ""
        
        # Validar dados
        validate_geometry(gdf)
        
        # Salvar como Parquet
        output_path = self._save_geodataframe(gdf, output_name)
        return output_path

        
    def process_all_files(self, max_workers: int = 4, force_refresh: bool = False) -> List[str]:
        """Processa todos os arquivos JSON em paralelo (com verificação de idempotência)"""
        
        # Verificar se transformação é necessária
        if not force_refresh:
            check_result = self.check_transformation_needed()
            if not check_result['needs_transformation']:
                existing_parquets = list(self.processed_data_path.glob("aerogeradores_processed_*.parquet"))
                reason = check_result.get('reason', 'Parquets já atualizados')
                logger.info(f"✅ {reason}")
                return [str(f) for f in existing_parquets]
            else:
                reason = check_result.get('reason', 'Transformação necessária')
                logger.info(f"🔄 Transformação necessária: {reason}")
                # Limpar parquets antigos quando há novos JSONs
                self._cleanup_old_transformations()
        else:
            logger.info("🔄 Forçando nova transformação...")
            # Limpar parquets antigos no force refresh
            self._cleanup_old_transformations()
        
        # Executar transformação normal
        json_files = self.discover_raw_files()
        
        if not json_files:
            logger.warning("Nenhum arquivo JSON encontrado para processar")
            return []
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Iniciando processamento de {len(json_files)} arquivos...")
        
        processed_files = []
        failed_files = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submeter jobs
            future_to_file = {
                executor.submit(self.process_single_file, json_file): json_file 
                for json_file in json_files
            }
            
            # Coletar resultados
            for future in as_completed(future_to_file):
                json_file = future_to_file[future]
                try:
                    output_path = future.result()
                    if output_path:  # Se não está vazio
                        processed_files.append(output_path)
                    logger.info(f"✅ Processado: {json_file.name}")
                except Exception as e:
                    failed_files.append(str(json_file))
                    logger.error(f"❌ Falha ao processar {json_file.name}: {e}")
        
        # Log final
        logger.info(f"Processamento concluído!")
        logger.info(f"Arquivos processados: {len(processed_files)}")
        logger.info(f"Arquivos com falha: {len(failed_files)}")
        
        if failed_files:
            logger.warning(f"Arquivos com falha: {failed_files}")
        
        return processed_files
    
    def cleanup_all_processed_data(self):
        """Remove todos os dados processados (método público para limpeza manual)"""
        try:
            old_parquet_files = list(self.processed_data_path.glob("aerogeradores_processed_*.parquet"))
            
            removed_count = 0
            for file in old_parquet_files:
                file.unlink()
                removed_count += 1
            
            logger.info(f"🗑️ Limpeza manual: {removed_count} parquets removidos")
            return removed_count
            
        except Exception as e:
            logger.error(f"Erro na limpeza manual de parquets: {e}")
            return 0

    def _cleanup_old_transformations(self):
        """Remove transformações antigas quando nova extração é feita"""
        try:
            old_parquet_files = list(self.processed_data_path.glob("aerogeradores_processed_*.parquet"))
            
            removed_count = 0
            for file in old_parquet_files:
                file.unlink()
                removed_count += 1
                
            if removed_count > 0:
                logger.info(f"🧹 Removidos {removed_count} parquets antigos")
                
        except Exception as e:
            logger.warning(f"Erro ao limpar parquets antigos: {e}")