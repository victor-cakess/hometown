"""
Consolidadores de dados - Parquet → CSV final
"""
import pandas as pd
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from config.settings import DATA_PATHS
from utils.logger import setup_logger
from utils.exceptions import DataProcessingError

logger = setup_logger(__name__)

class DataConsolidator:
    def __init__(self):
        self.processed_data_path = self._get_project_root() / "data/processed"
        self.output_data_path = self._get_project_root() / "data/output"
        self.output_data_path.mkdir(parents=True, exist_ok=True)
        
    def _get_project_root(self) -> Path:
        """Detecta raiz do projeto independente de onde está executando"""
        current_path = Path.cwd()
        if current_path.name == "notebooks":
            return current_path.parent
        return current_path
    
    def discover_parquet_files(self) -> List[Path]:
        """Descobre arquivos Parquet na pasta processed"""
        parquet_files = list(self.processed_data_path.glob("aerogeradores_processed_*.parquet"))
        parquet_files.sort()  # Ordenar por nome
        logger.info(f"Descobertos {len(parquet_files)} arquivos Parquet para consolidar")
        return parquet_files
    
    def load_and_combine_parquets(self, parquet_files: List[Path]) -> pd.DataFrame:
        """Carrega e combina todos os arquivos Parquet em um DataFrame"""
        if not parquet_files:
            raise DataProcessingError("Nenhum arquivo Parquet encontrado para consolidar")
        
        logger.info(f"Carregando {len(parquet_files)} arquivos Parquet...")
        
        dataframes = []
        total_records = 0
        
        for parquet_file in parquet_files:
            try:
                df = pd.read_parquet(parquet_file)
                dataframes.append(df)
                total_records += len(df)
                logger.debug(f"Carregado {parquet_file.name}: {len(df)} registros")
                
            except Exception as e:
                logger.error(f"Erro ao carregar {parquet_file.name}: {e}")
                raise DataProcessingError(f"Falha ao carregar {parquet_file.name}: {e}")
        
        # Concatenar todos os DataFrames
        logger.info(f"Consolidando {len(dataframes)} DataFrames...")
        consolidated_df = pd.concat(dataframes, ignore_index=True)
        
        logger.info(f"Consolidação concluída: {len(consolidated_df)} registros totais")
        
        # Validar se não perdemos registros
        if len(consolidated_df) != total_records:
            logger.warning(f"Possível perda de registros: {total_records} → {len(consolidated_df)}")
        
        return consolidated_df
    
    def clean_and_optimize_for_tableau(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e otimiza dados para Tableau"""
        logger.info("Otimizando dados para Tableau...")
        
        df_clean = df.copy()
        
        # 1. Remover colunas desnecessárias (se existirem)
        columns_to_drop = []
        if 'geometry_wkt' in df_clean.columns:
            columns_to_drop.append('geometry_wkt')
        
        if columns_to_drop:
            df_clean = df_clean.drop(columns=columns_to_drop)
            logger.info(f"Removidas colunas: {columns_to_drop}")
        
        # 2. Converter tipos de dados para otimização
        # Floats para coordenadas
        for col in ['latitude', 'longitude']:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # 3. Tratar valores nulos
        null_counts = df_clean.isnull().sum()
        if null_counts.any():
            logger.info(f"Valores nulos encontrados: {null_counts[null_counts > 0].to_dict()}")
        
        # 4. Reordenar colunas (coordenadas primeiro para Tableau)
        priority_columns = ['latitude', 'longitude']
        other_columns = [col for col in df_clean.columns if col not in priority_columns]
        
        # Reorganizar: coordenadas primeiro, depois resto
        final_columns = []
        for col in priority_columns:
            if col in df_clean.columns:
                final_columns.append(col)
        final_columns.extend(other_columns)
        
        df_clean = df_clean[final_columns]
        
        # 5. Validar dados finais
        self._validate_final_data(df_clean)
        
        logger.info(f"Otimização concluída: {len(df_clean)} registros, {len(df_clean.columns)} colunas")
        return df_clean
    
    def _validate_final_data(self, df: pd.DataFrame) -> None:
        """Valida dados finais antes do CSV"""
        logger.info("Validando dados finais...")
        
        # Verificar coordenadas
        if 'latitude' in df.columns and 'longitude' in df.columns:
            # Brasil aproximado: lat -35 a 5, lon -75 a -30
            invalid_lat = ((df['latitude'] < -35) | (df['latitude'] > 5)).sum()
            invalid_lon = ((df['longitude'] < -75) | (df['longitude'] > -30)).sum()
            
            if invalid_lat > 0:
                logger.warning(f"{invalid_lat} latitudes fora do Brasil")
            if invalid_lon > 0:
                logger.warning(f"{invalid_lon} longitudes fora do Brasil")
        
        # Verificar campos importantes
        important_fields = ['POT_MW', 'ALT_TOTAL', 'NOME_EOL']
        for field in important_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Campo {field}: {null_count} valores nulos")
        
        logger.info("Validação de dados finais concluída")
    
    def save_consolidated_csv(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """Salva DataFrame consolidado como CSV"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"aerogeradores_consolidado_{timestamp}.csv"
        
        output_path = self.output_data_path / filename
        
        try:
            # Salvar CSV com configurações otimizadas para Tableau
            df.to_csv(
                output_path,
                index=False,
                encoding='utf-8',
                sep=',',
                float_format='%.6f'  # 6 casas decimais para coordenadas
            )
            
            file_size_mb = output_path.stat().st_size / 1024 / 1024
            
            logger.info(f"CSV salvo: {filename}")
            logger.info(f"Registros: {len(df)}")
            logger.info(f"Colunas: {len(df.columns)}")
            logger.info(f"Tamanho: {file_size_mb:.2f} MB")
            logger.info(f"Localização: {output_path}")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Erro ao salvar CSV: {e}")
            raise DataProcessingError(f"Falha ao salvar CSV: {e}")
    
    def consolidate_all(self, output_filename: Optional[str] = None) -> str:
        """Pipeline completo: Parquet → CSV consolidado"""
        logger.info("Iniciando consolidação completa...")
        
        # 1. Descobrir arquivos Parquet
        parquet_files = self.discover_parquet_files()
        
        # 2. Carregar e combinar
        consolidated_df = self.load_and_combine_parquets(parquet_files)
        
        # 3. Limpar e otimizar
        clean_df = self.clean_and_optimize_for_tableau(consolidated_df)
        
        # 4. Salvar CSV final
        output_path = self.save_consolidated_csv(clean_df, output_filename)
        
        logger.info("Consolidação completa concluída com sucesso!")
        return output_path
    
    def get_data_summary(self, df: pd.DataFrame) -> dict:
        """Gera resumo dos dados para análise"""
        summary = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
        }
        
        # Estatísticas de coordenadas
        if 'latitude' in df.columns and 'longitude' in df.columns:
            summary['coordinate_stats'] = {
                'lat_min': df['latitude'].min(),
                'lat_max': df['latitude'].max(),
                'lon_min': df['longitude'].min(),
                'lon_max': df['longitude'].max(),
            }
        
        # Top campos com mais dados
        if 'NOME_EOL' in df.columns:
            summary['top_eol_names'] = df['NOME_EOL'].value_counts().head(10).to_dict()
        
        return summary