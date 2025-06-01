"""
Utilitários geográficos
"""
import geopandas as gpd
import pandas as pd
from typing import Tuple
from shapely.geometry import Point

from utils.logger import setup_logger
from utils.exceptions import ValidationError

logger = setup_logger(__name__)

def extract_coordinates(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Extrai coordenadas lat/long da geometria
    """
    try:
        gdf_copy = gdf.copy()
        
        # Extrair X e Y da geometria
        gdf_copy['longitude'] = gdf_copy.geometry.x
        gdf_copy['latitude'] = gdf_copy.geometry.y
        
        logger.debug(f"Coordenadas extraídas para {len(gdf_copy)} registros")
        return gdf_copy
        
    except Exception as e:
        logger.error(f"Erro ao extrair coordenadas: {e}")
        raise ValidationError(f"Falha na extração de coordenadas: {e}")

def validate_geometry(gdf: gpd.GeoDataFrame) -> bool:
    """
    Valida dados geográficos
    """
    try:
        # Verificar se tem geometria
        if 'geometry' not in gdf.columns:
            raise ValidationError("GeoDataFrame não contém coluna 'geometry'")
        
        # Verificar geometrias válidas
        invalid_geom = gdf.geometry.isna().sum()
        if invalid_geom > 0:
            logger.warning(f"{invalid_geom} geometrias inválidas encontradas")
        
        # Verificar coordenadas dentro do Brasil (aproximadamente)
        if 'longitude' in gdf.columns and 'latitude' in gdf.columns:
            # Brasil: lat -35 a 5, lon -75 a -30
            invalid_coords = (
                (gdf['latitude'] < -35) | (gdf['latitude'] > 5) |
                (gdf['longitude'] < -75) | (gdf['longitude'] > -30)
            ).sum()
            
            if invalid_coords > 0:
                logger.warning(f"{invalid_coords} coordenadas fora do Brasil encontradas")
        
        total_records = len(gdf)
        valid_records = total_records - invalid_geom
        
        logger.info(f"Validação geográfica: {valid_records}/{total_records} registros válidos")
        return True
        
    except Exception as e:
        logger.error(f"Erro na validação geográfica: {e}")
        raise ValidationError(f"Falha na validação geográfica: {e}")

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcula distância entre dois pontos (haversine)
    """
    from math import radians, cos, sin, asin, sqrt
    
    # Converter para radianos
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    # Raio da Terra em km
    r = 6371
    
    return c * r

def validate_coordinates_range(df: pd.DataFrame) -> bool:
    """
    Valida se coordenadas estão em range válido
    """
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        return False
    
    # Verificar range global
    valid_lat = (df['latitude'] >= -90) & (df['latitude'] <= 90)
    valid_lon = (df['longitude'] >= -180) & (df['longitude'] <= 180)
    
    invalid_count = (~(valid_lat & valid_lon)).sum()
    
    if invalid_count > 0:
        logger.warning(f"{invalid_count} coordenadas fora do range global")
        return False
    
    return True