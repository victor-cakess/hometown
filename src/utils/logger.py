"""
Configuração de logging estruturado
"""
import logging
import sys
from pathlib import Path
from typing import Optional

from config.settings import LOGGING_CONFIG

def setup_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """
    Configura logger com formatação padronizada
    
    Args:
        name: Nome do logger (geralmente __name__)
        log_file: Arquivo de log opcional
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    
    # Evitar duplicação de handlers
    if logger.handlers:
        return logger
        
    logger.setLevel(getattr(logging, LOGGING_CONFIG["level"]))
    
    # Formatter
    formatter = logging.Formatter(
        LOGGING_CONFIG["format"],
        datefmt=LOGGING_CONFIG["date_format"]
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (opcional)
    if log_file:
        log_path = Path("logs")
        log_path.mkdir(exist_ok=True)
        
        file_handler = logging.FileHandler(log_path / log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger