"""
Exceções customizadas do projeto
"""

class HomeTownBaseException(Exception):
    """Exceção base do projeto"""
    pass

class ExtractionError(HomeTownBaseException):
    """Erro durante extração de dados"""
    pass

class ValidationError(HomeTownBaseException):
    """Erro de validação de dados"""
    pass

class APIConnectionError(ExtractionError):
    """Erro de conexão com API"""
    pass

class DataProcessingError(HomeTownBaseException):
    """Erro durante processamento de dados"""
    pass