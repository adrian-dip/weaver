"""
Yarns Module
-----------
Data retrieval and integration components for the Weaver project.
Provides unified interfaces for working with various data sources including SQL,
NoSQL, vector databases, and external APIs.
"""

from typing import Dict, Any, Type

from .yarn_base import YarnBase, QueryMetadata
from .yarn_factory import YarnFactory
from .yarn_exceptions import (
    YarnException,
    YarnConfigError,
    YarnConnectionError,
    YarnQueryError,
    YarnAuthenticationError,
    YarnTimeoutError,
    YarnValidationError,
    YarnResourceError,
    YarnDataError
)

from .sql_yarn import SQLYarn
from .nosql_yarn import NoSQLYarn
from .vector_db_yarn import VectorDBYarn
from .api_yarn import APIYarn

__version__ = '0.1.0'
__author__ = 'Weaver Project Contributors'

__all__ = [
    'YarnBase',
    'QueryMetadata',
    
    'YarnFactory',
    
    'SQLYarn',
    'NoSQLYarn',
    'VectorDBYarn',
    'APIYarn',
    
    'YarnException',
    'YarnConfigError',
    'YarnConnectionError',
    'YarnQueryError',
    'YarnAuthenticationError',
    'YarnTimeoutError',
    'YarnValidationError',
    'YarnResourceError',
    'YarnDataError',
]

def create_yarn(yarn_type: str, config: Dict[str, Any]) -> YarnBase:
    """
    Convenience function to create a Yarn instance.
    
    Args:
        yarn_type: Type of Yarn to create ('sql', 'nosql', 'vector_db', 'api')
        config: Configuration dictionary for the Yarn
        
    Returns:
        Configured Yarn instance
        
    Raises:
        YarnConfigError: If yarn_type is not supported or configuration is invalid
    """
    return YarnFactory.create_yarn(yarn_type, config)

def get_supported_yarns() -> Dict[str, str]:
    """
    Get information about supported Yarn types.
    
    Returns:
        Dictionary mapping Yarn types to their descriptions
    """
    return YarnFactory.get_supported_types()

YarnFactory.YARN_REGISTRY = {
    'sql': SQLYarn,
    'nosql': NoSQLYarn,
    'vector_db': VectorDBYarn,
    'api': APIYarn,
}