from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass
from datetime import datetime

@dataclass
class QueryMetadata:
    """Metadata about a query execution"""
    start_time: datetime
    end_time: Optional[datetime] = None
    rows_affected: Optional[int] = None
    cache_hit: bool = False
    error: Optional[str] = None

class YarnBase(ABC):
    """
    Abstract base class for all Yarn implementations.
    Yarns are responsible for querying different types of data sources (SQL, NoSQL, Vector DBs, APIs, etc.)
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Yarn with configuration parameters.
        
        Args:
            config: Dictionary containing configuration parameters for the Yarn
        """
        self.config = config
        self.metadata = None
        self._validate_config()
    
    @abstractmethod
    def query(self, query_template: str, params: Dict[str, Any]) -> Any:
        """
        Execute a query against the data source.
        
        Args:
            query_template: The query template to execute
            params: Parameters to inject into the query template
            
        Returns:
            Query results in a format appropriate for the data source
            
        Raises:
            YarnException: If there's an error executing the query
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """
        Check the health of the data source connection.
        
        Returns:
            bool: True if the connection is healthy, False otherwise
        """
        pass
    
    @abstractmethod
    def _validate_config(self) -> None:
        """
        Validate the configuration parameters provided to the Yarn.
        
        Raises:
            ValueError: If required configuration parameters are missing or invalid
        """
        pass
    
    def get_metadata(self) -> Optional[QueryMetadata]:
        """
        Get metadata about the last executed query.
        
        Returns:
            QueryMetadata: Metadata about the last query execution, or None if no query has been executed
        """
        return self.metadata
    
    def _start_query(self) -> None:
        """Initialize query metadata at the start of a query execution"""
        self.metadata = QueryMetadata(start_time=datetime.utcnow())
    
    def _end_query(self, rows_affected: Optional[int] = None, error: Optional[str] = None) -> None:
        """
        Update query metadata at the end of a query execution
        
        Args:
            rows_affected: Number of rows affected by the query
            error: Error message if the query failed
        """
        if self.metadata:
            self.metadata.end_time = datetime.utcnow()
            self.metadata.rows_affected = rows_affected
            self.metadata.error = error
    
    def __enter__(self):
        """Context manager entry point"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point - ensures proper resource cleanup"""
        self.close()
    
    def close(self) -> None:
        """
        Clean up any resources held by the Yarn.
        Should be overridden by subclasses if they need to perform cleanup.
        """
        pass
