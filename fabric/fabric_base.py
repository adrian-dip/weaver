"""
Abstract base class for all Fabric implementations.
Defines the interface for managing database connections and API clients.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ConnectionMetrics:
    """Metrics for monitoring connection usage and performance."""
    created_at: datetime
    last_used: datetime
    total_queries: int
    average_response_time: float
    errors: int

class FabricBase(ABC):
    """
    Base class for all Fabric implementations.
    Provides interface for database and API connection management.
    """
    
    def __init__(self, config: dict):
        """
        Initialize the Fabric instance.
        
        Args:
            config (dict): Configuration dictionary containing connection parameters
                         and other settings specific to the fabric implementation.
        """
        self._config = config
        self._metrics: dict[str, ConnectionMetrics] = {}
        self._initialized = False
        
    @abstractmethod
    def get_connection(self) -> Any:
        """
        Get a connection to the data source.
        
        Returns:
            Any: A connection object specific to the implementation.
            
        Raises:
            FabricException: If connection cannot be established.
        """
        pass
    
    @abstractmethod
    def release_connection(self, connection: Any) -> None:
        """
        Release a connection back to the connection pool.
        
        Args:
            connection: The connection object to release.
            
        Raises:
            FabricException: If connection cannot be released.
        """
        pass
    
    @abstractmethod
    def get_api_client(self) -> Any:
        """
        Get an API client instance.
        
        Returns:
            Any: An API client object specific to the implementation.
            
        Raises:
            FabricException: If API client cannot be created.
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """
        Check the health of the fabric's connections.
        
        Returns:
            bool: True if all connections are healthy, False otherwise.
        """
        pass
    
    def initialize(self) -> None:
        """
        Initialize the fabric instance. This should be called before first use.
        
        Raises:
            FabricException: If initialization fails.
        """
        if not self._initialized:
            try:
                self._validate_config()
                self._setup_pools()
                self._initialized = True
            except Exception as e:
                raise FabricException(f"Failed to initialize fabric: {str(e)}")
    
    def get_metrics(self, connection_id: str) -> Optional[ConnectionMetrics]:
        """
        Get metrics for a specific connection.
        
        Args:
            connection_id (str): The ID of the connection to get metrics for.
            
        Returns:
            Optional[ConnectionMetrics]: Metrics for the connection if found, None otherwise.
        """
        return self._metrics.get(connection_id)
    
    def _validate_config(self) -> None:
        """
        Validate the configuration provided to the fabric.
        
        Raises:
            FabricException: If configuration is invalid.
        """
        required_fields = self._get_required_config_fields()
        missing_fields = [field for field in required_fields if field not in self._config]
        if missing_fields:
            raise FabricException(f"Missing required configuration fields: {', '.join(missing_fields)}")
    
    @abstractmethod
    def _get_required_config_fields(self) -> list[str]:
        """
        Get the list of required configuration fields for this fabric.
        
        Returns:
            list[str]: List of required configuration field names.
        """
        pass
    
    @abstractmethod
    def _setup_pools(self) -> None:
        """
        Set up connection pools or other resources needed by the fabric.
        
        Raises:
            FabricException: If resource setup fails.
        """
        pass
    
    def __enter__(self):
        """Context manager entry point."""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        pass