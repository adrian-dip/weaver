from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from abc import ABC, abstractmethod

from .fabric_base import FabricBase, ConnectionMetrics
from .fabric_exceptions import FabricException
from .connection_pool import ConnectionPool
from .rate_limiter import RateLimiter

@dataclass
class ConnectionConfig:
    """Configuration for a database connection."""
    connection_string: str
    pool_size: int = 5
    max_retries: int = 3
    timeout: int = 30
    retry_interval: int = 1

class ConnectionWrapper:
    """Wraps a database connection with metadata and monitoring."""
    
    def __init__(self, connection: Any, config: ConnectionConfig):
        self.connection = connection
        self.config = config
        self.created_at = datetime.now()
        self.last_used = datetime.now()
        self.total_operations = 0
        self.failed_operations = 0
    
    def mark_used(self):
        """Update usage statistics."""
        self.last_used = datetime.now()
        self.total_operations += 1
    
    def mark_failed(self):
        """Record a failed operation."""
        self.failed_operations += 1

class SQLFabric(FabricBase):
    """
    SQL Fabric implementation that provides a unified interface for database connections.
    Handles connection pooling, monitoring, and lifecycle management.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQLFabric with configuration.
        
        Args:
            config: Dictionary containing connection pool and monitoring settings
        """
        super().__init__(config)
        self._pools: Dict[str, ConnectionPool] = {}
        self._active_connections: Dict[str, ConnectionWrapper] = {}
        self._connection_counter = 0
    
    def _get_required_config_fields(self) -> List[str]:
        """Get required configuration fields."""
        return ['connection_configs']
    
    def _setup_pools(self) -> None:
        """Set up connection pools based on configuration."""
        try:
            for name, config in self._config['connection_configs'].items():
                conn_config = ConnectionConfig(**config)
                self._pools[name] = ConnectionPool(
                    name=name,
                    max_size=conn_config.pool_size,
                    create_connection=lambda: self._create_connection(conn_config)
                )
        except Exception as e:
            raise FabricException(f"Failed to setup connection pools: {str(e)}")
    
    def get_connection(self, pool_name: str = 'default') -> str:
        """
        Get a connection from the specified pool.
        
        Args:
            pool_name: Name of the connection pool to use
            
        Returns:
            Connection identifier that can be used in subsequent operations
            
        Raises:
            FabricException: If connection cannot be obtained
        """
        if not self._initialized:
            self.initialize()
        
        try:
            pool = self._pools.get(pool_name)
            if not pool:
                raise FabricException(f"Unknown connection pool: {pool_name}")
            
            # Get raw connection from pool
            connection = pool.acquire()
            
            # Wrap connection with monitoring
            connection_id = f"{pool_name}_{self._connection_counter}"
            self._connection_counter += 1
            
            wrapper = ConnectionWrapper(
                connection=connection,
                config=ConnectionConfig(**self._config['connection_configs'][pool_name])
            )
            
            self._active_connections[connection_id] = wrapper
            return connection_id
            
        except Exception as e:
            raise FabricException(f"Failed to get connection: {str(e)}")
    
    def execute_operation(self, connection_id: str, operation: Any) -> Any:
        """
        Execute an operation using the specified connection.
        
        Args:
            connection_id: Connection identifier returned by get_connection
            operation: Operation to execute (implementation specific)
            
        Returns:
            Result of the operation
            
        Raises:
            FabricException: If operation fails
        """
        wrapper = self._active_connections.get(connection_id)
        if not wrapper:
            raise FabricException(f"Invalid connection id: {connection_id}")
        
        try:
            wrapper.mark_used()
            return operation(wrapper.connection)
        except Exception as e:
            wrapper.mark_failed()
            raise FabricException(f"Operation failed: {str(e)}")
    
    def release_connection(self, connection_id: str) -> None:
        """
        Release a connection back to its pool.
        
        Args:
            connection_id: Connection identifier to release
            
        Raises:
            FabricException: If connection cannot be released
        """
        try:
            wrapper = self._active_connections.get(connection_id)
            if not wrapper:
                raise FabricException(f"Invalid connection id: {connection_id}")
            
            pool_name = connection_id.split('_')[0]
            pool = self._pools.get(pool_name)
            if pool:
                pool.release(wrapper.connection)
            
            del self._active_connections[connection_id]
            
        except Exception as e:
            raise FabricException(f"Failed to release connection: {str(e)}")
    
    def get_metrics(self, connection_id: str) -> Optional[ConnectionMetrics]:
        """Get metrics for a specific connection."""
        wrapper = self._active_connections.get(connection_id)
        if not wrapper:
            return None
        
        return ConnectionMetrics(
            created_at=wrapper.created_at,
            last_used=wrapper.last_used,
            total_queries=wrapper.total_operations,
            average_response_time=0.0,  # Will need timing logic to implement
            errors=wrapper.failed_operations
        )
    
    def health_check(self) -> bool:
        """Check health of all connection pools."""
        if not self._initialized:
            return False
        
        try:
            for pool in self._pools.values():
                if not pool.health_check():
                    return False
            return True
        except Exception:
            return False
    
    def _create_connection(self, config: ConnectionConfig) -> Any:
        """
        Create a new database connection.
        This method should be overridden by specific database implementations.
        """
        raise NotImplementedError("Must be implemented by specific database fabric")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources on context manager exit."""
        for pool in self._pools.values():
            pool.close()