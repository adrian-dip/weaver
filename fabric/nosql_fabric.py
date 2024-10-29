from typing import Any, Dict, List, Optional, Union, Type
from datetime import datetime
from dataclasses import dataclass
from abc import abstractmethod

from .fabric_base import FabricBase, ConnectionMetrics
from .fabric_exceptions import FabricException
from .connection_pool import ConnectionPool
from .rate_limiter import RateLimiter

@dataclass
class NoSQLConnectionConfig:
    """Configuration for NoSQL database connections."""
    db_type: str
    hosts: List[str]
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    keyspace: Optional[str] = None
    pool_size: int = 5
    max_retries: int = 3
    timeout: int = 30
    retry_interval: int = 1

class NoSQLConnectionWrapper:
    """Wraps a NoSQL database connection with metadata and monitoring."""
    
    def __init__(self, connection: Any, config: NoSQLConnectionConfig):
        self.connection = connection
        self.config = config
        self.created_at = datetime.now()
        self.last_used = datetime.now()
        self.total_operations = 0
        self.failed_operations = 0
        self._is_closed = False
    
    def mark_used(self):
        """Update usage statistics."""
        self.last_used = datetime.now()
        self.total_operations += 1
    
    def mark_failed(self):
        """Record a failed operation."""
        self.failed_operations += 1
        
    @property
    def is_closed(self) -> bool:
        return self._is_closed
    
    def close(self):
        """Close the wrapped connection."""
        if not self._is_closed:
            try:
                if hasattr(self.connection, 'close'):
                    self.connection.close()
                elif hasattr(self.connection, 'disconnect'):
                    self.connection.disconnect()
            finally:
                self._is_closed = True

class NoSQLFabric(FabricBase):
    """
    NoSQL Fabric implementation that provides a unified interface for NoSQL databases.
    Supports MongoDB, Redis, and Cassandra through a common interface.
    """
    
    SUPPORTED_DATABASES = {'mongodb', 'redis', 'cassandra'}
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize NoSQLFabric with configuration.
        
        Args:
            config: Dictionary containing connection pool and database settings
        """
        super().__init__(config)
        self._pools: Dict[str, ConnectionPool] = {}
        self._active_connections: Dict[str, NoSQLConnectionWrapper] = {}
        self._connection_counter = 0
        
    def _get_required_config_fields(self) -> List[str]:
        """Get required configuration fields."""
        return ['connection_configs']
    
    def _setup_pools(self) -> None:
        """Set up connection pools for each configured database."""
        try:
            for name, config in self._config['connection_configs'].items():
                conn_config = NoSQLConnectionConfig(**config)
                if conn_config.db_type not in self.SUPPORTED_DATABASES:
                    raise FabricException(f"Unsupported database type: {conn_config.db_type}")
                
                self._pools[name] = ConnectionPool(
                    name=name,
                    max_size=conn_config.pool_size,
                    create_connection=lambda: self._create_connection(conn_config)
                )
        except Exception as e:
            raise FabricException(f"Failed to setup connection pools: {str(e)}")
    
    def _create_connection(self, config: NoSQLConnectionConfig) -> Any:
        """Create a new database connection based on the database type."""
        try:
            if config.db_type == 'mongodb':
                return self._create_mongodb_connection(config)
            elif config.db_type == 'redis':
                return self._create_redis_connection(config)
            elif config.db_type == 'cassandra':
                return self._create_cassandra_connection(config)
            else:
                raise FabricException(f"Unsupported database type: {config.db_type}")
        except ImportError as e:
            raise FabricException(f"Required package not installed for {config.db_type}: {str(e)}")
        except Exception as e:
            raise FabricException(f"Failed to create {config.db_type} connection: {str(e)}")
    
    def _create_mongodb_connection(self, config: NoSQLConnectionConfig) -> Any:
        """Create MongoDB connection."""
        from pymongo import MongoClient
        
        connection_string = f"mongodb://"
        if config.username and config.password:
            connection_string += f"{config.username}:{config.password}@"
        
        connection_string += f"{config.hosts[0]}:{config.port}"
        if config.database:
            connection_string += f"/{config.database}"
            
        return MongoClient(
            connection_string,
            serverSelectionTimeoutMS=config.timeout * 1000
        )
    
    def _create_redis_connection(self, config: NoSQLConnectionConfig) -> Any:
        """Create Redis connection."""
        import redis
        
        return redis.Redis(
            host=config.hosts[0],
            port=config.port,
            username=config.username,
            password=config.password,
            db=config.database if config.database else 0,
            decode_responses=True,
            socket_timeout=config.timeout
        )
    
    def _create_cassandra_connection(self, config: NoSQLConnectionConfig) -> Any:
        """Create Cassandra connection."""
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        
        auth_provider = None
        if config.username and config.password:
            auth_provider = PlainTextAuthProvider(
                username=config.username,
                password=config.password
            )
        
        cluster = Cluster(
            contact_points=config.hosts,
            port=config.port,
            auth_provider=auth_provider,
            connect_timeout=config.timeout
        )
        
        session = cluster.connect(config.keyspace if config.keyspace else None)
        return session
    
    def get_connection(self, pool_name: str = 'default') -> str:
        """
        Get a connection from the specified pool.
        
        Args:
            pool_name: Name of the connection pool to use
            
        Returns:
            Connection identifier for use in subsequent operations
            
        Raises:
            FabricException: If connection cannot be obtained
        """
        if not self._initialized:
            self.initialize()
        
        try:
            pool = self._pools.get(pool_name)
            if not pool:
                raise FabricException(f"Unknown connection pool: {pool_name}")
            
            connection = pool.acquire()
            
            connection_id = f"{pool_name}_{self._connection_counter}"
            self._connection_counter += 1
            
            wrapper = NoSQLConnectionWrapper(
                connection=connection,
                config=NoSQLConnectionConfig(**self._config['connection_configs'][pool_name])
            )
            
            self._active_connections[connection_id] = wrapper
            return connection_id
            
        except Exception as e:
            raise FabricException(f"Failed to get connection: {str(e)}")
    
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
            
            if not wrapper.is_closed:
                wrapper.close()
            
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
            average_response_time=0.0,  
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
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources when exiting context."""
        try:
            for wrapper in self._active_connections.values():
                if not wrapper.is_closed:
                    wrapper.close()
            
            self._active_connections.clear()
            
            for pool in self._pools.values():
                pool.close()
                
            self._pools.clear()
            
        except Exception as e:
            raise FabricException(f"Error during cleanup: {str(e)}")