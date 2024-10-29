from typing import Any, Dict, List, Optional, Union, Type
from datetime import datetime
from dataclasses import dataclass
import numpy as np

from .fabric_base import FabricBase, ConnectionMetrics
from .fabric_exceptions import FabricException
from .connection_pool import ConnectionPool
from .rate_limiter import RateLimiter

@dataclass
class VectorDBConnectionConfig:
    """Configuration for vector database connections."""
    db_type: str
    dimension: int
    hosts: List[str]
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    environment: Optional[str] = None
    pool_size: int = 5
    max_retries: int = 3
    timeout: int = 30
    retry_interval: int = 1
    metric: str = 'cosine'  # cosine, euclidean, dot_product

class VectorDBConnectionWrapper:
    """Wraps a vector database connection with metadata and monitoring."""
    
    def __init__(self, connection: Any, config: VectorDBConnectionConfig):
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

class VectorDBFabric(FabricBase):
    """
    Vector Database Fabric implementation that provides a unified interface for vector databases.
    Supports PostgreSQL with pgvector and Pinecone through a common interface.
    """
    
    SUPPORTED_DATABASES = {'pgvector', 'pinecone'}
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize VectorDBFabric with configuration.
        
        Args:
            config: Dictionary containing connection pool and database settings
        """
        super().__init__(config)
        self._pools: Dict[str, ConnectionPool] = {}
        self._active_connections: Dict[str, VectorDBConnectionWrapper] = {}
        self._connection_counter = 0
        
    def _get_required_config_fields(self) -> List[str]:
        """Get required configuration fields."""
        return ['connection_configs']
    
    def _setup_pools(self) -> None:
        """Set up connection pools for each configured database."""
        try:
            for name, config in self._config['connection_configs'].items():
                conn_config = VectorDBConnectionConfig(**config)
                if conn_config.db_type not in self.SUPPORTED_DATABASES:
                    raise FabricException(f"Unsupported database type: {conn_config.db_type}")
                
                self._pools[name] = ConnectionPool(
                    name=name,
                    max_size=conn_config.pool_size,
                    create_connection=lambda: self._create_connection(conn_config)
                )
        except Exception as e:
            raise FabricException(f"Failed to setup connection pools: {str(e)}")
    
    def _create_connection(self, config: VectorDBConnectionConfig) -> Any:
        """Create a new database connection based on the database type."""
        try:
            if config.db_type == 'pgvector':
                return self._create_pgvector_connection(config)
            elif config.db_type == 'pinecone':
                return self._create_pinecone_connection(config)
            else:
                raise FabricException(f"Unsupported database type: {config.db_type}")
        except ImportError as e:
            raise FabricException(f"Required package not installed for {config.db_type}: {str(e)}")
        except Exception as e:
            raise FabricException(f"Failed to create {config.db_type} connection: {str(e)}")
    
    def _create_pgvector_connection(self, config: VectorDBConnectionConfig) -> Any:
        """Create PostgreSQL with pgvector connection."""
        try:
            import psycopg2
            from psycopg2.extras import Json
            
            conn = psycopg2.connect(
                host=config.hosts[0],
                port=config.port or 5432,
                database='postgres',  # Default database
                user=config.username,
                password=config.password
            )
            
            # Enable pgvector extension if not already enabled
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                conn.commit()
            
            return conn
            
        except Exception as e:
            raise FabricException(f"Failed to create pgvector connection: {str(e)}")
    
    def _create_pinecone_connection(self, config: VectorDBConnectionConfig) -> Any:
        """Create Pinecone connection."""
        try:
            import pinecone
            
            pinecone.init(
                api_key=config.api_key,
                environment=config.environment
            )
            
            return pinecone
            
        except Exception as e:
            raise FabricException(f"Failed to create Pinecone connection: {str(e)}")
    
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
            
            wrapper = VectorDBConnectionWrapper(
                connection=connection,
                config=VectorDBConnectionConfig(**self._config['connection_configs'][pool_name])
            )
            
            self._active_connections[connection_id] = wrapper
            return connection_id
            
        except Exception as e:
            raise FabricException(f"Failed to get connection: {str(e)}")
    
    def execute_operation(self, connection_id: str, operation: str, 
                         collection_name: str, **kwargs) -> Any:
        """
        Execute a vector database operation.
        
        Args:
            connection_id: Connection identifier
            operation: Operation type (create_collection, upsert, search, etc.)
            collection_name: Name of the collection to operate on
            **kwargs: Additional operation-specific parameters
            
        Returns:
            Operation result
            
        Raises:
            FabricException: If operation fails
        """
        wrapper = self._active_connections.get(connection_id)
        if not wrapper:
            raise FabricException(f"Invalid connection id: {connection_id}")
        
        try:
            wrapper.mark_used()
            
            if wrapper.config.db_type == 'pgvector':
                return self._execute_pgvector_operation(
                    wrapper.connection, operation, collection_name, **kwargs
                )
            elif wrapper.config.db_type == 'pinecone':
                return self._execute_pinecone_operation(
                    wrapper.connection, operation, collection_name, **kwargs
                )
                
        except Exception as e:
            wrapper.mark_failed()
            raise FabricException(f"Operation failed: {str(e)}")
    
    def _execute_pgvector_operation(self, connection: Any, operation: str,
                                  collection_name: str, **kwargs) -> Any:
        """Execute operation on pgvector database."""
        with connection.cursor() as cur:
            if operation == 'create_collection':
                dimension = kwargs['dimension']
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {collection_name} (
                        id SERIAL PRIMARY KEY,
                        vector vector({dimension}),
                        metadata JSONB
                    )
                """)
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{collection_name}_vector 
                    ON {collection_name} USING ivfflat (vector vector_l2_ops)
                """)
                connection.commit()
                
            elif operation == 'upsert':
                vectors = kwargs['vectors']
                metadata = kwargs.get('metadata', [{}] * len(vectors))
                ids = kwargs.get('ids', [None] * len(vectors))
                
                result_ids = []
                for vector, meta, id in zip(vectors, metadata, ids):
                    vector_str = f"[{','.join(map(str, vector))}]"
                    if id is None:
                        cur.execute(
                            f"""
                            INSERT INTO {collection_name} (vector, metadata)
                            VALUES (%s::vector, %s::jsonb)
                            RETURNING id
                            """,
                            (vector_str, Json(meta))
                        )
                    else:
                        cur.execute(
                            f"""
                            INSERT INTO {collection_name} (id, vector, metadata)
                            VALUES (%s, %s::vector, %s::jsonb)
                            ON CONFLICT (id) DO UPDATE
                            SET vector = EXCLUDED.vector, metadata = EXCLUDED.metadata
                            """,
                            (id, vector_str, Json(meta))
                        )
                    result_ids.append(cur.fetchone()[0])
                connection.commit()
                return result_ids
                
            elif operation == 'search':
                query_vector = kwargs['query_vector']
                k = kwargs.get('k', 10)
                filter_metadata = kwargs.get('filter_metadata')
                
                vector_str = f"[{','.join(map(str, query_vector))}]"
                filter_clause = ""
                if filter_metadata:
                    conditions = []
                    for key, value in filter_metadata.items():
                        conditions.append(f"metadata->'{key}' = '{value}'::jsonb")
                    filter_clause = "WHERE " + " AND ".join(conditions)
                
                cur.execute(f"""
                    SELECT id, metadata, vector <-> %s::vector as distance
                    FROM {collection_name}
                    {filter_clause}
                    ORDER BY distance
                    LIMIT %s
                """, (vector_str, k))
                
                return cur.fetchall()
    
    def _execute_pinecone_operation(self, client: Any, operation: str,
                                  collection_name: str, **kwargs) -> Any:
        """Execute operation on Pinecone database."""
        if operation == 'create_collection':
            dimension = kwargs['dimension']
            if collection_name not in client.list_indexes():
                client.create_index(
                    collection_name,
                    dimension=dimension,
                    metric=kwargs.get('metric', 'cosine')
                )
            
        elif operation == 'upsert':
            index = client.Index(collection_name)
            vectors = kwargs['vectors']
            metadata = kwargs.get('metadata', [{}] * len(vectors))
            ids = kwargs.get('ids', [str(i) for i in range(len(vectors))])
            
            vectors_list = [v.tolist() for v in vectors]
            upsert_data = list(zip(ids, vectors_list, metadata))
            
            batch_size = 100
            for i in range(0, len(ids), batch_size):
                batch = upsert_data[i:i + batch_size]
                index.upsert(vectors=batch)
            
            return ids
            
        elif operation == 'search':
            index = client.Index(collection_name)
            query_vector = kwargs['query_vector']
            k = kwargs.get('k', 10)
            filter_metadata = kwargs.get('filter_metadata')
            
            results = index.query(
                query_vector.tolist(),
                top_k=k,
                filter=filter_metadata
            )
            
            return [
                {
                    'id': match.id,
                    'metadata': match.metadata,
                    'distance': match.score
                }
                for match in results.matches
            ]
    
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
            average_response_time=0.0,  # We will need timing logic to implement
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
                
            # Clear pools
            self._pools.clear()
            
        except Exception as e:
            raise FabricException(f"Error during cleanup: {str(e)}")