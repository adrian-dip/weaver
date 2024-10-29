from typing import Any, Dict, List, Optional, Union, Type, Tuple
from abc import ABC, abstractmethod
import numpy as np
import yaml
from contextlib import contextmanager

from .yarn_base import YarnBase, QueryMetadata
from .yarn_exceptions import YarnConnectionError, YarnQueryError, YarnConfigError

class VectorDBManager(ABC):
    """Abstract base class for vector database connection managers."""
    
    @abstractmethod
    def connect(self) -> Any:
        """Establish connection to the vector database."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the connection is active."""
        pass
    
    @abstractmethod
    def create_collection(self, collection_name: str, dimension: int) -> None:
        """Create a new collection in the vector database."""
        pass
    
    @abstractmethod
    def delete_collection(self, collection_name: str) -> None:
        """Delete a collection from the vector database."""
        pass
    
    @abstractmethod
    def upsert_vectors(self, collection_name: str, vectors: List[np.ndarray], 
                      metadata: List[Dict[str, Any]], ids: Optional[List[str]] = None) -> List[str]:
        """Insert or update vectors in the database."""
        pass
    
    @abstractmethod
    def search_vectors(self, collection_name: str, query_vector: np.ndarray, 
                      k: int = 10, filter_metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Search for similar vectors in the database."""
        pass

class PgVectorManager(VectorDBManager):
    """PostgreSQL with pgvector extension manager."""
    
    def __init__(self, config: Dict[str, Any]):
        try:
            from psycopg2 import pool
            import psycopg2.extras
            self.pool = pool
            self.extras = psycopg2.extras
        except ImportError:
            raise YarnConnectionError("psycopg2-binary is not installed")
            
        self.config = config
        self.connection_pool = None
        
    def connect(self) -> Any:
        """Create connection pool for PostgreSQL."""
        if not self.connection_pool:
            self.connection_pool = self.pool.SimpleConnectionPool(
                minconn=self.config.get('min_connections', 1),
                maxconn=self.config.get('max_connections', 10),
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5432),
                database=self.config['database'],
                user=self.config.get('username'),
                password=self.config.get('password')
            )
        return self.connection_pool
        
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool."""
        conn = self.connection_pool.getconn()
        try:
            yield conn
        finally:
            self.connection_pool.putconn(conn)
    
    def disconnect(self) -> None:
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.connection_pool = None
            
    def is_connected(self) -> bool:
        """Check database connection status."""
        if not self.connection_pool:
            return False
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return True
        except Exception:
            return False
            
    def create_collection(self, collection_name: str, dimension: int) -> None:
        """Create a new table for vector storage."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {collection_name} (
                        id SERIAL PRIMARY KEY,
                        vector vector({dimension}),
                        metadata JSONB
                    )
                """)
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{collection_name}_vector ON {collection_name} USING ivfflat (vector vector_l2_ops)")
                conn.commit()
                
    def delete_collection(self, collection_name: str) -> None:
        """Drop the vector table."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {collection_name}")
                conn.commit()
                
    def upsert_vectors(self, collection_name: str, vectors: List[np.ndarray], 
                      metadata: List[Dict[str, Any]], ids: Optional[List[str]] = None) -> List[str]:
        """Insert or update vectors in the database."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                result_ids = []
                for i, (vector, meta) in enumerate(zip(vectors, metadata)):
                    vector_str = f"[{','.join(map(str, vector))}]"
                    cur.execute(
                        f"""
                        INSERT INTO {collection_name} (vector, metadata)
                        VALUES (%s::vector, %s::jsonb)
                        RETURNING id
                        """,
                        (vector_str, self.extras.Json(meta))
                    )
                    result_ids.append(str(cur.fetchone()[0]))
                conn.commit()
                return result_ids
                
    def search_vectors(self, collection_name: str, query_vector: np.ndarray,
                      k: int = 10, filter_metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Search for similar vectors using L2 distance."""
        vector_str = f"[{','.join(map(str, query_vector))}]"
        filter_clause = ""
        if filter_metadata:
            conditions = []
            for key, value in filter_metadata.items():
                conditions.append(f"metadata->'{key}' = '{value}'::jsonb")
            filter_clause = "WHERE " + " AND ".join(conditions)
            
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=self.extras.RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT id, metadata, vector <-> %s::vector as distance
                    FROM {collection_name}
                    {filter_clause}
                    ORDER BY distance
                    LIMIT %s
                """, (vector_str, k))
                return cur.fetchall()

class PineconeManager(VectorDBManager):    
    def __init__(self, config: Dict[str, Any]):
        try:
            import pinecone
            self.pinecone = pinecone
        except ImportError:
            raise YarnConnectionError("pinecone-client is not installed")
            
        self.config = config
        self.client = None
        
    def connect(self) -> Any:
        if not self.client:
            self.pinecone.init(
                api_key=self.config['api_key'],
                environment=self.config['environment']
            )
            self.client = self.pinecone
        return self.client
        
    def disconnect(self) -> None:
        self.client = None
        
    def is_connected(self) -> bool:
        if not self.client:
            return False
        try:
            self.client.list_indexes()
            return True
        except Exception:
            return False
            
    def create_collection(self, collection_name: str, dimension: int) -> None:
        if collection_name not in self.client.list_indexes():
            self.client.create_index(
                collection_name,
                dimension=dimension,
                metric=self.config.get('metric', 'cosine')
            )
            
    def delete_collection(self, collection_name: str) -> None:
        if collection_name in self.client.list_indexes():
            self.client.delete_index(collection_name)
            
    def upsert_vectors(self, collection_name: str, vectors: List[np.ndarray],
                      metadata: List[Dict[str, Any]], ids: Optional[List[str]] = None) -> List[str]:
        index = self.client.Index(collection_name)
        if ids is None:
            ids = [str(i) for i in range(len(vectors))]
            
        vectors_list = [v.tolist() for v in vectors]
        upsert_data = zip(ids, vectors_list, metadata)
        
        batch_size = 100
        for i in range(0, len(ids), batch_size):
            batch = list(upsert_data)[i:i + batch_size]
            index.upsert(vectors=batch)
            
        return ids
        
    def search_vectors(self, collection_name: str, query_vector: np.ndarray,
                      k: int = 10, filter_metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Search for similar vectors in Pinecone."""
        index = self.client.Index(collection_name)
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

class VectorDBYarn(YarnBase):
    """
    Vector Database Yarn implementation supporting multiple vector databases.
    Currently supports PostgreSQL with pgvector and Pinecone.
    """
    
    # Mapping of database types to their connection managers
    SUPPORTED_DATABASES = {
        'pgvector': PgVectorManager,
        'pinecone': PineconeManager
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize VectorDBYarn with configuration.
        
        Args:
            config: Dictionary containing:
                - db_type: Type of vector database (pgvector, pinecone)
                - Additional database-specific configuration
        """
        super().__init__(config)
        self.db_manager = self._create_db_manager()
        
    def _validate_config(self) -> None:
        """Validate the provided configuration."""
        if 'db_type' not in self.config:
            raise YarnConfigError("Missing required field: db_type")
            
        if self.config['db_type'] not in self.SUPPORTED_DATABASES:
            raise YarnConfigError(
                f"Unsupported database type: {self.config['db_type']}. "
                f"Supported types are: {list(self.SUPPORTED_DATABASES.keys())}"
            )
    
    def _create_db_manager(self) -> VectorDBManager:
        """Create appropriate database manager based on configuration."""
        db_type = self.config['db_type']
        manager_class = self.SUPPORTED_DATABASES[db_type]
        return manager_class(self.config)
    
    def query(self, query_template: str, params: Dict[str, Any]) -> Any:
        """
        Execute a vector database operation.
        
        Args:
            query_template: YAML-formatted operation template
            params: Operation parameters
            
        Returns:
            Operation results
            
        Raises:
            YarnQueryError: If there's an error executing the operation
        """
        self._start_query()
        try:
            # Parse operation template
            operation_dict = yaml.safe_load(query_template)
            operation = operation_dict['operation']
            
            # Execute appropriate operation
            if operation == 'create_collection':
                result = self.db_manager.create_collection(
                    operation_dict['collection_name'],
                    operation_dict['dimension']
                )
            elif operation == 'delete_collection':
                result = self.db_manager.delete_collection(
                    operation_dict['collection_name']
                )
            elif operation == 'upsert':
                result = self.db_manager.upsert_vectors(
                    operation_dict['collection_name'],
                    params['vectors'],
                    params['metadata'],
                    params.get('ids')
                )
            elif operation == 'search':
                result = self.db_manager.search_vectors(
                    operation_dict['collection_name'],
                    params['query_vector'],
                    params.get('k', 10),
                    params.get('filter_metadata')
                )
            else:
                raise YarnQueryError(f"Unsupported operation: {operation}")
                
            self._end_query()
            return result
            
        except Exception as e:
            error_msg = f"Error executing vector database operation: {str(e)}"
            self._end_query(error=error_msg)
            raise YarnQueryError(error_msg) from e
    
    def health_check(self) -> bool:
        """Check database connection health."""
        try:
            return self.db_manager.is_connected()
        except Exception:
            return False
    
    def close(self) -> None:
        """Clean up database connections."""
        if self.db_manager:
            self.db_manager.disconnect()
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'VectorDBYarn':
        """
        Create VectorDBYarn instance from YAML configuration file.
        
        Args:
            yaml_path: Path to YAML configuration file
            
        Returns:
            VectorDBYarn instance
        """
        try:
            with open(yaml_path, 'r') as f:
                config = yaml.safe_load(f)
            return cls(config)
        except Exception as e:
            raise YarnConfigError(f"Error loading YAML configuration: {str(e)}") from e