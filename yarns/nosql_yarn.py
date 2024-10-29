from typing import Any, Dict, List, Optional, Union, Type
from abc import ABC, abstractmethod
import importlib
from datetime import datetime
import yaml
from contextlib import contextmanager

from .yarn_base import YarnBase, QueryMetadata
from .yarn_exceptions import YarnConnectionError, YarnQueryError, YarnConfigError

class NoSQLConnectionManager(ABC):
    """Abstract base class for NoSQL database connection managers."""
    
    @abstractmethod
    def connect(self) -> Any:
        """Establish connection to the database."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the connection is active."""
        pass

class MongoDBManager(NoSQLConnectionManager):
    """MongoDB connection manager."""
    
    def __init__(self, config: Dict[str, Any]):
        try:
            from pymongo import MongoClient
            self.MongoClient = MongoClient
        except ImportError:
            raise YarnConnectionError("pymongo is not installed")
        
        self.config = config
        self.client = None
        
    def connect(self) -> Any:
        """Create MongoDB client connection."""
        if not self.client:
            connection_string = self._build_connection_string()
            self.client = self.MongoClient(
                connection_string,
                serverSelectionTimeoutMS=self.config.get('timeout', 5000)
            )
        return self.client
    
    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            
    def is_connected(self) -> bool:
        """Check MongoDB connection status."""
        if not self.client:
            return False
        try:
            self.client.admin.command('ping')
            return True
        except Exception:
            return False
            
    def _build_connection_string(self) -> str:
        """Build MongoDB connection string from config."""
        auth = ""
        if self.config.get('username') and self.config.get('password'):
            auth = f"{self.config['username']}:{self.config['password']}@"
            
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 27017)
        
        return f"mongodb://{auth}{host}:{port}"

class RedisManager(NoSQLConnectionManager):
    """Redis connection manager."""
    
    def __init__(self, config: Dict[str, Any]):
        try:
            import redis
            self.redis = redis
        except ImportError:
            raise YarnConnectionError("redis-py is not installed")
            
        self.config = config
        self.client = None
        
    def connect(self) -> Any:
        """Create Redis client connection."""
        if not self.client:
            self.client = self.redis.Redis(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 6379),
                password=self.config.get('password'),
                db=self.config.get('database', 0),
                decode_responses=True
            )
        return self.client
    
    def disconnect(self) -> None:
        """Close Redis connection."""
        if self.client:
            self.client.close()
            self.client = None
            
    def is_connected(self) -> bool:
        """Check Redis connection status."""
        if not self.client:
            return False
        try:
            return self.client.ping()
        except Exception:
            return False

class CassandraManager(NoSQLConnectionManager):
    """Cassandra connection manager."""
    
    def __init__(self, config: Dict[str, Any]):
        try:
            from cassandra.cluster import Cluster
            self.Cluster = Cluster
        except ImportError:
            raise YarnConnectionError("cassandra-driver is not installed")
            
        self.config = config
        self.cluster = None
        self.session = None
        
    def connect(self) -> Any:
        """Create Cassandra cluster connection."""
        if not self.session:
            self.cluster = self.Cluster(
                contact_points=self.config.get('hosts', ['localhost']),
                port=self.config.get('port', 9042),
                auth_provider=self._get_auth_provider()
            )
            self.session = self.cluster.connect(self.config.get('keyspace'))
        return self.session
    
    def disconnect(self) -> None:
        """Close Cassandra connection."""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        self.session = None
        self.cluster = None
            
    def is_connected(self) -> bool:
        """Check Cassandra connection status."""
        return bool(self.session and not self.cluster.is_shutdown)
    
    def _get_auth_provider(self) -> Any:
        """Configure Cassandra authentication."""
        if self.config.get('username') and self.config.get('password'):
            from cassandra.auth import PlainTextAuthProvider
            return PlainTextAuthProvider(
                username=self.config['username'],
                password=self.config['password']
            )
        return None

class NoSQLYarn(YarnBase):
    """
    NoSQL Yarn implementation supporting multiple NoSQL databases.
    Supports MongoDB, Redis, and Cassandra.
    """
    
    # Mapping of database types to their connection managers
    SUPPORTED_DATABASES = {
        'mongodb': MongoDBManager,
        'redis': RedisManager,
        'cassandra': CassandraManager
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize NoSQLYarn with configuration.
        
        Args:
            config: Dictionary containing:
                - db_type: Type of database (mongodb, redis, cassandra)
                - host/hosts: Database host(s)
                - port: Database port
                - username: Database username (optional)
                - password: Database password (optional)
                - database/keyspace: Database/keyspace name
                Additional database-specific configuration options
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
    
    def _create_db_manager(self) -> NoSQLConnectionManager:
        """Create appropriate database manager based on configuration."""
        db_type = self.config['db_type']
        manager_class = self.SUPPORTED_DATABASES[db_type]
        return manager_class(self.config)
    
    def query(self, query_template: str, params: Dict[str, Any]) -> Any:
        """
        Execute a query against the NoSQL database.
        
        Args:
            query_template: Query template (format depends on database type)
            params: Parameters to inject into the query
            
        Returns:
            Query results in a format appropriate for the database
            
        Raises:
            YarnQueryError: If there's an error executing the query
        """
        self._start_query()
        try:
            connection = self.db_manager.connect()
            
            # Execute query based on database type
            if self.config['db_type'] == 'mongodb':
                result = self._execute_mongodb_query(connection, query_template, params)
            elif self.config['db_type'] == 'redis':
                result = self._execute_redis_query(connection, query_template, params)
            elif self.config['db_type'] == 'cassandra':
                result = self._execute_cassandra_query(connection, query_template, params)
            else:
                raise YarnQueryError(f"Unsupported database type: {self.config['db_type']}")
            
            self._end_query()
            return result
            
        except Exception as e:
            error_msg = f"Error executing NoSQL query: {str(e)}"
            self._end_query(error=error_msg)
            raise YarnQueryError(error_msg) from e
    
    def _execute_mongodb_query(self, client: Any, query_template: str, params: Dict[str, Any]) -> Any:
        """Execute MongoDB query."""
        # Parse query template and parameters
        query_dict = yaml.safe_load(query_template)
        collection = client[self.config['database']][query_dict['collection']]
        
        operation = query_dict['operation']
        if operation == 'find':
            return list(collection.find(params))
        elif operation == 'insert':
            return collection.insert_many(params).inserted_ids
        elif operation == 'update':
            return collection.update_many(params['filter'], params['update']).modified_count
        elif operation == 'delete':
            return collection.delete_many(params).deleted_count
        else:
            raise YarnQueryError(f"Unsupported MongoDB operation: {operation}")
    
    def _execute_redis_query(self, client: Any, query_template: str, params: Dict[str, Any]) -> Any:
        """Execute Redis query."""
        # Parse command and arguments
        command_parts = query_template.strip().split()
        command = command_parts[0].lower()
        
        # Replace parameter placeholders
        args = [params.get(arg[1:], arg) if arg.startswith(':') else arg 
               for arg in command_parts[1:]]
        
        return getattr(client, command)(*args)
    
    def _execute_cassandra_query(self, session: Any, query_template: str, params: Dict[str, Any]) -> Any:
        """Execute Cassandra query."""
        # Execute prepared statement
        prepared = session.prepare(query_template)
        return session.execute(prepared, params)
    
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
    def from_yaml(cls, yaml_path: str) -> 'NoSQLYarn':
        """
        Create NoSQLYarn instance from YAML configuration file.
        
        Args:
            yaml_path: Path to YAML configuration file
            
        Returns:
            NoSQLYarn instance
        """
        try:
            with open(yaml_path, 'r') as f:
                config = yaml.safe_load(f)
            return cls(config)
        except Exception as e:
            raise YarnConfigError(f"Error loading YAML configuration: {str(e)}") from e