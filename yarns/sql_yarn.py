from typing import Any, Dict, Optional, List, Union, Type
from datetime import datetime
import importlib
from contextlib import contextmanager
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from .yarn_base import YarnBase, QueryMetadata
from .yarn_exceptions import YarnConnectionError, YarnQueryError, YarnConfigError

class SQLYarn(YarnBase):
    """
    SQL Yarn implementation supporting multiple SQL databases through SQLAlchemy.
    Supports PostgreSQL, MySQL, SQLite, Oracle, and MS SQL Server.
    """
    
    # Mapping of database types to their SQLAlchemy URL prefixes
    SUPPORTED_DATABASES = {
        'postgresql': 'postgresql',
        'mysql': 'mysql+pymysql',
        'sqlite': 'sqlite',
        'oracle': 'oracle+cx_oracle',
        'mssql': 'mssql+pyodbc'
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQLYarn with configuration.
        
        Args:
            config: Dictionary containing:
                - db_type: Type of database (postgresql, mysql, etc.)
                - host: Database host
                - port: Database port
                - database: Database name
                - username: Database username
                - password: Database password
                - pool_size: Connection pool size
                - max_overflow: Max number of connections above pool_size
                - pool_timeout: Timeout for getting connection from pool
                - echo: Whether to echo SQL queries (default False)
        """
        super().__init__(config)
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)
        self._current_session: Optional[Session] = None
    
    def _validate_config(self) -> None:
        """Validate the provided configuration."""
        required_fields = ['db_type', 'database']
        
        if not all(field in self.config for field in required_fields):
            raise YarnConfigError(
                f"Missing required configuration fields: "
                f"{[field for field in required_fields if field not in self.config]}"
            )
        
        if self.config['db_type'] not in self.SUPPORTED_DATABASES:
            raise YarnConfigError(
                f"Unsupported database type: {self.config['db_type']}. "
                f"Supported types are: {list(self.SUPPORTED_DATABASES.keys())}"
            )
    
    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine based on configuration."""
        try:
            db_type = self.config['db_type']
            url_prefix = self.SUPPORTED_DATABASES[db_type]
            
            if db_type == 'sqlite':
                url = f"{url_prefix}:///{self.config['database']}"
            else:
                url = (
                    f"{url_prefix}://"
                    f"{self.config.get('username', '')}:"
                    f"{self.config.get('password', '')}@"
                    f"{self.config.get('host', 'localhost')}:"
                    f"{self.config.get('port', self._get_default_port(db_type))}/"
                    f"{self.config['database']}"
                )
            
            return create_engine(
                url,
                pool_size=self.config.get('pool_size', 5),
                max_overflow=self.config.get('max_overflow', 10),
                pool_timeout=self.config.get('pool_timeout', 30),
                echo=self.config.get('echo', False)
            )
            
        except Exception as e:
            raise YarnConnectionError(f"Failed to create database engine: {str(e)}") from e
    
    def _get_default_port(self, db_type: str) -> int:
        """Get default port for database type."""
        DEFAULT_PORTS = {
            'postgresql': 5432,
            'mysql': 3306,
            'oracle': 1521,
            'mssql': 1433
        }
        return DEFAULT_PORTS.get(db_type, 5432)
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
    
    def query(self, query_template: str, params: Dict[str, Any]) -> Union[List[Dict[str, Any]], int]:
        """
        Execute a SQL query with parameters.
        
        Args:
            query_template: SQL query template
            params: Parameters to inject into the query
            
        Returns:
            List of dictionaries containing query results for SELECT queries
            Number of affected rows for INSERT/UPDATE/DELETE queries
            
        Raises:
            YarnQueryError: If there's an error executing the query
        """
        self._start_query()
        try:
            with self.session_scope() as session:
                result = session.execute(text(query_template), params)
                
                if result.returns_rows:
                    # SELECT query
                    rows = [dict(row) for row in result]
                    self._end_query(rows_affected=len(rows))
                    return rows
                else:
                    # INSERT/UPDATE/DELETE query
                    rows_affected = result.rowcount
                    self._end_query(rows_affected=rows_affected)
                    return rows_affected
                    
        except SQLAlchemyError as e:
            error_msg = f"Error executing SQL query: {str(e)}"
            self._end_query(error=error_msg)
            raise YarnQueryError(error_msg) from e
    
    def health_check(self) -> bool:
        """Check database connection health."""
        try:
            with self.session_scope() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def close(self) -> None:
        """Clean up database connections."""
        if self._current_session:
            self._current_session.close()
        if self.engine:
            self.engine.dispose()
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'SQLYarn':
        """
        Create SQLYarn instance from YAML configuration file.
        
        Args:
            yaml_path: Path to YAML configuration file
            
        Returns:
            SQLYarn instance
        """
        try:
            with open(yaml_path, 'r') as f:
                config = yaml.safe_load(f)
            return cls(config)
        except Exception as e:
            raise YarnConfigError(f"Error loading YAML configuration: {str(e)}") from e

    def execute_transaction(self, queries: List[Dict[str, Any]]) -> List[Any]:
        """
        Execute multiple queries in a single transaction.
        
        Args:
            queries: List of dictionaries containing:
                - query: Query template
                - params: Query parameters
                
        Returns:
            List of query results
        """
        results = []
        self._start_query()
        
        try:
            with self.session_scope() as session:
                for query_dict in queries:
                    result = session.execute(
                        text(query_dict['query']),
                        query_dict.get('params', {})
                    )
                    
                    if result.returns_rows:
                        results.append([dict(row) for row in result])
                    else:
                        results.append(result.rowcount)
                        
            self._end_query(rows_affected=sum(
                r if isinstance(r, int) else len(r) for r in results
            ))
            return results
            
        except SQLAlchemyError as e:
            error_msg = f"Error executing transaction: {str(e)}"
            self._end_query(error=error_msg)
            raise YarnQueryError(error_msg) from e