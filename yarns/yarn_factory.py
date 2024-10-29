from typing import Dict, Any, Type, Optional
import yaml
from pathlib import Path

from .yarn_base import YarnBase
from .sql_yarn import SQLYarn
from .nosql_yarn import NoSQLYarn
from .vector_db_yarn import VectorDBYarn
from .api_yarn import APIYarn
from .yarn_exceptions import YarnConfigError

class YarnFactory:
    """
    Factory class for creating Yarn instances.
    Manages the creation of different types of Yarns (SQL, NoSQL, Vector DB, API)
    based on configuration.
    """
    
    # Registry mapping yarn types to their implementing classes
    YARN_REGISTRY: Dict[str, Type[YarnBase]] = {
        'sql': SQLYarn,
        'nosql': NoSQLYarn,
        'vector_db': VectorDBYarn,
        'api': APIYarn
    }
    
    @classmethod
    def create_yarn(cls, yarn_type: str, config: Dict[str, Any]) -> YarnBase:
        """
        Create a new Yarn instance of the specified type.
        
        Args:
            yarn_type: Type of Yarn to create (sql, nosql, vector_db, api)
            config: Configuration dictionary for the Yarn
            
        Returns:
            Configured Yarn instance
            
        Raises:
            YarnConfigError: If yarn_type is not supported or configuration is invalid
        """
        if yarn_type not in cls.YARN_REGISTRY:
            raise YarnConfigError(
                f"Unsupported Yarn type: {yarn_type}. "
                f"Supported types are: {list(cls.YARN_REGISTRY.keys())}"
            )
            
        yarn_class = cls.YARN_REGISTRY[yarn_type]
        return yarn_class(config)
    
    @classmethod
    def from_yaml(cls, yaml_path: str, yarn_type: Optional[str] = None) -> YarnBase:
        """
        Create a Yarn instance from a YAML configuration file.
        
        Args:
            yaml_path: Path to the YAML configuration file
            yarn_type: Optional yarn type override. If not provided, 
                      will be read from the configuration file
            
        Returns:
            Configured Yarn instance
            
        Raises:
            YarnConfigError: If configuration file is invalid or missing required fields
        """
        try:
            config_path = Path(yaml_path)
            if not config_path.exists():
                raise YarnConfigError(f"Configuration file not found: {yaml_path}")
                
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            # Use provided yarn_type or get from config
            yarn_type = yarn_type or config.get('yarn_type')
            if not yarn_type:
                raise YarnConfigError("Yarn type must be specified either in config or as parameter")
                
            return cls.create_yarn(yarn_type, config)
            
        except yaml.YAMLError as e:
            raise YarnConfigError(f"Error parsing YAML configuration: {str(e)}")
        except Exception as e:
            raise YarnConfigError(f"Error creating Yarn instance: {str(e)}")
    
    @classmethod
    def register_yarn(cls, yarn_type: str, yarn_class: Type[YarnBase]) -> None:
        """
        Register a new Yarn type.
        Allows extending the factory with custom Yarn implementations.
        
        Args:
            yarn_type: Identifier for the new Yarn type
            yarn_class: Class implementing the new Yarn type
            
        Raises:
            ValueError: If yarn_type is already registered
            TypeError: If yarn_class doesn't inherit from YarnBase
        """
        if yarn_type in cls.YARN_REGISTRY:
            raise ValueError(f"Yarn type already registered: {yarn_type}")
            
        if not issubclass(yarn_class, YarnBase):
            raise TypeError("Yarn class must inherit from YarnBase")
            
        cls.YARN_REGISTRY[yarn_type] = yarn_class
    
    @classmethod
    def create_multiple(cls, configs: Dict[str, Dict[str, Any]]) -> Dict[str, YarnBase]:
        """
        Create multiple Yarn instances from a dictionary of configurations.
        
        Args:
            configs: Dictionary mapping Yarn identifiers to their configurations
                    Each configuration must include a 'yarn_type' field
                    
        Returns:
            Dictionary mapping Yarn identifiers to their instances
            
        Raises:
            YarnConfigError: If any configuration is invalid
        """
        yarns = {}
        try:
            for yarn_id, config in configs.items():
                if 'yarn_type' not in config:
                    raise YarnConfigError(f"Missing yarn_type in configuration for {yarn_id}")
                    
                yarn_type = config['yarn_type']
                yarns[yarn_id] = cls.create_yarn(yarn_type, config)
                
            return yarns
            
        except Exception as e:
            # Clean up any created Yarns if there's an error
            for yarn in yarns.values():
                yarn.close()
            raise YarnConfigError(f"Error creating multiple Yarns: {str(e)}")
    
    @classmethod
    def get_supported_types(cls) -> Dict[str, str]:
        """
        Get information about supported Yarn types.
        
        Returns:
            Dictionary mapping Yarn types to their descriptions
        """
        return {
            'sql': "SQL database connections supporting PostgreSQL, MySQL, SQLite, Oracle, and MS SQL Server",
            'nosql': "NoSQL database connections supporting MongoDB, Redis, and Cassandra",
            'vector_db': "Vector database connections supporting pgvector and Pinecone",
            'api': "API connections supporting REST and GraphQL APIs"
        }
