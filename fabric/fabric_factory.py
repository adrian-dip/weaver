from typing import Dict, Any, Type, Optional, List
import yaml
from pathlib import Path

from .fabric_base import FabricBase
from .sql_fabric import SQLFabric
from .nosql_fabric import NoSQLFabric
from .vector_db_fabric import VectorDBFabric
from .api_fabric import APIFabric
from .fabric_exceptions import FabricConfigError
from .connection_pool import ConnectionPool

class FabricFactory:
    """
    Factory class for creating Fabric instances.
    Manages the creation and configuration of different types of Fabrics
    (SQL, NoSQL, Vector DB, API) for database and API integration.
    """
    
    # Registry mapping fabric types to their implementing classes
    FABRIC_REGISTRY: Dict[str, Type[FabricBase]] = {
        'sql': SQLFabric,
        'nosql': NoSQLFabric,
        'vector_db': VectorDBFabric,
        'api': APIFabric
    }
    
    @classmethod
    def create_fabric(cls, fabric_type: str, config: Dict[str, Any]) -> FabricBase:
        """
        Create a new Fabric instance of the specified type.
        
        Args:
            fabric_type: Type of Fabric to create (sql, nosql, vector_db, api)
            config: Configuration dictionary for the Fabric
            
        Returns:
            Configured Fabric instance
            
        Raises:
            FabricConfigError: If fabric_type is not supported or configuration is invalid
        """
        if fabric_type not in cls.FABRIC_REGISTRY:
            raise FabricConfigError(
                f"Unsupported Fabric type: {fabric_type}. "
                f"Supported types are: {list(cls.FABRIC_REGISTRY.keys())}"
            )
        
        try:
            # Validate configuration
            cls._validate_fabric_config(fabric_type, config)
            
            # Create and initialize fabric instance
            fabric_class = cls.FABRIC_REGISTRY[fabric_type]
            fabric = fabric_class(config)
            fabric.initialize()
            
            return fabric
            
        except Exception as e:
            raise FabricConfigError(f"Error creating {fabric_type} fabric: {str(e)}")
    
    @classmethod
    def from_yaml(cls, yaml_path: str, fabric_type: Optional[str] = None) -> FabricBase:
        """
        Create a Fabric instance from a YAML configuration file.
        
        Args:
            yaml_path: Path to the YAML configuration file
            fabric_type: Optional fabric type override. If not provided, 
                        will be read from the configuration file
            
        Returns:
            Configured Fabric instance
            
        Raises:
            FabricConfigError: If configuration file is invalid or missing required fields
        """
        try:
            config_path = Path(yaml_path)
            if not config_path.exists():
                raise FabricConfigError(f"Configuration file not found: {yaml_path}")
                
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            # Use provided fabric_type or get from config
            fabric_type = fabric_type or config.get('fabric_type')
            if not fabric_type:
                raise FabricConfigError(
                    "Fabric type must be specified either in config or as parameter"
                )
                
            return cls.create_fabric(fabric_type, config)
            
        except yaml.YAMLError as e:
            raise FabricConfigError(f"Error parsing YAML configuration: {str(e)}")
        except Exception as e:
            raise FabricConfigError(f"Error creating Fabric instance: {str(e)}")
    
    @classmethod
    def create_multiple(cls, configs: Dict[str, Dict[str, Any]]) -> Dict[str, FabricBase]:
        """
        Create multiple Fabric instances from a dictionary of configurations.
        
        Args:
            configs: Dictionary mapping Fabric identifiers to their configurations
                    Each configuration must include a 'fabric_type' field
                    
        Returns:
            Dictionary mapping Fabric identifiers to their instances
            
        Raises:
            FabricConfigError: If any configuration is invalid
        """
        fabrics = {}
        try:
            for fabric_id, config in configs.items():
                if 'fabric_type' not in config:
                    raise FabricConfigError(
                        f"Missing fabric_type in configuration for {fabric_id}"
                    )
                    
                fabric_type = config['fabric_type']
                fabrics[fabric_id] = cls.create_fabric(fabric_type, config)
                
            return fabrics
            
        except Exception as e:
            # Clean up any created Fabrics if there's an error
            for fabric in fabrics.values():
                fabric.close()
            raise FabricConfigError(f"Error creating multiple Fabrics: {str(e)}")
    
    @classmethod
    def register_fabric(cls, fabric_type: str, fabric_class: Type[FabricBase]) -> None:
        """
        Register a new Fabric type.
        Allows extending the factory with custom Fabric implementations.
        
        Args:
            fabric_type: Identifier for the new Fabric type
            fabric_class: Class implementing the new Fabric type
            
        Raises:
            ValueError: If fabric_type is already registered
            TypeError: If fabric_class doesn't inherit from FabricBase
        """
        if fabric_type in cls.FABRIC_REGISTRY:
            raise ValueError(f"Fabric type already registered: {fabric_type}")
            
        if not issubclass(fabric_class, FabricBase):
            raise TypeError("Fabric class must inherit from FabricBase")
            
        cls.FABRIC_REGISTRY[fabric_type] = fabric_class
    
    @classmethod
    def _validate_fabric_config(cls, fabric_type: str, config: Dict[str, Any]) -> None:
        """
        Validate fabric configuration based on type-specific requirements.
        
        Args:
            fabric_type: Type of Fabric being configured
            config: Configuration dictionary to validate
            
        Raises:
            FabricConfigError: If configuration is invalid
        """
        common_required = {'connection_configs'}
        
        type_specific_required = {
            'sql': {'connection_string', 'pool_size'},
            'nosql': {'db_type', 'hosts'},
            'vector_db': {'db_type', 'dimension'},
            'api': {'api_type', 'base_url'}
        }
        
        # Validate common requirements
        missing_common = common_required - set(config.keys())
        if missing_common:
            raise FabricConfigError(
                f"Missing required configuration fields: {', '.join(missing_common)}"
            )
        
        # Validate connection configs
        conn_configs = config.get('connection_configs', {})
        if not isinstance(conn_configs, dict):
            raise FabricConfigError("connection_configs must be a dictionary")
            
        # Validate type-specific requirements for each connection
        required_fields = type_specific_required.get(fabric_type, set())
        for conn_name, conn_config in conn_configs.items():
            missing_fields = required_fields - set(conn_config.keys())
            if missing_fields:
                raise FabricConfigError(
                    f"Connection '{conn_name}' missing required fields: {', '.join(missing_fields)}"
                )
    
    @classmethod
    def get_supported_types(cls) -> Dict[str, str]:
        """
        Get information about supported Fabric types.
        
        Returns:
            Dictionary mapping Fabric types to their descriptions
        """
        return {
            'sql': (
                "SQL database integration supporting connection pooling, "
                "monitoring, and transaction management"
            ),
            'nosql': (
                "NoSQL database integration supporting MongoDB, Redis, and "
                "Cassandra with connection management"
            ),
            'vector_db': (
                "Vector database integration supporting pgvector and Pinecone "
                "with specialized vector operations"
            ),
            'api': (
                "API integration supporting REST and GraphQL with rate limiting "
                "and request management"
            )
        }