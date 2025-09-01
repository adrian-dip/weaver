# test_fabric_factory.py
import pytest
from unittest.mock import Mock, MagicMock, patch, mock_open
from pathlib import Path
import yaml
import tempfile
import os

from fabric_factory import FabricFactory
from fabric_base import FabricBase
from fabric_exceptions import FabricConfigError
from sql_fabric import SQLFabric
from nosql_fabric import NoSQLFabric
from vector_db_fabric import VectorDBFabric
from api_fabric import APIFabric


class MockFabric(FabricBase):
    """Mock fabric for testing."""
    
    def __init__(self, config):
        super().__init__(config)
        self.initialized = False
    
    def get_connection(self):
        return "mock_connection"
    
    def release_connection(self, connection):
        pass
    
    def get_api_client(self):
        return "mock_client"
    
    def health_check(self):
        return True
    
    def initialize(self):
        self.initialized = True
    
    def _get_required_config_fields(self):
        return ["connection_configs"]
    
    def _setup_pools(self):
        pass


class TestFabricFactory:
    """Test suite for FabricFactory class."""
    
    def test_fabric_registry_default_types(self):
        """Test that default fabric types are registered."""
        expected_types = ['sql', 'nosql', 'vector_db', 'api']
        
        for fabric_type in expected_types:
            assert fabric_type in FabricFactory.FABRIC_REGISTRY
        
        assert FabricFactory.FABRIC_REGISTRY['sql'] == SQLFabric
        assert FabricFactory.FABRIC_REGISTRY['nosql'] == NoSQLFabric
        assert FabricFactory.FABRIC_REGISTRY['vector_db'] == VectorDBFabric
        assert FabricFactory.FABRIC_REGISTRY['api'] == APIFabric
    
    @patch('fabric_factory.SQLFabric')
    def test_create_fabric_sql_success(self, mock_sql_fabric):
        """Test successful SQL fabric creation."""
        mock_fabric_instance = Mock()
        mock_fabric_instance.initialize = Mock()
        mock_sql_fabric.return_value = mock_fabric_instance
        
        config = {
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://user:pass@localhost/db',
                    'pool_size': 10
                }
            }
        }
        
        result = FabricFactory.create_fabric('sql', config)
        
        assert result == mock_fabric_instance
        mock_sql_fabric.assert_called_once_with(config)
        mock_fabric_instance.initialize.assert_called_once()
    
    @patch('fabric_factory.NoSQLFabric')
    def test_create_fabric_nosql_success(self, mock_nosql_fabric):
        """Test successful NoSQL fabric creation."""
        mock_fabric_instance = Mock()
        mock_fabric_instance.initialize = Mock()
        mock_nosql_fabric.return_value = mock_fabric_instance
        
        config = {
            'connection_configs': {
                'mongodb_primary': {
                    'db_type': 'mongodb',
                    'hosts': ['localhost'],
                    'port': 27017,
                    'pool_size': 5
                }
            }
        }
        
        result = FabricFactory.create_fabric('nosql', config)
        
        assert result == mock_fabric_instance
        mock_nosql_fabric.assert_called_once_with(config)
        mock_fabric_instance.initialize.assert_called_once()
    
    def test_create_fabric_unsupported_type(self):
        """Test error when creating unsupported fabric type."""
        config = {'connection_configs': {}}
        
        with pytest.raises(FabricConfigError, match="Unsupported Fabric type: unsupported"):
            FabricFactory.create_fabric('unsupported', config)
    
    @patch('fabric_factory.FabricFactory._validate_fabric_config')
    def test_create_fabric_validation_failure(self, mock_validate):
        """Test fabric creation failure during validation."""
        mock_validate.side_effect = FabricConfigError("Invalid config")
        config = {'connection_configs': {}}
        
        with pytest.raises(FabricConfigError, match="Invalid config"):
            FabricFactory.create_fabric('sql', config)
    
    @patch('fabric_factory.SQLFabric')
    @patch('fabric_factory.FabricFactory._validate_fabric_config')
    def test_create_fabric_initialization_failure(self, mock_validate, mock_sql_fabric):
        """Test fabric creation failure during initialization."""
        mock_fabric_instance = Mock()
        mock_fabric_instance.initialize.side_effect = Exception("Init failed")
        mock_sql_fabric.return_value = mock_fabric_instance
        
        config = {'connection_configs': {}}
        
        with pytest.raises(FabricConfigError, match="Error creating sql fabric: Init failed"):
            FabricFactory.create_fabric('sql', config)
    
    def test_validate_fabric_config_missing_common_fields(self):
        """Test validation failure for missing common required fields."""
        config = {'some_other_field': 'value'}
        
        with pytest.raises(FabricConfigError, match="Missing required configuration fields: connection_configs"):
            FabricFactory._validate_fabric_config('sql', config)
    
    def test_validate_fabric_config_invalid_connection_configs_type(self):
        """Test validation failure for invalid connection_configs type."""
        config = {'connection_configs': 'not_a_dict'}
        
        with pytest.raises(FabricConfigError, match="connection_configs must be a dictionary"):
            FabricFactory._validate_fabric_config('sql', config)
    
    def test_validate_fabric_config_sql_missing_fields(self):
        """Test validation failure for SQL fabric missing required fields."""
        config = {
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://...'
                    # Missing pool_size
                }
            }
        }
        
        with pytest.raises(FabricConfigError, match="Connection 'primary' missing required fields: pool_size"):
            FabricFactory._validate_fabric_config('sql', config)
    
    def test_validate_fabric_config_nosql_missing_fields(self):
        """Test validation failure for NoSQL fabric missing required fields."""
        config = {
            'connection_configs': {
                'mongo': {
                    'db_type': 'mongodb'
                    # Missing hosts
                }
            }
        }
        
        with pytest.raises(FabricConfigError, match="Connection 'mongo' missing required fields: hosts"):
            FabricFactory._validate_fabric_config('nosql', config)
    
    def test_validate_fabric_config_vector_db_success(self):
        """Test successful validation for vector DB fabric."""
        config = {
            'connection_configs': {
                'pinecone': {
                    'db_type': 'pinecone',
                    'dimension': 1536,
                    'hosts': ['api.pinecone.io']
                }
            }
        }
        
        # Should not raise any exception
        FabricFactory._validate_fabric_config('vector_db', config)
    
    def test_validate_fabric_config_api_success(self):
        """Test successful validation for API fabric."""
        config = {
            'connection_configs': {
                'rest_api': {
                    'api_type': 'rest',
                    'base_url': 'https://api.example.com'
                }
            }
        }
        
        # Should not raise any exception
        FabricFactory._validate_fabric_config('api', config)
    
    def test_validate_fabric_config_multiple_connections(self):
        """Test validation with multiple connections."""
        config = {
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://primary',
                    'pool_size': 10
                },
                'secondary': {
                    'connection_string': 'postgresql://secondary',
                    'pool_size': 5
                },
                'cache': {
                    'connection_string': 'redis://cache',
                    'pool_size': 3
                }
            }
        }
        
        # Should not raise any exception
        FabricFactory._validate_fabric_config('sql', config)
    
    def test_from_yaml_file_not_found(self):
        """Test error when YAML file not found."""
        with pytest.raises(FabricConfigError, match="Configuration file not found: nonexistent.yaml"):
            FabricFactory.from_yaml("nonexistent.yaml")
    
    def test_from_yaml_invalid_yaml(self):
        """Test error when YAML file contains invalid YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [unclosed")
            temp_file = f.name
        
        try:
            with pytest.raises(FabricConfigError, match="Error parsing YAML configuration"):
                FabricFactory.from_yaml(temp_file)
        finally:
            os.unlink(temp_file)
    
    def test_from_yaml_missing_fabric_type(self):
        """Test error when YAML missing fabric_type."""
        yaml_content = {
            'connection_configs': {
                'test': {'connection_string': 'test', 'pool_size': 1}
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_file = f.name
        
        try:
            with pytest.raises(FabricConfigError, match="Fabric type must be specified"):
                FabricFactory.from_yaml(temp_file)
        finally:
            os.unlink(temp_file)
    
    @patch('fabric_factory.FabricFactory.create_fabric')
    def test_from_yaml_success_with_fabric_type_in_config(self, mock_create):
        """Test successful YAML loading with fabric_type in config."""
        mock_fabric = Mock()
        mock_create.return_value = mock_fabric
        
        yaml_content = {
            'fabric_type': 'sql',
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://test',
                    'pool_size': 10
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_file = f.name
        
        try:
            result = FabricFactory.from_yaml(temp_file)
            
            assert result == mock_fabric
            mock_create.assert_called_once_with('sql', yaml_content)
        finally:
            os.unlink(temp_file)
    
    @patch('fabric_factory.FabricFactory.create_fabric')
    def test_from_yaml_success_with_fabric_type_override(self, mock_create):
        """Test successful YAML loading with fabric_type override."""
        mock_fabric = Mock()
        mock_create.return_value = mock_fabric
        
        yaml_content = {
            'fabric_type': 'nosql',  # This should be overridden
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://test',
                    'pool_size': 10
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_file = f.name
        
        try:
            result = FabricFactory.from_yaml(temp_file, fabric_type='sql')
            
            assert result == mock_fabric
            mock_create.assert_called_once_with('sql', yaml_content)  # Override used
        finally:
            os.unlink(temp_file)
    
    @patch('fabric_factory.FabricFactory.create_fabric')
    def test_create_multiple_success(self, mock_create):
        """Test successful creation of multiple fabrics."""
        mock_sql_fabric = Mock()
        mock_nosql_fabric = Mock()
        mock_create.side_effect = [mock_sql_fabric, mock_nosql_fabric]
        
        configs = {
            'database': {
                'fabric_type': 'sql',
                'connection_configs': {
                    'primary': {
                        'connection_string': 'postgresql://db',
                        'pool_size': 10
                    }
                }
            },
            'cache': {
                'fabric_type': 'nosql',
                'connection_configs': {
                    'redis': {
                        'db_type': 'redis',
                        'hosts': ['localhost']
                    }
                }
            }
        }
        
        result = FabricFactory.create_multiple(configs)
        
        assert len(result) == 2
        assert result['database'] == mock_sql_fabric
        assert result['cache'] == mock_nosql_fabric
        assert mock_create.call_count == 2
    
    def test_create_multiple_missing_fabric_type(self):
        """Test error when fabric_type missing in multiple creation."""
        configs = {
            'invalid': {
                'connection_configs': {}
                # Missing fabric_type
            }
        }
        
        with pytest.raises(FabricConfigError, match="Missing fabric_type in configuration for invalid"):
            FabricFactory.create_multiple(configs)
    
    @patch('fabric_factory.FabricFactory.create_fabric')
    def test_create_multiple_partial_failure_cleanup(self, mock_create):
        """Test cleanup when partial failure occurs in multiple creation."""
        mock_fabric1 = Mock()
        mock_fabric1.close = Mock()
        mock_create.side_effect = [mock_fabric1, Exception("Creation failed")]
        
        configs = {
            'fabric1': {
                'fabric_type': 'sql',
                'connection_configs': {}
            },
            'fabric2': {
                'fabric_type': 'nosql',
                'connection_configs': {}
            }
        }
        
        with pytest.raises(FabricConfigError, match="Error creating multiple Fabrics"):
            FabricFactory.create_multiple(configs)
        
        # First fabric should be cleaned up
        mock_fabric1.close.assert_called_once()
    
    def test_register_fabric_success(self):
        """Test successful registration of new fabric type."""
        original_registry = FabricFactory.FABRIC_REGISTRY.copy()
        
        try:
            FabricFactory.register_fabric('mock', MockFabric)
            
            assert 'mock' in FabricFactory.FABRIC_REGISTRY
            assert FabricFactory.FABRIC_REGISTRY['mock'] == MockFabric
        finally:
            # Restore original registry
            FabricFactory.FABRIC_REGISTRY.clear()
            FabricFactory.FABRIC_REGISTRY.update(original_registry)
    
    def test_register_fabric_already_exists(self):
        """Test error when registering fabric type that already exists."""
        with pytest.raises(ValueError, match="Fabric type already registered: sql"):
            FabricFactory.register_fabric('sql', MockFabric)
    
    def test_register_fabric_not_subclass(self):
        """Test error when registering class that doesn't inherit from FabricBase."""
        class NotAFabric:
            pass
        
        with pytest.raises(TypeError, match="Fabric class must inherit from FabricBase"):
            FabricFactory.register_fabric('invalid', NotAFabric)
    
    def test_get_supported_types(self):
        """Test getting supported fabric types information."""
        supported_types = FabricFactory.get_supported_types()
        
        assert isinstance(supported_types, dict)
        assert len(supported_types) == 4
        
        expected_types = ['sql', 'nosql', 'vector_db', 'api']
        for fabric_type in expected_types:
            assert fabric_type in supported_types
            assert isinstance(supported_types[fabric_type], str)
            assert len(supported_types[fabric_type]) > 0
        
        # Check specific descriptions contain expected keywords
        assert 'SQL database' in supported_types['sql']
        assert 'connection pooling' in supported_types['sql']
        assert 'NoSQL database' in supported_types['nosql']
        assert 'MongoDB' in supported_types['nosql']
        assert 'Vector database' in supported_types['vector_db']
        assert 'pgvector' in supported_types['vector_db']
        assert 'API integration' in supported_types['api']
        assert 'REST' in supported_types['api']
    
    @patch('fabric_factory.FabricFactory.create_fabric')
    def test_integration_with_real_yaml_file(self, mock_create):
        """Test integration with a realistic YAML configuration file."""
        mock_fabric = Mock()
        mock_create.return_value = mock_fabric
        
        yaml_content = {
            'fabric_type': 'sql',
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://user:password@localhost:5432/mydb',
                    'pool_size': 20,
                    'max_retries': 3,
                    'timeout': 30
                },
                'replica': {
                    'connection_string': 'postgresql://user:password@replica:5432/mydb',
                    'pool_size': 10,
                    'max_retries': 3,
                    'timeout': 30
                }
            },
            'monitoring': {
                'enabled': True,
                'metrics_interval': 60
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_file = f.name
        
        try:
            result = FabricFactory.from_yaml(temp_file)
            
            assert result == mock_fabric
            mock_create.assert_called_once_with('sql', yaml_content)
        finally:
            os.unlink(temp_file)


class TestFabricFactoryEdgeCases:
    """Test edge cases and error conditions for FabricFactory."""
    
    def test_empty_connection_configs(self):
        """Test behavior with empty connection_configs."""
        config = {'connection_configs': {}}
        
        # Should pass validation but might fail during fabric creation
        # depending on specific fabric implementation
        FabricFactory._validate_fabric_config('sql', config)
    
    def test_validate_unknown_fabric_type(self):
        """Test validation with unknown fabric type."""
        config = {
            'connection_configs': {
                'test': {'some_field': 'value'}
            }
        }
        
        # Should not raise exception for unknown types (no type-specific validation)
        FabricFactory._validate_fabric_config('unknown_type', config)
    
    def test_create_fabric_with_extra_config_fields(self):
        """Test fabric creation with extra configuration fields."""
        # This should work fine, extra fields should be ignored by validation
        config = {
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://test',
                    'pool_size': 10,
                    'extra_field': 'value',
                    'another_extra': {'nested': 'data'}
                }
            },
            'extra_top_level': 'ignored'
        }
        
        # Should pass validation
        FabricFactory._validate_fabric_config('sql', config)
    
    @patch('builtins.open')
    @patch('pathlib.Path.exists')
    def test_from_yaml_file_read_error(self, mock_exists, mock_open_func):
        """Test error handling when file cannot be read."""
        mock_exists.return_value = True
        mock_open_func.side_effect = IOError("Permission denied")
        
        with pytest.raises(FabricConfigError, match="Error creating Fabric instance"):
            FabricFactory.from_yaml("restricted_file.yaml")
    
    def test_validation_with_nested_connection_configs(self):
        """Test validation with deeply nested connection configurations."""
        config = {
            'connection_configs': {
                'complex': {
                    'connection_string': 'postgresql://test',
                    'pool_size': 10,
                    'nested_config': {
                        'ssl_config': {
                            'cert_file': '/path/to/cert',
                            'key_file': '/path/to/key',
                            'ca_file': '/path/to/ca'
                        },
                        'retry_policy': {
                            'max_retries': 5,
                            'backoff_factor': 2.0,
                            'retry_codes': [500, 502, 503, 504]
                        }
                    }
                }
            }
        }
        
        # Should handle nested configurations without issues
        FabricFactory._validate_fabric_config('sql', config)
    
    @patch('fabric_factory.FabricFactory.create_fabric')
    def test_create_multiple_empty_configs(self, mock_create):
        """Test create_multiple with empty configs dictionary."""
        result = FabricFactory.create_multiple({})
        
        assert result == {}
        mock_create.assert_not_called()