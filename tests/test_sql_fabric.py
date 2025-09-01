# test_sql_fabric.py
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from sql_fabric import SQLFabric, ConnectionConfig, ConnectionWrapper
from fabric_exceptions import FabricException
from fabric_base import ConnectionMetrics


class TestConnectionConfig:
    """Test suite for ConnectionConfig dataclass."""
    
    def test_connection_config_creation_full(self):
        """Test ConnectionConfig creation with all parameters."""
        config = ConnectionConfig(
            connection_string="postgresql://user:pass@localhost:5432/db",
            pool_size=20,
            max_retries=5,
            timeout=60,
            retry_interval=2
        )
        
        assert config.connection_string == "postgresql://user:pass@localhost:5432/db"
        assert config.pool_size == 20
        assert config.max_retries == 5
        assert config.timeout == 60
        assert config.retry_interval == 2
    
    def test_connection_config_creation_defaults(self):
        """Test ConnectionConfig creation with default values."""
        config = ConnectionConfig(
            connection_string="mysql://root@localhost/test"
        )
        
        assert config.connection_string == "mysql://root@localhost/test"
        assert config.pool_size == 5  # Default
        assert config.max_retries == 3  # Default
        assert config.timeout == 30  # Default
        assert config.retry_interval == 1  # Default


class TestConnectionWrapper:
    """Test suite for ConnectionWrapper class."""
    
    def test_connection_wrapper_creation(self):
        """Test ConnectionWrapper creation and initialization."""
        mock_connection = Mock()
        config = ConnectionConfig("test://connection")
        
        wrapper = ConnectionWrapper(mock_connection, config)
        
        assert wrapper.connection == mock_connection
        assert wrapper.config == config
        assert isinstance(wrapper.created_at, datetime)
        assert isinstance(wrapper.last_used, datetime)
        assert wrapper.total_operations == 0
        assert wrapper.failed_operations == 0
        
        # Timestamps should be close to current time
        time_diff = datetime.now() - wrapper.created_at
        assert time_diff.total_seconds() < 1.0
    
    def test_connection_wrapper_mark_used(self):
        """Test marking connection as used updates statistics."""
        wrapper = ConnectionWrapper(Mock(), ConnectionConfig("test://connection"))
        
        initial_operations = wrapper.total_operations
        initial_last_used = wrapper.last_used
        
        # Wait a tiny bit to ensure timestamp difference
        import time
        time.sleep(0.01)
        
        wrapper.mark_used()
        
        assert wrapper.total_operations == initial_operations + 1
        assert wrapper.last_used > initial_last_used
    
    def test_connection_wrapper_mark_failed(self):
        """Test marking connection operation as failed."""
        wrapper = ConnectionWrapper(Mock(), ConnectionConfig("test://connection"))
        
        initial_failed = wrapper.failed_operations
        
        wrapper.mark_failed()
        wrapper.mark_failed()
        
        assert wrapper.failed_operations == initial_failed + 2
    
    def test_connection_wrapper_multiple_operations(self):
        """Test multiple operations on connection wrapper."""
        wrapper = ConnectionWrapper(Mock(), ConnectionConfig("test://connection"))
        
        # Simulate multiple successful operations
        for _ in range(5):
            wrapper.mark_used()
        
        # Simulate some failures
        for _ in range(2):
            wrapper.mark_failed()
        
        assert wrapper.total_operations == 5
        assert wrapper.failed_operations == 2


class TestSQLFabric:
    """Test suite for SQLFabric class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.config = {
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://user:pass@localhost:5432/db',
                    'pool_size': 10,
                    'max_retries': 3,
                    'timeout': 30
                },
                'secondary': {
                    'connection_string': 'postgresql://user:pass@replica:5432/db',
                    'pool_size': 5,
                    'max_retries': 3,
                    'timeout': 30
                }
            }
        }
        
        self.fabric = SQLFabric(self.config)
    
    def test_sql_fabric_initialization(self):
        """Test SQLFabric initialization."""
        fabric = SQLFabric(self.config)
        
        assert fabric._config == self.config
        assert fabric._pools == {}
        assert fabric._active_connections == {}
        assert fabric._connection_counter == 0
        assert fabric._initialized is False
    
    def test_get_required_config_fields(self):
        """Test required configuration fields."""
        required_fields = self.fabric._get_required_config_fields()
        
        assert isinstance(required_fields, list)
        assert 'connection_configs' in required_fields
    
    @patch('sql_fabric.ConnectionPool')
    def test_setup_pools_success(self, mock_pool_class):
        """Test successful connection pool setup."""
        mock_pool_instance = Mock()
        mock_pool_class.return_value = mock_pool_instance
        
        self.fabric._setup_pools()
        
        # Should create pools for both connections
        assert mock_pool_class.call_count == 2
        assert len(self.fabric._pools) == 2
        assert 'primary' in self.fabric._pools
        assert 'secondary' in self.fabric._pools
        assert self.fabric._pools['primary'] == mock_pool_instance
        assert self.fabric._pools['secondary'] == mock_pool_instance
    
    @patch('sql_fabric.ConnectionPool')
    def test_setup_pools_failure(self, mock_pool_class):
        """Test connection pool setup failure."""
        mock_pool_class.side_effect = Exception("Pool creation failed")
        
        with pytest.raises(FabricException, match="Failed to setup connection pools"):
            self.fabric._setup_pools()
    
    @patch('sql_fabric.ConnectionPool')
    def test_get_connection_success(self, mock_pool_class):
        """Test successful connection acquisition."""
        # Setup mock pool
        mock_pool = Mock()
        mock_connection = Mock()
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        # Initialize fabric
        self.fabric.initialize()
        
        connection_id = self.fabric.get_connection('primary')
        
        assert isinstance(connection_id, str)
        assert connection_id.startswith('primary_')
        assert connection_id in self.fabric._active_connections
        
        # Verify connection wrapper is created
        wrapper = self.fabric._active_connections[connection_id]
        assert isinstance(wrapper, ConnectionWrapper)
        assert wrapper.connection == mock_connection
        
        # Verify pool was called
        mock_pool.acquire.assert_called_once()
    
    def test_get_connection_not_initialized(self):
        """Test get_connection when fabric not initialized."""
        # Should auto-initialize
        with patch.object(self.fabric, 'initialize') as mock_init:
            with patch.object(self.fabric, '_pools', {'default': Mock()}):
                mock_pool = Mock()
                mock_pool.acquire.return_value = Mock()
                self.fabric._pools = {'default': mock_pool}
                
                connection_id = self.fabric.get_connection('default')
                
                mock_init.assert_called_once()
                assert isinstance(connection_id, str)
    
    @patch('sql_fabric.ConnectionPool')
    def test_get_connection_unknown_pool(self, mock_pool_class):
        """Test get_connection with unknown pool name."""
        mock_pool_class.return_value = Mock()
        self.fabric.initialize()
        
        with pytest.raises(FabricException, match="Unknown connection pool: nonexistent"):
            self.fabric.get_connection('nonexistent')
    
    @patch('sql_fabric.ConnectionPool')
    def test_get_connection_pool_acquire_failure(self, mock_pool_class):
        """Test get_connection when pool acquire fails."""
        mock_pool = Mock()
        mock_pool.acquire.side_effect = Exception("Pool exhausted")
        mock_pool_class.return_value = mock_pool
        
        self.fabric.initialize()
        
        with pytest.raises(FabricException, match="Failed to get connection"):
            self.fabric.get_connection('primary')
    
    @patch('sql_fabric.ConnectionPool')
    def test_execute_operation_success(self, mock_pool_class):
        """Test successful operation execution."""
        # Setup
        mock_pool_class.return_value = Mock()
        self.fabric.initialize()
        
        # Get connection
        connection_id = self.fabric.get_connection('primary')
        
        # Setup operation
        def test_operation(connection):
            return f"Result from {connection}"
        
        # Execute operation
        result = self.fabric.execute_operation(connection_id, test_operation)
        
        # Verify result
        assert isinstance(result, str)
        assert "Result from" in result
        
        # Verify wrapper statistics updated
        wrapper = self.fabric._active_connections[connection_id]
        assert wrapper.total_operations == 1
        assert wrapper.failed_operations == 0
    
    def test_execute_operation_invalid_connection(self):
        """Test execute_operation with invalid connection ID."""
        def test_operation(connection):
            return "test"
        
        with pytest.raises(FabricException, match="Invalid connection id: invalid_id"):
            self.fabric.execute_operation('invalid_id', test_operation)
    
    @patch('sql_fabric.ConnectionPool')
    def test_execute_operation_failure(self, mock_pool_class):
        """Test operation execution failure."""
        mock_pool_class.return_value = Mock()
        self.fabric.initialize()
        
        connection_id = self.fabric.get_connection('primary')
        
        def failing_operation(connection):
            raise ValueError("Operation failed")
        
        with pytest.raises(FabricException, match="Operation failed"):
            self.fabric.execute_operation(connection_id, failing_operation)
        
        # Verify failure was recorded
        wrapper = self.fabric._active_connections[connection_id]
        assert wrapper.failed_operations == 1
    
    @patch('sql_fabric.ConnectionPool')
    def test_release_connection_success(self, mock_pool_class):
        """Test successful connection release."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        self.fabric.initialize()
        connection_id = self.fabric.get_connection('primary')
        
        # Release connection
        self.fabric.release_connection(connection_id)
        
        # Verify connection removed from active connections
        assert connection_id not in self.fabric._active_connections
        
        # Verify pool release was called
        mock_pool.release.assert_called_once()
    
    def test_release_connection_invalid_id(self):
        """Test release_connection with invalid connection ID."""
        with pytest.raises(FabricException, match="Invalid connection id: invalid"):
            self.fabric.release_connection('invalid')
    
    @patch('sql_fabric.ConnectionPool')
    def test_release_connection_pool_failure(self, mock_pool_class):
        """Test release_connection when pool release fails."""
        mock_pool = Mock()
        mock_pool.release.side_effect = Exception("Release failed")
        mock_pool_class.return_value = mock_pool
        
        self.fabric.initialize()
        connection_id = self.fabric.get_connection('primary')
        
        with pytest.raises(FabricException, match="Failed to release connection"):
            self.fabric.release_connection(connection_id)
    
    @patch('sql_fabric.ConnectionPool')
    def test_get_metrics_success(self, mock_pool_class):
        """Test getting connection metrics."""
        mock_pool_class.return_value = Mock()
        self.fabric.initialize()
        
        connection_id = self.fabric.get_connection('primary')
        
        # Simulate some operations
        wrapper = self.fabric._active_connections[connection_id]
        wrapper.mark_used()
        wrapper.mark_used()
        wrapper.mark_failed()
        
        metrics = self.fabric.get_metrics(connection_id)
        
        assert isinstance(metrics, ConnectionMetrics)
        assert metrics.total_queries == 2
        assert metrics.errors == 1
        assert isinstance(metrics.created_at, datetime)
        assert isinstance(metrics.last_used, datetime)
        assert metrics.average_response_time == 0.0  # Not implemented yet
    
    def test_get_metrics_invalid_connection(self):
        """Test get_metrics with invalid connection ID."""
        metrics = self.fabric.get_metrics('invalid_id')
        assert metrics is None
    
    @patch('sql_fabric.ConnectionPool')
    def test_health_check_success(self, mock_pool_class):
        """Test successful health check."""
        mock_pool = Mock()
        mock_pool.health_check.return_value = True
        mock_pool_class.return_value = mock_pool
        
        self.fabric.initialize()
        
        result = self.fabric.health_check()
        
        assert result is True
        # Should check all pools
        assert mock_pool.health_check.call_count == 2  # primary + secondary
    
    @patch('sql_fabric.ConnectionPool')
    def test_health_check_pool_failure(self, mock_pool_class):
        """Test health check with pool failure."""
        mock_pool = Mock()
        mock_pool.health_check.side_effect = [True, False]  # Second pool fails
        mock_pool_class.return_value = mock_pool
        
        self.fabric.initialize()
        
        result = self.fabric.health_check()
        
        assert result is False
    
    def test_health_check_not_initialized(self):
        """Test health check when not initialized."""
        result = self.fabric.health_check()
        assert result is False
    
    @patch('sql_fabric.ConnectionPool')
    def test_health_check_exception(self, mock_pool_class):
        """Test health check with exception."""
        mock_pool = Mock()
        mock_pool.health_check.side_effect = Exception("Health check failed")
        mock_pool_class.return_value = mock_pool
        
        self.fabric.initialize()
        
        result = self.fabric.health_check()
        assert result is False
    
    def test_create_connection_not_implemented(self):
        """Test that _create_connection raises NotImplementedError."""
        config = ConnectionConfig("test://connection")
        
        with pytest.raises(NotImplementedError, match="Must be implemented by specific database fabric"):
            self.fabric._create_connection(config)
    
    @patch('sql_fabric.ConnectionPool')
    def test_context_manager_exit(self, mock_pool_class):
        """Test context manager exit cleanup."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        self.fabric.initialize()
        
        # Exit context manager
        self.fabric.__exit__(None, None, None)
        
        # Verify all pools were closed
        assert mock_pool.close.call_count == 2  # primary + secondary
    
    @patch('sql_fabric.ConnectionPool')
    def test_connection_counter_increment(self, mock_pool_class):
        """Test that connection counter increments properly."""
        mock_pool = Mock()
        mock_pool.acquire.return_value = Mock()
        mock_pool_class.return_value = mock_pool
        
        self.fabric.initialize()
        
        # Get multiple connections
        conn1 = self.fabric.get_connection('primary')
        conn2 = self.fabric.get_connection('primary')
        conn3 = self.fabric.get_connection('secondary')
        
        # Verify counter incremented and IDs are unique
        assert conn1 == 'primary_0'
        assert conn2 == 'primary_1'
        assert conn3 == 'secondary_2'
        assert self.fabric._connection_counter == 3
    
    @patch('sql_fabric.ConnectionPool')
    def test_multiple_operations_on_connection(self, mock_pool_class):
        """Test multiple operations on same connection."""
        mock_pool_class.return_value = Mock()
        self.fabric.initialize()
        
        connection_id = self.fabric.get_connection('primary')
        
        # Execute multiple operations
        operations = [
            lambda conn: f"op1_{conn}",
            lambda conn: f"op2_{conn}",
            lambda conn: f"op3_{conn}",
        ]
        
        results = []
        for op in operations:
            result = self.fabric.execute_operation(connection_id, op)
            results.append(result)
        
        assert len(results) == 3
        for i, result in enumerate(results, 1):
            assert f"op{i}_" in result
        
        # Verify statistics
        wrapper = self.fabric._active_connections[connection_id]
        assert wrapper.total_operations == 3
        assert wrapper.failed_operations == 0


class TestSQLFabricEdgeCases:
    """Test edge cases and error conditions for SQLFabric."""
    
    def test_empty_connection_configs(self):
        """Test fabric with empty connection configs."""
        config = {'connection_configs': {}}
        fabric = SQLFabric(config)
        
        # Should initialize without error
        fabric.initialize()
        assert len(fabric._pools) == 0
    
    def test_connection_config_with_extra_fields(self):
        """Test connection config with extra fields."""
        config = {
            'connection_configs': {
                'primary': {
                    'connection_string': 'postgresql://test',
                    'pool_size': 10,
                    'extra_field': 'ignored',
                    'nested_extra': {'key': 'value'}
                }
            }
        }
        
        fabric = SQLFabric(config)
        
        # Should work fine, extra fields are ignored by ConnectionConfig
        with patch('sql_fabric.ConnectionPool'):
            fabric.initialize()
    
    @patch('sql_fabric.ConnectionPool')
    def test_get_connection_default_pool(self, mock_pool_class):
        """Test get_connection with default pool name."""
        # Setup config with 'default' pool
        config = {
            'connection_configs': {
                'default': {
                    'connection_string': 'postgresql://default',
                    'pool_size': 5
                }
            }
        }
        
        fabric = SQLFabric(config)
        mock_pool = Mock()
        mock_pool.acquire.return_value = Mock()
        mock_pool_class.return_value = mock_pool
        
        fabric.initialize()
        
        # Should work without specifying pool name
        connection_id = fabric.get_connection()  # Uses 'default'
        
        assert connection_id.startswith('default_')
        assert connection_id in fabric._active_connections