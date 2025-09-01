# test_nosql_fabric.py
import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from datetime import datetime, timedelta
from nosql_fabric import NoSQLFabric, NoSQLConnectionConfig, NoSQLConnectionWrapper
from fabric_exceptions import FabricException
from fabric_base import ConnectionMetrics


class TestNoSQLConnectionConfig:
    """Test suite for NoSQLConnectionConfig dataclass."""
    
    def test_nosql_connection_config_creation_full(self):
        """Test NoSQLConnectionConfig creation with all parameters."""
        config = NoSQLConnectionConfig(
            db_type="mongodb",
            hosts=["localhost", "replica1", "replica2"],
            port=27017,
            username="admin",
            password="secret",
            database="test_db",
            keyspace="test_keyspace",
            pool_size=15,
            max_retries=5,
            timeout=45,
            retry_interval=2
        )
        
        assert config.db_type == "mongodb"
        assert config.hosts == ["localhost", "replica1", "replica2"]
        assert config.port == 27017
        assert config.username == "admin"
        assert config.password == "secret"
        assert config.database == "test_db"
        assert config.keyspace == "test_keyspace"
        assert config.pool_size == 15
        assert config.max_retries == 5
        assert config.timeout == 45
        assert config.retry_interval == 2
    
    def test_nosql_connection_config_creation_minimal(self):
        """Test NoSQLConnectionConfig creation with minimal parameters."""
        config = NoSQLConnectionConfig(
            db_type="redis",
            hosts=["127.0.0.1"],
            port=6379
        )
        
        assert config.db_type == "redis"
        assert config.hosts == ["127.0.0.1"]
        assert config.port == 6379
        # Test defaults
        assert config.username is None
        assert config.password is None
        assert config.database is None
        assert config.keyspace is None
        assert config.pool_size == 5
        assert config.max_retries == 3
        assert config.timeout == 30
        assert config.retry_interval == 1
    
    def test_nosql_connection_config_cassandra_keyspace(self):
        """Test NoSQLConnectionConfig for Cassandra with keyspace."""
        config = NoSQLConnectionConfig(
            db_type="cassandra",
            hosts=["cassandra1", "cassandra2", "cassandra3"],
            port=9042,
            username="cassandra",
            password="cassandra",
            keyspace="analytics"
        )
        
        assert config.db_type == "cassandra"
        assert len(config.hosts) == 3
        assert config.keyspace == "analytics"
        assert config.database is None  # Cassandra uses keyspace, not database


class TestNoSQLConnectionWrapper:
    """Test suite for NoSQLConnectionWrapper class."""
    
    def test_nosql_connection_wrapper_creation(self):
        """Test NoSQLConnectionWrapper creation and initialization."""
        mock_connection = Mock()
        config = NoSQLConnectionConfig("mongodb", ["localhost"], 27017)
        
        wrapper = NoSQLConnectionWrapper(mock_connection, config)
        
        assert wrapper.connection == mock_connection
        assert wrapper.config == config
        assert isinstance(wrapper.created_at, datetime)
        assert isinstance(wrapper.last_used, datetime)
        assert wrapper.total_operations == 0
        assert wrapper.failed_operations == 0
        assert wrapper.is_closed is False
        
        # Verify timestamps are recent
        time_diff = datetime.now() - wrapper.created_at
        assert time_diff.total_seconds() < 1.0
    
    def test_nosql_connection_wrapper_mark_used(self):
        """Test marking connection as used updates statistics."""
        wrapper = NoSQLConnectionWrapper(
            Mock(), 
            NoSQLConnectionConfig("redis", ["localhost"], 6379)
        )
        
        initial_operations = wrapper.total_operations
        initial_last_used = wrapper.last_used
        
        import time
        time.sleep(0.01)
        
        wrapper.mark_used()
        
        assert wrapper.total_operations == initial_operations + 1
        assert wrapper.last_used > initial_last_used
    
    def test_nosql_connection_wrapper_mark_failed(self):
        """Test marking connection operation as failed."""
        wrapper = NoSQLConnectionWrapper(
            Mock(), 
            NoSQLConnectionConfig("cassandra", ["localhost"], 9042)
        )
        
        initial_failed = wrapper.failed_operations
        
        wrapper.mark_failed()
        wrapper.mark_failed()
        wrapper.mark_failed()
        
        assert wrapper.failed_operations == initial_failed + 3
    
    def test_nosql_connection_wrapper_close_with_close_method(self):
        """Test closing connection wrapper when connection has close method."""
        mock_connection = Mock()
        mock_connection.close = Mock()
        
        wrapper = NoSQLConnectionWrapper(
            mock_connection,
            NoSQLConnectionConfig("mongodb", ["localhost"], 27017)
        )
        
        assert wrapper.is_closed is False
        
        wrapper.close()
        
        assert wrapper.is_closed is True
        mock_connection.close.assert_called_once()
    
    def test_nosql_connection_wrapper_close_with_disconnect_method(self):
        """Test closing connection wrapper when connection has disconnect method."""
        mock_connection = Mock()
        mock_connection.disconnect = Mock()
        # Remove close method to test disconnect fallback
        del mock_connection.close
        
        wrapper = NoSQLConnectionWrapper(
            mock_connection,
            NoSQLConnectionConfig("cassandra", ["localhost"], 9042)
        )
        
        wrapper.close()
        
        assert wrapper.is_closed is True
        mock_connection.disconnect.assert_called_once()
    
    def test_nosql_connection_wrapper_close_no_method(self):
        """Test closing connection wrapper when connection has no close/disconnect method."""
        mock_connection = Mock(spec=[])  # Empty spec means no methods
        
        wrapper = NoSQLConnectionWrapper(
            mock_connection,
            NoSQLConnectionConfig("redis", ["localhost"], 6379)
        )
        
        # Should not raise exception even without close/disconnect methods
        wrapper.close()
        
        assert wrapper.is_closed is True
    
    def test_nosql_connection_wrapper_double_close(self):
        """Test closing connection wrapper multiple times."""
        mock_connection = Mock()
        mock_connection.close = Mock()
        
        wrapper = NoSQLConnectionWrapper(
            mock_connection,
            NoSQLConnectionConfig("mongodb", ["localhost"], 27017)
        )
        
        wrapper.close()
        wrapper.close()  # Second close should be safe
        
        assert wrapper.is_closed is True
        # Close should only be called once
        mock_connection.close.assert_called_once()


class TestNoSQLFabric:
    """Test suite for NoSQLFabric class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.config = {
            'connection_configs': {
                'mongodb_primary': {
                    'db_type': 'mongodb',
                    'hosts': ['localhost'],
                    'port': 27017,
                    'username': 'admin',
                    'password': 'secret',
                    'database': 'test_db',
                    'pool_size': 10
                },
                'redis_cache': {
                    'db_type': 'redis',
                    'hosts': ['localhost'],
                    'port': 6379,
                    'database': 0,
                    'pool_size': 5
                },
                'cassandra_analytics': {
                    'db_type': 'cassandra',
                    'hosts': ['cassandra1', 'cassandra2'],
                    'port': 9042,
                    'username': 'cassandra',
                    'password': 'cassandra',
                    'keyspace': 'analytics',
                    'pool_size': 8
                }
            }
        }
        
        self.fabric = NoSQLFabric(self.config)
    
    def test_nosql_fabric_initialization(self):
        """Test NoSQLFabric initialization."""
        fabric = NoSQLFabric(self.config)
        
        assert fabric._config == self.config
        assert fabric._pools == {}
        assert fabric._active_connections == {}
        assert fabric._connection_counter == 0
        assert fabric._initialized is False
        assert fabric.SUPPORTED_DATABASES == {'mongodb', 'redis', 'cassandra'}
    
    def test_get_required_config_fields(self):
        """Test required configuration fields."""
        required_fields = self.fabric._get_required_config_fields()
        
        assert isinstance(required_fields, list)
        assert 'connection_configs' in required_fields
    
    @patch('nosql_fabric.ConnectionPool')
    def test_setup_pools_success(self, mock_pool_class):
        """Test successful connection pool setup."""
        mock_pool_instance = Mock()
        mock_pool_class.return_value = mock_pool_instance
        
        with patch.object(self.fabric, '_create_connection') as mock_create:
            mock_create.return_value = Mock()
            
            self.fabric._setup_pools()
            
            # Should create pools for all three connections
            assert mock_pool_class.call_count == 3
            assert len(self.fabric._pools) == 3
            assert 'mongodb_primary' in self.fabric._pools
            assert 'redis_cache' in self.fabric._pools
            assert 'cassandra_analytics' in self.fabric._pools
    
    def test_setup_pools_unsupported_database(self):
        """Test setup_pools with unsupported database type."""
        config = {
            'connection_configs': {
                'unsupported': {
                    'db_type': 'neo4j',
                    'hosts': ['localhost'],
                    'port': 7687
                }
            }
        }
        
        fabric = NoSQLFabric(config)
        
        with pytest.raises(FabricException, match="Unsupported database type: neo4j"):
            fabric._setup_pools()
    
    @patch('nosql_fabric.ConnectionPool')
    def test_setup_pools_failure(self, mock_pool_class):
        """Test connection pool setup failure."""
        mock_pool_class.side_effect = Exception("Pool creation failed")
        
        with pytest.raises(FabricException, match="Failed to setup connection pools"):
            self.fabric._setup_pools()
    
    @patch('pymongo.MongoClient')
    def test_create_mongodb_connection_success(self, mock_mongo_client):
        """Test successful MongoDB connection creation."""
        mock_client = Mock()
        mock_mongo_client.return_value = mock_client
        
        config = NoSQLConnectionConfig(
            db_type='mongodb',
            hosts=['localhost'],
            port=27017,
            username='user',
            password='pass',
            database='testdb'
        )
        
        result = self.fabric._create_mongodb_connection(config)
        
        assert result == mock_client
        mock_mongo_client.assert_called_once_with(
            "mongodb://user:pass@localhost:27017/testdb",
            serverSelectionTimeoutMS=30000
        )
    
    @patch('pymongo.MongoClient')
    def test_create_mongodb_connection_no_auth(self, mock_mongo_client):
        """Test MongoDB connection creation without authentication."""
        mock_client = Mock()
        mock_mongo_client.return_value = mock_client
        
        config = NoSQLConnectionConfig(
            db_type='mongodb',
            hosts=['localhost'],
            port=27017,
            database='testdb'
        )
        
        result = self.fabric._create_mongodb_connection(config)
        
        mock_mongo_client.assert_called_once_with(
            "mongodb://localhost:27017/testdb",
            serverSelectionTimeoutMS=30000
        )
    
    @patch('redis.Redis')
    def test_create_redis_connection_success(self, mock_redis):
        """Test successful Redis connection creation."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        config = NoSQLConnectionConfig(
            db_type='redis',
            hosts=['localhost'],
            port=6379,
            username='user',
            password='pass',
            database=1,
            timeout=60
        )
        
        result = self.fabric._create_redis_connection(config)
        
        assert result == mock_client
        mock_redis.assert_called_once_with(
            host='localhost',
            port=6379,
            username='user',
            password='pass',
            db=1,
            decode_responses=True,
            socket_timeout=60
        )
    
    @patch('redis.Redis')
    def test_create_redis_connection_defaults(self, mock_redis):
        """Test Redis connection creation with default values."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        config = NoSQLConnectionConfig(
            db_type='redis',
            hosts=['localhost'],
            port=6379
        )
        
        result = self.fabric._create_redis_connection(config)
        
        mock_redis.assert_called_once_with(
            host='localhost',
            port=6379,
            username=None,
            password=None,
            db=0,  # Default database
            decode_responses=True,
            socket_timeout=30
        )
    
    @patch('cassandra.cluster.Cluster')
    @patch('cassandra.auth.PlainTextAuthProvider')
    def test_create_cassandra_connection_with_auth(self, mock_auth_provider_class, mock_cluster_class):
        """Test Cassandra connection creation with authentication."""
        mock_auth_provider = Mock()
        mock_auth_provider_class.return_value = mock_auth_provider
        
        mock_cluster = Mock()
        mock_session = Mock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster
        
        config = NoSQLConnectionConfig(
            db_type='cassandra',
            hosts=['cassandra1', 'cassandra2'],
            port=9042,
            username='cassandra',
            password='cassandra',
            keyspace='test_keyspace',
            timeout=45
        )
        
        result = self.fabric._create_cassandra_connection(config)
        
        assert result == mock_session
        mock_auth_provider_class.assert_called_once_with(
            username='cassandra',
            password='cassandra'
        )
        mock_cluster_class.assert_called_once_with(
            contact_points=['cassandra1', 'cassandra2'],
            port=9042,
            auth_provider=mock_auth_provider,
            connect_timeout=45
        )
        mock_cluster.connect.assert_called_once_with('test_keyspace')
    
    @patch('cassandra.cluster.Cluster')
    def test_create_cassandra_connection_no_auth(self, mock_cluster_class):
        """Test Cassandra connection creation without authentication."""
        mock_cluster = Mock()
        mock_session = Mock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster
        
        config = NoSQLConnectionConfig(
            db_type='cassandra',
            hosts=['localhost'],
            port=9042
        )
        
        result = self.fabric._create_cassandra_connection(config)
        
        mock_cluster_class.assert_called_once_with(
            contact_points=['localhost'],
            port=9042,
            auth_provider=None,
            connect_timeout=30
        )
        mock_cluster.connect.assert_called_once_with(None)  # No keyspace
    
    def test_create_connection_unsupported_type(self):
        """Test _create_connection with unsupported database type."""
        config = NoSQLConnectionConfig(
            db_type='unsupported',
            hosts=['localhost'],
            port=1234
        )
        
        with pytest.raises(FabricException, match="Unsupported database type: unsupported"):
            self.fabric._create_connection(config)
    
    @patch('pymongo.MongoClient')
    def test_create_connection_import_error(self, mock_mongo_client):
        """Test _create_connection with missing package."""
        mock_mongo_client.side_effect = ImportError("No module named 'pymongo'")
        
        config = NoSQLConnectionConfig(
            db_type='mongodb',
            hosts=['localhost'],
            port=27017
        )
        
        with pytest.raises(FabricException, match="Required package not installed for mongodb"):
            self.fabric._create_connection(config)
    
    @patch('nosql_fabric.ConnectionPool')
    def test_get_connection_success(self, mock_pool_class):
        """Test successful connection acquisition."""
        # Setup mock pool
        mock_pool = Mock()
        mock_connection = Mock()
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        # Mock connection creation
        with patch.object(self.fabric, '_create_connection') as mock_create:
            mock_create.return_value = Mock()
            self.fabric.initialize()
        
        connection_id = self.fabric.get_connection('mongodb_primary')
        
        assert isinstance(connection_id, str)
        assert connection_id.startswith('mongodb_primary_')
        assert connection_id in self.fabric._active_connections
        
        # Verify wrapper is created
        wrapper = self.fabric._active_connections[connection_id]
        assert isinstance(wrapper, NoSQLConnectionWrapper)
        assert wrapper.connection == mock_connection
    
    @patch('nosql_fabric.ConnectionPool')
    def test_get_connection_unknown_pool(self, mock_pool_class):
        """Test get_connection with unknown pool name."""
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
        
        with pytest.raises(FabricException, match="Unknown connection pool: nonexistent"):
            self.fabric.get_connection('nonexistent')
    
    @patch('nosql_fabric.ConnectionPool')
    def test_release_connection_success(self, mock_pool_class):
        """Test successful connection release."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('redis_cache')
        
        # Release connection
        self.fabric.release_connection(connection_id)
        
        # Verify connection removed and wrapper closed
        assert connection_id not in self.fabric._active_connections
        mock_pool.release.assert_called()
    
    def test_release_connection_invalid_id(self):
        """Test release_connection with invalid connection ID."""
        with pytest.raises(FabricException, match="Invalid connection id: invalid"):
            self.fabric.release_connection('invalid')
    
    @patch('nosql_fabric.ConnectionPool')
    def test_get_metrics_success(self, mock_pool_class):
        """Test getting connection metrics."""
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('cassandra_analytics')
        
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
        assert metrics.average_response_time == 0.0
    
    def test_get_metrics_invalid_connection(self):
        """Test get_metrics with invalid connection ID."""
        metrics = self.fabric.get_metrics('invalid_id')
        assert metrics is None
    
    @patch('nosql_fabric.ConnectionPool')
    def test_health_check_success(self, mock_pool_class):
        """Test successful health check."""
        mock_pool = Mock()
        mock_pool.health_check.return_value = True
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
        
        result = self.fabric.health_check()
        
        assert result is True
        # Should check all pools (3 in our config)
        assert mock_pool.health_check.call_count == 3
    
    @patch('nosql_fabric.ConnectionPool')
    def test_health_check_pool_failure(self, mock_pool_class):
        """Test health check with pool failure."""
        mock_pool = Mock()
        mock_pool.health_check.side_effect = [True, False, True]  # Middle pool fails
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
        
        result = self.fabric.health_check()
        
        assert result is False
    
    def test_health_check_not_initialized(self):
        """Test health check when not initialized."""
        result = self.fabric.health_check()
        assert result is False
    
    @patch('nosql_fabric.ConnectionPool')
    def test_context_manager_exit_cleanup(self, mock_pool_class):
        """Test context manager exit cleanup."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('mongodb_primary')
        
        # Exit context manager
        self.fabric.__exit__(None, None, None)
        
        # Verify cleanup
        assert len(self.fabric._active_connections) == 0
        assert len(self.fabric._pools) == 0
        # All pools should be closed
        assert mock_pool.close.call_count == 3
    
    @patch('nosql_fabric.ConnectionPool')
    def test_multiple_database_types(self, mock_pool_class):
        """Test working with multiple database types simultaneously."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
        
        # Get connections from different database types
        mongo_conn = self.fabric.get_connection('mongodb_primary')
        redis_conn = self.fabric.get_connection('redis_cache')
        cassandra_conn = self.fabric.get_connection('cassandra_analytics')
        
        # Verify all connections are unique and tracked
        assert mongo_conn != redis_conn != cassandra_conn
        assert mongo_conn.startswith('mongodb_primary_')
        assert redis_conn.startswith('redis_cache_')
        assert cassandra_conn.startswith('cassandra_analytics_')
        
        # All should be in active connections
        assert len(self.fabric._active_connections) == 3
        assert mongo_conn in self.fabric._active_connections
        assert redis_conn in self.fabric._active_connections
        assert cassandra_conn in self.fabric._active_connections
    
    def test_connection_counter_increment(self):
        """Test connection counter increments across different pools."""
        with patch('nosql_fabric.ConnectionPool') as mock_pool_class:
            mock_pool = Mock()
            mock_pool.acquire.return_value = Mock()
            mock_pool_class.return_value = mock_pool
            
            with patch.object(self.fabric, '_create_connection'):
                self.fabric.initialize()
            
            # Get connections from different pools
            conn1 = self.fabric.get_connection('mongodb_primary')
            conn2 = self.fabric.get_connection('redis_cache')
            conn3 = self.fabric.get_connection('mongodb_primary')
            
            # Verify counter incremented globally
            assert conn1 == 'mongodb_primary_0'
            assert conn2 == 'redis_cache_1'
            assert conn3 == 'mongodb_primary_2'
            assert self.fabric._connection_counter == 3


class TestNoSQLFabricEdgeCases:
    """Test edge cases and error conditions for NoSQLFabric."""
    
    def test_empty_connection_configs(self):
        """Test fabric with empty connection configs."""
        config = {'connection_configs': {}}
        fabric = NoSQLFabric(config)
        
        # Should initialize without error
        fabric.initialize()
        assert len(fabric._pools) == 0
    
    def test_connection_creation_edge_cases(self):
        """Test connection creation with edge case configurations."""
        fabric = NoSQLFabric({})
        
        # Test MongoDB with multiple hosts and no database
        mongo_config = NoSQLConnectionConfig(
            db_type='mongodb',
            hosts=['host1', 'host2', 'host3'],
            port=27017
        )
        
        with patch('pymongo.MongoClient') as mock_client:
            fabric._create_mongodb_connection(mongo_config)
            # Should use first host only in current implementation
            mock_client.assert_called_once()
            connection_string = mock_client.call_args[0][0]
            assert 'host1:27017' in connection_string
            assert 'host2' not in connection_string  # Current implementation limitation
    
    def test_wrapper_close_exception_handling(self):
        """Test wrapper close handles exceptions gracefully."""
        mock_connection = Mock()
        mock_connection.close.side_effect = Exception("Close failed")
        
        wrapper = NoSQLConnectionWrapper(
            mock_connection,
            NoSQLConnectionConfig('redis', ['localhost'], 6379)
        )
        
        # Should not raise exception even if close fails
        wrapper.close()
        
        assert wrapper.is_closed is True
    
    @patch('nosql_fabric.ConnectionPool')
    def test_release_connection_already_closed_wrapper(self, mock_pool_class):
        """Test releasing connection with already closed wrapper."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        fabric = NoSQLFabric({'connection_configs': {'test': {'db_type': 'redis', 'hosts': ['localhost'], 'port': 6379}}})
        
        with patch.object(fabric, '_create_connection'):
            fabric.initialize()
            connection_id = fabric.get_connection('test')
            
            # Manually close the wrapper
            wrapper = fabric._active_connections[connection_id]
            wrapper.close()
            
            # Release should still work
            fabric.release_connection(connection_id)
            
            assert connection_id not in fabric._active_connections
    
    def test_supported_databases_immutable(self):
        """Test that SUPPORTED_DATABASES is properly defined."""
        fabric = NoSQLFabric({})
        
        assert isinstance(fabric.SUPPORTED_DATABASES, set)
        assert 'mongodb' in fabric.SUPPORTED_DATABASES
        assert 'redis' in fabric.SUPPORTED_DATABASES
        assert 'cassandra' in fabric.SUPPORTED_DATABASES
        assert len(fabric.SUPPORTED_DATABASES) == 3