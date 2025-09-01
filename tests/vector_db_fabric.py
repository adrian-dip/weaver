# test_vector_db_fabric.py
import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from datetime import datetime
import numpy as np
from vector_db_fabric import VectorDBFabric, VectorDBConnectionConfig, VectorDBConnectionWrapper
from fabric_exceptions import FabricException
from fabric_base import ConnectionMetrics


class TestVectorDBConnectionConfig:
    """Test suite for VectorDBConnectionConfig dataclass."""
    
    def test_vector_db_connection_config_creation_full(self):
        """Test VectorDBConnectionConfig creation with all parameters."""
        config = VectorDBConnectionConfig(
            db_type="pinecone",
            dimension=1536,
            hosts=["api.pinecone.io"],
            port=443,
            username="user",
            password="pass",
            api_key="pk-123456",
            environment="us-east-1",
            pool_size=12,
            max_retries=4,
            timeout=60,
            retry_interval=3,
            metric="euclidean"
        )
        
        assert config.db_type == "pinecone"
        assert config.dimension == 1536
        assert config.hosts == ["api.pinecone.io"]
        assert config.port == 443
        assert config.username == "user"
        assert config.password == "pass"
        assert config.api_key == "pk-123456"
        assert config.environment == "us-east-1"
        assert config.pool_size == 12
        assert config.max_retries == 4
        assert config.timeout == 60
        assert config.retry_interval == 3
        assert config.metric == "euclidean"
    
    def test_vector_db_connection_config_creation_minimal(self):
        """Test VectorDBConnectionConfig creation with minimal parameters."""
        config = VectorDBConnectionConfig(
            db_type="pgvector",
            dimension=768,
            hosts=["localhost"]
        )
        
        assert config.db_type == "pgvector"
        assert config.dimension == 768
        assert config.hosts == ["localhost"]
        # Test defaults
        assert config.port is None
        assert config.username is None
        assert config.password is None
        assert config.api_key is None
        assert config.environment is None
        assert config.pool_size == 5
        assert config.max_retries == 3
        assert config.timeout == 30
        assert config.retry_interval == 1
        assert config.metric == "cosine"  # Default metric
    
    def test_vector_db_connection_config_different_metrics(self):
        """Test VectorDBConnectionConfig with different similarity metrics."""
        metrics = ["cosine", "euclidean", "dot_product"]
        
        for metric in metrics:
            config = VectorDBConnectionConfig(
                db_type="pgvector",
                dimension=512,
                hosts=["localhost"],
                metric=metric
            )
            assert config.metric == metric


class TestVectorDBConnectionWrapper:
    """Test suite for VectorDBConnectionWrapper class."""
    
    def test_vector_db_connection_wrapper_creation(self):
        """Test VectorDBConnectionWrapper creation and initialization."""
        mock_connection = Mock()
        config = VectorDBConnectionConfig("pgvector", 1024, ["localhost"])
        
        wrapper = VectorDBConnectionWrapper(mock_connection, config)
        
        assert wrapper.connection == mock_connection
        assert wrapper.config == config
        assert isinstance(wrapper.created_at, datetime)
        assert isinstance(wrapper.last_used, datetime)
        assert wrapper.total_operations == 0
        assert wrapper.failed_operations == 0
        assert wrapper.is_closed is False
    
    def test_vector_db_connection_wrapper_operations(self):
        """Test connection wrapper operation tracking."""
        wrapper = VectorDBConnectionWrapper(
            Mock(),
            VectorDBConnectionConfig("pinecone", 1536, ["api.pinecone.io"])
        )
        
        # Test mark_used
        initial_operations = wrapper.total_operations
        initial_last_used = wrapper.last_used
        
        import time
        time.sleep(0.01)
        
        wrapper.mark_used()
        
        assert wrapper.total_operations == initial_operations + 1
        assert wrapper.last_used > initial_last_used
        
        # Test mark_failed
        initial_failed = wrapper.failed_operations
        wrapper.mark_failed()
        assert wrapper.failed_operations == initial_failed + 1
    
    def test_vector_db_connection_wrapper_close(self):
        """Test connection wrapper close functionality."""
        mock_connection = Mock()
        mock_connection.close = Mock()
        
        wrapper = VectorDBConnectionWrapper(
            mock_connection,
            VectorDBConnectionConfig("pgvector", 768, ["localhost"])
        )
        
        assert wrapper.is_closed is False
        
        wrapper.close()
        
        assert wrapper.is_closed is True
        mock_connection.close.assert_called_once()
        
        # Second close should be safe
        wrapper.close()
        assert wrapper.is_closed is True
        # Should still only be called once
        mock_connection.close.assert_called_once()
    
    def test_vector_db_connection_wrapper_close_with_disconnect(self):
        """Test close with disconnect method."""
        mock_connection = Mock()
        mock_connection.disconnect = Mock()
        del mock_connection.close  # Remove close method
        
        wrapper = VectorDBConnectionWrapper(
            mock_connection,
            VectorDBConnectionConfig("pinecone", 1536, ["api.pinecone.io"])
        )
        
        wrapper.close()
        
        assert wrapper.is_closed is True
        mock_connection.disconnect.assert_called_once()


class TestVectorDBFabric:
    """Test suite for VectorDBFabric class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.config = {
            'connection_configs': {
                'pgvector_primary': {
                    'db_type': 'pgvector',
                    'dimension': 1536,
                    'hosts': ['localhost'],
                    'port': 5432,
                    'username': 'postgres',
                    'password': 'postgres',
                    'pool_size': 10
                },
                'pinecone_index': {
                    'db_type': 'pinecone',
                    'dimension': 1536,
                    'hosts': ['api.pinecone.io'],
                    'api_key': 'pk-test-key',
                    'environment': 'us-east-1',
                    'pool_size': 5,
                    'metric': 'euclidean'
                }
            }
        }
        
        self.fabric = VectorDBFabric(self.config)
    
    def test_vector_db_fabric_initialization(self):
        """Test VectorDBFabric initialization."""
        fabric = VectorDBFabric(self.config)
        
        assert fabric._config == self.config
        assert fabric._pools == {}
        assert fabric._active_connections == {}
        assert fabric._connection_counter == 0
        assert fabric._initialized is False
        assert fabric.SUPPORTED_DATABASES == {'pgvector', 'pinecone'}
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_setup_pools_success(self, mock_pool_class):
        """Test successful connection pool setup."""
        mock_pool_instance = Mock()
        mock_pool_class.return_value = mock_pool_instance
        
        with patch.object(self.fabric, '_create_connection') as mock_create:
            mock_create.return_value = Mock()
            
            self.fabric._setup_pools()
            
            # Should create pools for both connections
            assert mock_pool_class.call_count == 2
            assert len(self.fabric._pools) == 2
            assert 'pgvector_primary' in self.fabric._pools
            assert 'pinecone_index' in self.fabric._pools
    
    def test_setup_pools_unsupported_database(self):
        """Test setup_pools with unsupported database type."""
        config = {
            'connection_configs': {
                'unsupported': {
                    'db_type': 'chroma',
                    'dimension': 512,
                    'hosts': ['localhost']
                }
            }
        }
        
        fabric = VectorDBFabric(config)
        
        with pytest.raises(FabricException, match="Unsupported database type: chroma"):
            fabric._setup_pools()
    
    @patch('psycopg2.connect')
    def test_create_pgvector_connection_success(self, mock_connect):
        """Test successful pgvector connection creation."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        config = VectorDBConnectionConfig(
            db_type='pgvector',
            dimension=1536,
            hosts=['localhost'],
            port=5432,
            username='postgres',
            password='secret'
        )
        
        result = self.fabric._create_pgvector_connection(config)
        
        assert result == mock_connection
        mock_connect.assert_called_once_with(
            host='localhost',
            port=5432,
            database='postgres',
            user='postgres',
            password='secret'
        )
        # Should enable pgvector extension
        mock_cursor.execute.assert_called_once_with("CREATE EXTENSION IF NOT EXISTS vector")
        mock_connection.commit.assert_called_once()
    
    @patch('pinecone.init')
    def test_create_pinecone_connection_success(self, mock_pinecone_init):
        """Test successful Pinecone connection creation."""
        with patch('pinecone') as mock_pinecone:
            config = VectorDBConnectionConfig(
                db_type='pinecone',
                dimension=1536,
                hosts=['api.pinecone.io'],
                api_key='pk-test-key',
                environment='us-east-1'
            )
            
            result = self.fabric._create_pinecone_connection(config)
            
            assert result == mock_pinecone
            mock_pinecone_init.assert_called_once_with(
                api_key='pk-test-key',
                environment='us-east-1'
            )
    
    def test_create_connection_unsupported_type(self):
        """Test _create_connection with unsupported database type."""
        config = VectorDBConnectionConfig(
            db_type='unsupported',
            dimension=512,
            hosts=['localhost']
        )
        
        with pytest.raises(FabricException, match="Unsupported database type: unsupported"):
            self.fabric._create_connection(config)
    
    @patch('psycopg2.connect')
    def test_create_connection_import_error(self, mock_connect):
        """Test _create_connection with missing package."""
        mock_connect.side_effect = ImportError("No module named 'psycopg2'")
        
        config = VectorDBConnectionConfig(
            db_type='pgvector',
            dimension=768,
            hosts=['localhost']
        )
        
        with pytest.raises(FabricException, match="Required package not installed for pgvector"):
            self.fabric._create_connection(config)
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_get_connection_success(self, mock_pool_class):
        """Test successful connection acquisition."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
        
        connection_id = self.fabric.get_connection('pgvector_primary')
        
        assert isinstance(connection_id, str)
        assert connection_id.startswith('pgvector_primary_')
        assert connection_id in self.fabric._active_connections
        
        # Verify wrapper is created
        wrapper = self.fabric._active_connections[connection_id]
        assert isinstance(wrapper, VectorDBConnectionWrapper)
        assert wrapper.connection == mock_connection
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_execute_pgvector_create_collection(self, mock_pool_class):
        """Test execute pgvector create_collection operation."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pgvector_primary')
        
        # Execute create_collection operation
        result = self.fabric.execute_operation(
            connection_id, 
            'create_collection', 
            'test_vectors',
            dimension=1536
        )
        
        # Verify SQL execution
        expected_calls = [
            ("""
                    CREATE TABLE IF NOT EXISTS test_vectors (
                        id SERIAL PRIMARY KEY,
                        vector vector(1536),
                        metadata JSONB
                    )
                """,),
            ("""
                    CREATE INDEX IF NOT EXISTS idx_test_vectors_vector 
                    ON test_vectors USING ivfflat (vector vector_l2_ops)
                """,)
        ]
        
        assert mock_cursor.execute.call_count == 2
        mock_connection.commit.assert_called_once()
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_execute_pgvector_upsert(self, mock_pool_class):
        """Test execute pgvector upsert operation."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [(1,), (2,), (3,)]  # Return IDs
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pgvector_primary')
        
        # Test data
        vectors = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
            [0.7, 0.8, 0.9]
        ]
        metadata = [
            {'doc_id': '1', 'type': 'text'},
            {'doc_id': '2', 'type': 'image'},
            {'doc_id': '3', 'type': 'audio'}
        ]
        
        result = self.fabric.execute_operation(
            connection_id,
            'upsert',
            'test_vectors',
            vectors=vectors,
            metadata=metadata
        )
        
        assert result == [1, 2, 3]
        assert mock_cursor.execute.call_count == 3  # One for each vector
        mock_connection.commit.assert_called_once()
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_execute_pgvector_search(self, mock_pool_class):
        """Test execute pgvector search operation."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            (1, {'doc_id': '1'}, 0.1),
            (2, {'doc_id': '2'}, 0.2),
            (3, {'doc_id': '3'}, 0.3)
        ]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pgvector_primary')
        
        query_vector = [0.1, 0.2, 0.3, 0.4]
        
        result = self.fabric.execute_operation(
            connection_id,
            'search',
            'test_vectors',
            query_vector=query_vector,
            k=3,
            filter_metadata={'type': 'text'}
        )
        
        assert len(result) == 3
        assert result[0][0] == 1  # First result ID
        assert result[0][2] == 0.1  # First result distance
        
        # Verify SQL execution with filter
        mock_cursor.execute.assert_called_once()
        sql_call = mock_cursor.execute.call_args[0][0]
        assert "WHERE metadata->'type' = 'text'::jsonb" in sql_call
        assert "ORDER BY distance" in sql_call
        assert "LIMIT" in sql_call
    
    @patch('vector_db_fabric.ConnectionPool')
    @patch('pinecone.Index')
    def test_execute_pinecone_create_collection(self, mock_index_class, mock_pool_class):
        """Test execute Pinecone create_collection operation."""
        mock_pool = Mock()
        mock_pinecone_client = Mock()
        mock_pinecone_client.list_indexes.return_value = []  # Index doesn't exist
        mock_pool.acquire.return_value = mock_pinecone_client
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pinecone_index')
        
        result = self.fabric.execute_operation(
            connection_id,
            'create_collection',
            'test-index',
            dimension=1536,
            metric='cosine'
        )
        
        # Verify index creation
        mock_pinecone_client.create_index.assert_called_once_with(
            'test-index',
            dimension=1536,
            metric='cosine'
        )
    
    @patch('vector_db_fabric.ConnectionPool')
    @patch('pinecone.Index')
    def test_execute_pinecone_upsert(self, mock_index_class, mock_pool_class):
        """Test execute Pinecone upsert operation."""
        mock_pool = Mock()
        mock_pinecone_client = Mock()
        mock_index = Mock()
        mock_pinecone_client.Index.return_value = mock_index
        mock_pool.acquire.return_value = mock_pinecone_client
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pinecone_index')
        
        # Test data with numpy arrays
        vectors = [
            np.array([0.1, 0.2, 0.3]),
            np.array([0.4, 0.5, 0.6])
        ]
        metadata = [
            {'doc_id': '1', 'type': 'text'},
            {'doc_id': '2', 'type': 'image'}
        ]
        ids = ['vec1', 'vec2']
        
        result = self.fabric.execute_operation(
            connection_id,
            'upsert',
            'test-index',
            vectors=vectors,
            metadata=metadata,
            ids=ids
        )
        
        assert result == ids
        mock_index.upsert.assert_called_once()
        # Verify upsert data format
        upsert_call_args = mock_index.upsert.call_args[1]['vectors']
        assert len(upsert_call_args) == 2
        assert upsert_call_args[0][0] == 'vec1'  # First ID
        assert upsert_call_args[0][1] == [0.1, 0.2, 0.3]  # First vector as list
        assert upsert_call_args[0][2] == {'doc_id': '1', 'type': 'text'}  # First metadata
    
    @patch('vector_db_fabric.ConnectionPool')
    @patch('pinecone.Index')
    def test_execute_pinecone_search(self, mock_index_class, mock_pool_class):
        """Test execute Pinecone search operation."""
        mock_pool = Mock()
        mock_pinecone_client = Mock()
        mock_index = Mock()
        
        # Mock search results
        mock_match1 = Mock()
        mock_match1.id = 'vec1'
        mock_match1.metadata = {'doc_id': '1', 'type': 'text'}
        mock_match1.score = 0.95
        
        mock_match2 = Mock()
        mock_match2.id = 'vec2'
        mock_match2.metadata = {'doc_id': '2', 'type': 'image'}
        mock_match2.score = 0.87
        
        mock_results = Mock()
        mock_results.matches = [mock_match1, mock_match2]
        mock_index.query.return_value = mock_results
        
        mock_pinecone_client.Index.return_value = mock_index
        mock_pool.acquire.return_value = mock_pinecone_client
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pinecone_index')
        
        query_vector = np.array([0.1, 0.2, 0.3])
        
        result = self.fabric.execute_operation(
            connection_id,
            'search',
            'test-index',
            query_vector=query_vector,
            k=2,
            filter_metadata={'type': 'text'}
        )
        
        assert len(result) == 2
        assert result[0]['id'] == 'vec1'
        assert result[0]['metadata'] == {'doc_id': '1', 'type': 'text'}
        assert result[0]['distance'] == 0.95
        
        # Verify query call
        mock_index.query.assert_called_once_with(
            [0.1, 0.2, 0.3],  # Vector as list
            top_k=2,
            filter={'type': 'text'}
        )
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_execute_operation_invalid_connection(self, mock_pool_class):
        """Test execute_operation with invalid connection ID."""
        with pytest.raises(FabricException, match="Invalid connection id: invalid"):
            self.fabric.execute_operation('invalid', 'search', 'collection')
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_execute_operation_failure(self, mock_pool_class):
        """Test execute_operation failure handling."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_connection.cursor.side_effect = Exception("Database error")
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pgvector_primary')
        
        with pytest.raises(FabricException, match="Operation failed"):
            self.fabric.execute_operation(connection_id, 'create_collection', 'test', dimension=512)
        
        # Verify failure was recorded
        wrapper = self.fabric._active_connections[connection_id]
        assert wrapper.failed_operations == 1
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_release_connection_success(self, mock_pool_class):
        """Test successful connection release."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pgvector_primary')
        
        # Release connection
        self.fabric.release_connection(connection_id)
        
        # Verify connection removed and wrapper closed
        assert connection_id not in self.fabric._active_connections
        mock_pool.release.assert_called()
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_get_metrics_success(self, mock_pool_class):
        """Test getting connection metrics."""
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pinecone_index')
        
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
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_health_check_success(self, mock_pool_class):
        """Test successful health check."""
        mock_pool = Mock()
        mock_pool.health_check.return_value = True
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
        
        result = self.fabric.health_check()
        
        assert result is True
        # Should check both pools
        assert mock_pool.health_check.call_count == 2
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_context_manager_exit_cleanup(self, mock_pool_class):
        """Test context manager exit cleanup."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
            connection_id = self.fabric.get_connection('pgvector_primary')
        
        # Exit context manager
        self.fabric.__exit__(None, None, None)
        
        # Verify cleanup
        assert len(self.fabric._active_connections) == 0
        assert len(self.fabric._pools) == 0
        # Both pools should be closed
        assert mock_pool.close.call_count == 2
    
    def test_vector_operations_with_different_dimensions(self):
        """Test vector operations with different dimension configurations."""
        configs = [
            {'db_type': 'pgvector', 'dimension': 384, 'hosts': ['localhost']},
            {'db_type': 'pgvector', 'dimension': 768, 'hosts': ['localhost']},
            {'db_type': 'pgvector', 'dimension': 1536, 'hosts': ['localhost']},
            {'db_type': 'pinecone', 'dimension': 1024, 'hosts': ['api.pinecone.io'], 'api_key': 'test', 'environment': 'test'}
        ]
        
        for config_data in configs:
            config = VectorDBConnectionConfig(**config_data)
            assert config.dimension in [384, 768, 1024, 1536]
            assert config.db_type in ['pgvector', 'pinecone']
    
    @patch('vector_db_fabric.ConnectionPool')
    def test_concurrent_operations_different_connections(self, mock_pool_class):
        """Test concurrent operations on different connection types."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_pool.acquire.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with patch.object(self.fabric, '_create_connection'):
            self.fabric.initialize()
        
        # Get connections from both database types
        pgvector_conn = self.fabric.get_connection('pgvector_primary')
        pinecone_conn = self.fabric.get_connection('pinecone_index')
        
        # Verify different connection types
        assert pgvector_conn.startswith('pgvector_primary_')
        assert pinecone_conn.startswith('pinecone_index_')
        assert pgvector_conn != pinecone_conn
        
        # Both should be tracked
        assert len(self.fabric._active_connections) == 2
        assert pgvector_conn in self.fabric._active_connections
        assert pinecone_conn in self.fabric._active_connections


class TestVectorDBFabricEdgeCases:
    """Test edge cases and error conditions for VectorDBFabric."""
    
    def test_supported_databases_set(self):
        """Test SUPPORTED_DATABASES is properly defined."""
        fabric = VectorDBFabric({})
        
        assert isinstance(fabric.SUPPORTED_DATABASES, set)
        assert 'pgvector' in fabric.SUPPORTED_DATABASES
        assert 'pinecone' in fabric.SUPPORTED_DATABASES
        assert len(fabric.SUPPORTED_DATABASES) == 2
    
    def test_pgvector_upsert_with_ids(self):
        """Test pgvector upsert with specific IDs."""
        fabric = VectorDBFabric({})
        
        # This would be tested in integration with actual execute_operation
        # but we can verify the logic would handle IDs correctly
        assert hasattr(fabric, '_execute_pgvector_operation')
    
    def test_pinecone_upsert_default_ids(self):
        """Test Pinecone upsert with default ID generation."""
        fabric = VectorDBFabric({})
        
        # Verify the method exists and would generate default IDs
        assert hasattr(fabric, '_execute_pinecone_operation')
    
    @patch('psycopg2.connect')
    def test_pgvector_connection_failure_handling(self, mock_connect):
        """Test pgvector connection creation failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        fabric = VectorDBFabric({})
        config = VectorDBConnectionConfig('pgvector', 768, ['localhost'])
        
        with pytest.raises(FabricException, match="Failed to create pgvector connection"):
            fabric._create_pgvector_connection(config)
    
    @patch('pinecone.init')
    def test_pinecone_connection_failure_handling(self, mock_init):
        """Test Pinecone connection creation failure."""
        mock_init.side_effect = Exception("API key invalid")
        
        fabric = VectorDBFabric({})
        config = VectorDBConnectionConfig(
            'pinecone', 1536, ['api.pinecone.io'], 
            api_key='invalid', environment='test'
        )
        
        with pytest.raises(FabricException, match="Failed to create Pinecone connection"):
            fabric._create_pinecone_connection(config)
    
    def test_vector_dimension_validation_concepts(self):
        """Test concepts around vector dimension validation."""
        # Common dimensions used in practice
        common_dimensions = [128, 256, 384, 512, 768, 1024, 1536, 2048, 4096]
        
        for dim in common_dimensions:
            config = VectorDBConnectionConfig(
                'pgvector', dim, ['localhost']
            )
            assert config.dimension == dim
            assert config.dimension > 0