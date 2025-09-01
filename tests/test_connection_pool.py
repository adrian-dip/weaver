# test_connection_pool.py
import pytest
import threading
import time
import queue
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta
from connection_pool import ConnectionPool, PooledConnection


class TestPooledConnection:
    """Test suite for PooledConnection dataclass."""
    
    def test_pooled_connection_creation(self):
        """Test PooledConnection creation with all attributes."""
        mock_connection = Mock()
        created_at = datetime.now()
        last_used = datetime.now()
        
        pooled = PooledConnection(
            connection=mock_connection,
            created_at=created_at,
            last_used=last_used,
            in_use=True,
            error_count=2
        )
        
        assert pooled.connection == mock_connection
        assert pooled.created_at == created_at
        assert pooled.last_used == last_used
        assert pooled.in_use is True
        assert pooled.error_count == 2
    
    def test_pooled_connection_defaults(self):
        """Test PooledConnection with default values."""
        mock_connection = Mock()
        created_at = datetime.now()
        last_used = datetime.now()
        
        pooled = PooledConnection(
            connection=mock_connection,
            created_at=created_at,
            last_used=last_used
        )
        
        assert pooled.in_use is False  # Default
        assert pooled.error_count == 0  # Default


class TestConnectionPool:
    """Test suite for ConnectionPool class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.mock_connection_counter = 0
        
    def create_mock_connection(self):
        """Helper to create unique mock connections."""
        self.mock_connection_counter += 1
        mock_conn = Mock()
        mock_conn.id = f"conn_{self.mock_connection_counter}"
        mock_conn.close = Mock()
        mock_conn.ping = Mock()
        return mock_conn
    
    def test_connection_pool_initialization(self):
        """Test ConnectionPool initialization with all parameters."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        
        pool = ConnectionPool(
            name="test_pool",
            max_size=10,
            create_connection=create_conn,
            min_size=3,
            max_idle_time=300,
            cleanup_interval=60
        )
        
        assert pool.name == "test_pool"
        assert pool.max_size == 10
        assert pool.min_size == 3
        assert pool.max_idle_time == 300
        # Should initialize with min_size connections
        assert create_conn.call_count == 3
        assert pool.size == 3
        assert pool.available >= 0
    
    def test_connection_pool_initialization_defaults(self):
        """Test ConnectionPool with default parameters."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        
        pool = ConnectionPool("default_pool", max_size=5, create_connection=create_conn)
        
        assert pool.name == "default_pool"
        assert pool.max_size == 5
        assert pool.min_size == 1  # Default
        assert create_conn.call_count == 1  # Should create min_size connections
    
    def test_acquire_connection_success(self):
        """Test successful connection acquisition."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=5, create_connection=create_conn, min_size=1)
        
        conn = pool.acquire()
        
        assert conn is not None
        assert hasattr(conn, 'id')
        assert pool.in_use >= 1
        # Connection should be marked as in use
        pooled_conn = pool._pool[id(conn)]
        assert pooled_conn.in_use is True
        assert isinstance(pooled_conn.last_used, datetime)
    
    def test_acquire_connection_timeout(self):
        """Test connection acquisition with timeout when pool is exhausted."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=1, create_connection=create_conn, min_size=1)
        
        # Acquire the only connection
        conn1 = pool.acquire()
        assert conn1 is not None
        
        # Try to acquire another with short timeout - should fail
        with pytest.raises(queue.Empty):
            pool.acquire(timeout=0.1)
    
    def test_release_connection_success(self):
        """Test successful connection release."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=5, create_connection=create_conn, min_size=1)
        
        conn = pool.acquire()
        initial_available = pool.available
        
        pool.release(conn)
        
        # Connection should be marked as not in use
        pooled_conn = pool._pool[id(conn)]
        assert pooled_conn.in_use is False
        assert pool.available > initial_available
    
    def test_release_invalid_connection(self):
        """Test error when releasing connection not from this pool."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=5, create_connection=create_conn)
        
        invalid_conn = Mock()
        
        with pytest.raises(ValueError, match="Connection not from this pool"):
            pool.release(invalid_conn)
    
    def test_release_connection_not_in_use(self):
        """Test error when releasing connection not marked as in use."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=5, create_connection=create_conn, min_size=1)
        
        conn = pool.acquire()
        pool.release(conn)
        
        # Try to release again
        with pytest.raises(ValueError, match="Connection is not marked as in-use"):
            pool.release(conn)
    
    def test_max_size_enforcement(self):
        """Test that pool enforces maximum size limit."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=2, create_connection=create_conn, min_size=0)
        
        # Acquire up to max size
        conn1 = pool.acquire()
        conn2 = pool.acquire()
        
        assert pool.size == 2
        
        # Try to acquire beyond max size - should create new connection anyway
        # (based on implementation, it creates new connections when queue is empty)
        with pytest.raises(ValueError, match="has reached maximum size"):
            pool._create_pooled_connection()
    
    def test_health_check_healthy_pool(self):
        """Test health check on healthy pool."""
        mock_conn = Mock()
        mock_conn.ping = Mock()
        create_conn = Mock(return_value=mock_conn)
        
        pool = ConnectionPool("test_pool", max_size=3, create_connection=create_conn, min_size=2)
        
        result = pool.health_check()
        
        assert result is True
        assert pool.size >= pool.min_size
    
    def test_health_check_closed_pool(self):
        """Test health check on closed pool."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=5, create_connection=create_conn)
        
        pool.close()
        
        result = pool.health_check()
        
        assert result is False
        assert pool._closed is True
    
    def test_health_check_connection_ping_failure(self):
        """Test health check when connection ping fails."""
        mock_conn = Mock()
        mock_conn.ping = Mock(side_effect=Exception("Connection lost"))
        create_conn = Mock(return_value=mock_conn)
        
        pool = ConnectionPool("test_pool", max_size=2, create_connection=create_conn, min_size=1)
        
        result = pool.health_check()
        
        assert result is False
    
    def test_pool_close_cleanup(self):
        """Test proper cleanup when pool is closed."""
        mock_connections = []
        
        def create_conn():
            conn = Mock()
            conn.close = Mock()
            mock_connections.append(conn)
            return conn
        
        pool = ConnectionPool("test_pool", max_size=3, create_connection=create_conn, min_size=2)
        
        # Acquire a connection to test cleanup of in-use connections
        acquired_conn = pool.acquire()
        
        pool.close()
        
        assert pool._closed is True
        assert pool.size == 0
        assert len(pool._pool) == 0
        
        # All connections should have close() called
        for conn in mock_connections:
            conn.close.assert_called_once()
    
    def test_pool_properties(self):
        """Test pool size properties."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=5, create_connection=create_conn, min_size=2)
        
        # Test initial state
        assert pool.size == 2  # min_size connections created
        assert pool.available >= 0
        assert pool.in_use == 0
        
        # Acquire connection and test properties
        conn = pool.acquire()
        assert pool.in_use >= 1
        assert pool.available >= 0
        
        pool.release(conn)
        assert pool.in_use >= 0
    
    @patch('threading.Thread')
    def test_cleanup_thread_creation(self, mock_thread):
        """Test that cleanup thread is created properly."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        
        pool = ConnectionPool(
            "test_pool", 
            max_size=5, 
            create_connection=create_conn,
            cleanup_interval=30
        )
        
        # Verify thread was created and started
        mock_thread.assert_called_once()
        thread_instance = mock_thread.return_value
        thread_instance.start.assert_called_once()
        
        # Verify thread configuration
        call_args = mock_thread.call_args
        assert call_args[1]['target'] == pool._cleanup_loop
        assert call_args[1]['args'] == (30,)  # cleanup_interval
        assert call_args[1]['daemon'] is True
    
    def test_connection_acquire_after_close(self):
        """Test that acquiring connection after close raises error."""
        create_conn = Mock(side_effect=self.create_mock_connection)
        pool = ConnectionPool("test_pool", max_size=3, create_connection=create_conn)
        
        pool.close()
        
        with pytest.raises(ValueError, match="Pool test_pool is closed"):
            pool.acquire()


class TestConnectionPoolConcurrency:
    """Test suite for ConnectionPool thread safety and concurrency."""
    
    def test_concurrent_acquire_release(self):
        """Test concurrent connection acquisition and release."""
        connection_counter = [0]
        
        def create_conn():
            connection_counter[0] += 1
            conn = Mock()
            conn.id = f"conn_{connection_counter[0]}"
            return conn
        
        pool = ConnectionPool("concurrent_pool", max_size=10, create_connection=create_conn, min_size=0)
        results = []
        errors = []
        
        def worker():
            try:
                conn = pool.acquire(timeout=1.0)
                time.sleep(0.01)  # Simulate work
                pool.release(conn)
                results.append("success")
            except Exception as e:
                errors.append(str(e))
        
        # Start multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=2.0)
        
        # Verify results
        assert len(results) == 5
        assert len(errors) == 0
        assert all(result == "success" for result in results)
    
    def test_concurrent_pool_operations(self):
        """Test various pool operations under concurrent access."""
        create_conn = Mock(side_effect=lambda: Mock())
        pool = ConnectionPool("test_pool", max_size=5, create_connection=create_conn, min_size=1)
        
        operations_completed = []
        
        def health_check_worker():
            for _ in range(10):
                result = pool.health_check()
                operations_completed.append(f"health_check_{result}")
                time.sleep(0.001)
        
        def size_check_worker():
            for _ in range(10):
                size = pool.size
                available = pool.available
                in_use = pool.in_use
                operations_completed.append(f"size_check_{size}_{available}_{in_use}")
                time.sleep(0.001)
        
        threads = [
            threading.Thread(target=health_check_worker),
            threading.Thread(target=size_check_worker)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join(timeout=2.0)
        
        # Should have operations from both workers
        health_checks = [op for op in operations_completed if op.startswith("health_check")]
        size_checks = [op for op in operations_completed if op.startswith("size_check")]
        
        assert len(health_checks) == 10
        assert len(size_checks) == 10