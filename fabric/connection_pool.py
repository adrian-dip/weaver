from typing import Any, Callable, Dict, Optional, List
from datetime import datetime
import threading
import queue
import logging
from dataclasses import dataclass

@dataclass
class PooledConnection:
    """Represents a connection managed by the pool."""
    connection: Any
    created_at: datetime
    last_used: datetime
    in_use: bool = False
    error_count: int = 0

class ConnectionPool:
    """
    Thread-safe connection pool implementation that manages database and API connections.
    Supports connection creation, acquisition, release, and health monitoring.
    """
    
    def __init__(self, name: str, max_size: int,
                 create_connection: Callable[[], Any],
                 min_size: int = 1,
                 max_idle_time: float = 300,  # 5 minutes
                 cleanup_interval: float = 60):  # 1 minute
        """
        Initialize the connection pool.
        
        Args:
            name: Pool identifier
            max_size: Maximum number of connections
            create_connection: Factory function to create new connections
            min_size: Minimum number of connections to maintain
            max_idle_time: Maximum time (seconds) a connection can be idle
            cleanup_interval: Interval (seconds) between cleanup runs
        """
        self.name = name
        self.max_size = max_size
        self.min_size = min_size
        self.create_connection = create_connection
        self.max_idle_time = max_idle_time
        
        self._pool: Dict[int, PooledConnection] = {}
        self._available = queue.Queue()
        self._lock = threading.RLock()
        self._closed = False
        self._conn_count = 0
        
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            args=(cleanup_interval,),
            daemon=True
        )
        self._cleanup_thread.start()
        
        self._initialize_min_connections()
    
    def _initialize_min_connections(self) -> None:
        """Initialize the minimum number of connections."""
        for _ in range(self.min_size):
            try:
                conn = self._create_pooled_connection()
                self._available.put(conn)
            except Exception as e:
                logging.error(f"Error initializing connection in pool {self.name}: {e}")
    
    def _create_pooled_connection(self) -> PooledConnection:
        """Create a new pooled connection."""
        with self._lock:
            if self._conn_count >= self.max_size:
                raise ValueError(f"Pool {self.name} has reached maximum size")
            
            connection = self.create_connection()
            self._conn_count += 1
            
            pooled = PooledConnection(
                connection=connection,
                created_at=datetime.now(),
                last_used=datetime.now()
            )
            
            self._pool[id(connection)] = pooled
            return pooled
    
    def acquire(self, timeout: Optional[float] = None) -> Any:
        """
        Acquire a connection from the pool.
        
        Args:
            timeout: Maximum time to wait for a connection
            
        Returns:
            A connection object
            
        Raises:
            queue.Empty: If no connection is available within timeout
            ValueError: If pool is closed
        """
        if self._closed:
            raise ValueError(f"Pool {self.name} is closed")
        
        try:
            pooled = self._available.get(timeout=timeout)
        except queue.Empty:
            pooled = self._create_pooled_connection()
        
        with self._lock:
            pooled.in_use = True
            pooled.last_used = datetime.now()
        
        return pooled.connection
    
    def release(self, connection: Any) -> None:
        """
        Release a connection back to the pool.
        
        Args:
            connection: Connection to release
            
        Raises:
            ValueError: If connection is not from this pool
        """
        with self._lock:
            pooled = self._pool.get(id(connection))
            if not pooled:
                raise ValueError("Connection not from this pool")
            
            if not pooled.in_use:
                raise ValueError("Connection is not marked as in-use")
            
            pooled.in_use = False
            pooled.last_used = datetime.now()
            
            if not self._closed:
                self._available.put(pooled)
    
    def _cleanup_loop(self, interval: float) -> None:
        """Periodically clean up idle connections."""
        while not self._closed:
            try:
                self._cleanup_idle_connections()
            except Exception as e:
                logging.error(f"Error in cleanup loop for pool {self.name}: {e}")
            
            threading.Event().wait(interval)
    
    def _cleanup_idle_connections(self) -> None:
        """Remove idle connections exceeding max_idle_time."""
        now = datetime.now()
        to_remove: List[PooledConnection] = []
        
        with self._lock:
            for pooled in self._pool.values():
                if (not pooled.in_use and
                    (now - pooled.last_used).total_seconds() > self.max_idle_time and
                    self._conn_count > self.min_size):
                    to_remove.append(pooled)
            
            for pooled in to_remove:
                try:
                    self._remove_connection(pooled)
                except Exception as e:
                    logging.error(f"Error removing connection from pool {self.name}: {e}")
    
    def _remove_connection(self, pooled: PooledConnection) -> None:
        """Remove a connection from the pool."""
        with self._lock:
            conn_id = id(pooled.connection)
            if conn_id in self._pool:
                try:
                    if hasattr(pooled.connection, 'close'):
                        pooled.connection.close()
                finally:
                    del self._pool[conn_id]
                    self._conn_count -= 1
    
    def close(self) -> None:
        """Close the pool and all connections."""
        with self._lock:
            if self._closed:
                return
            
            self._closed = True
            
            for pooled in self._pool.values():
                try:
                    if hasattr(pooled.connection, 'close'):
                        pooled.connection.close()
                except Exception as e:
                    logging.error(f"Error closing connection in pool {self.name}: {e}")
            
            self._pool.clear()
            self._conn_count = 0
            
            while not self._available.empty():
                try:
                    self._available.get_nowait()
                except queue.Empty:
                    break
    
    def health_check(self) -> bool:
        """Check the health of the pool."""
        with self._lock:
            if self._closed:
                return False
            
            if self._conn_count < self.min_size:
                return False
            
            for pooled in self._pool.values():
                try:
                    if hasattr(pooled.connection, 'ping'):
                        pooled.connection.ping()
                    elif hasattr(pooled.connection, 'is_connected'):
                        if not pooled.connection.is_connected():
                            return False
                except Exception:
                    return False
            
            return True
    
    @property
    def size(self) -> int:
        """Get current pool size."""
        return self._conn_count
    
    @property
    def available(self) -> int:
        """Get number of available connections."""
        return self._available.qsize()
    
    @property
    def in_use(self) -> int:
        """Get number of connections currently in use."""
        return sum(1 for conn in self._pool.values() if conn.in_use)