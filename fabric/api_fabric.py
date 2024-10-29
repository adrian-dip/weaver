import time
import threading
import asyncio
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

@dataclass
class TokenBucket:
    """Represents a token bucket for rate limiting. Rate == tokens per second"""
    capacity: int
    tokens: float
    rate: float  
    last_update: float

class RateLimiter:
    """
    Token bucket implementation of a rate limiter.
    Supports both synchronous and asynchronous operation.
    """
    
    def __init__(self, rate_limit: int, time_window: float = 1.0,
                 burst_limit: Optional[int] = None):
        """
        Initialize the rate limiter.
        
        Args:
            rate_limit: Number of operations allowed per time window
            time_window: Time window in seconds
            burst_limit: Maximum burst size (defaults to rate_limit)
        """
        self.rate = rate_limit / time_window
        self.capacity = burst_limit or rate_limit
        
        self._bucket = TokenBucket(
            capacity=self.capacity,
            tokens=self.capacity,
            rate=self.rate,
            last_update=time.time()
        )
        
        self._lock = threading.RLock()
        
        self._windows: Dict[int, List[datetime]] = {}
        self._window_size = timedelta(seconds=time_window)
    
    def _update_tokens(self) -> None:
        """Update the number of available tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._bucket.last_update
        
        # Add tokens based on elapsed time
        new_tokens = elapsed * self._bucket.rate
        self._bucket.tokens = min(
            self._bucket.capacity,
            self._bucket.tokens + new_tokens
        )
        
        self._bucket.last_update = now
    
    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Acquire tokens from the bucket (synchronous).
        
        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait for tokens
            
        Returns:
            True if tokens were acquired, False if timed out
            
        Raises:
            ValueError: If requested tokens exceed capacity
        """
        if tokens > self._bucket.capacity:
            raise ValueError(
                f"Requested tokens {tokens} exceed capacity {self._bucket.capacity}"
            )
        
        start_time = time.time()
        
        with self._lock:
            while True:
                self._update_tokens()
                
                if self._bucket.tokens >= tokens:
                    self._bucket.tokens -= tokens
                    return True
                
                if timeout is not None:
                    if time.time() - start_time >= timeout:
                        return False
                
                # Calculate wait time for next token
                required = tokens - self._bucket.tokens
                wait_time = required / self._bucket.rate
                
                # If we have a timeout, don't wait longer than remaining time
                if timeout is not None:
                    elapsed = time.time() - start_time
                    wait_time = min(wait_time, timeout - elapsed)
                
                if wait_time > 0:
                    time.sleep(wait_time)
    
    async def acquire_async(self, tokens: int = 1,
                          timeout: Optional[float] = None) -> bool:
        """
        Acquire tokens from the bucket (asynchronous).
        
        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait for tokens
            
        Returns:
            True if tokens were acquired, False if timed out
            
        Raises:
            ValueError: If requested tokens exceed capacity
        """
        if tokens > self._bucket.capacity:
            raise ValueError(
                f"Requested tokens {tokens} exceed capacity {self._bucket.capacity}"
            )
        
        start_time = time.time()
        
        while True:
            acquire_success = False
            
            with self._lock:
                self._update_tokens()
                
                if self._bucket.tokens >= tokens:
                    self._bucket.tokens -= tokens
                    acquire_success = True
            
            if acquire_success:
                return True
            
            if timeout is not None:
                if time.time() - start_time >= timeout:
                    return False
            
            with self._lock:
                required = tokens - self._bucket.tokens
                wait_time = required / self._bucket.rate
            
            if timeout is not None:
                elapsed = time.time() - start_time
                wait_time = min(wait_time, timeout - elapsed)
            
            if wait_time > 0:
                await asyncio.sleep(wait_time)
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens without waiting.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            True if tokens were acquired, False otherwise
        """
        with self._lock:
            self._update_tokens()
            
            if self._bucket.tokens >= tokens:
                self._bucket.tokens -= tokens
                return True
            
            return False
    
    def get_window_count(self, window_id: int) -> int:
        """
        Get the number of operations in a specific window.
        
        Args:
            window_id: Window identifier
            
        Returns:
            Number of operations in the window
        """
        now = datetime.now()
        window_start = now - self._window_size
        
        with self._lock:
            # Clean up old timestamps
            self._windows[window_id] = [
                ts for ts in self._windows.get(window_id, [])
                if ts > window_start
            ]
            
            return len(self._windows[window_id])
    
    def record_operation(self, window_id: int) -> None:
        """
        Record an operation in a specific window.
        
        Args:
            window_id: Window identifier
        """
        now = datetime.now()
        window_start = now - self._window_size
        
        with self._lock:
            self._windows[window_id] = [
                ts for ts in self._windows.get(window_id, [])
                if ts > window_start
            ] + [now]
    
    def reset(self) -> None:
        """Reset the rate limiter to its initial state."""
        with self._lock:
            self._bucket.tokens = self._bucket.capacity
            self._bucket.last_update = time.time()
            self._windows.clear()
    
    @property
    def available_tokens(self) -> float:
        """Get the current number of available tokens."""
        with self._lock:
            self._update_tokens()
            return self._bucket.tokens