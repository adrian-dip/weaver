# test_rate_limiter.py
import pytest
import time
import asyncio
import threading
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from rate_limiter import RateLimiter, TokenBucket


class TestTokenBucket:
    """Test suite for TokenBucket dataclass."""
    
    def test_token_bucket_creation(self):
        """Test TokenBucket creation with all attributes."""
        capacity = 100
        tokens = 50.0
        rate = 10.0
        last_update = time.time()
        
        bucket = TokenBucket(
            capacity=capacity,
            tokens=tokens,
            rate=rate,
            last_update=last_update
        )
        
        assert bucket.capacity == capacity
        assert bucket.tokens == tokens
        assert bucket.rate == rate
        assert bucket.last_update == last_update


class TestRateLimiter:
    """Test suite for RateLimiter class."""
    
    def test_rate_limiter_initialization_default_burst(self):
        """Test RateLimiter initialization with default burst limit."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0)
        
        assert limiter.rate == 10.0
        assert limiter.capacity == 10  # Should equal rate_limit when burst_limit not specified
        assert limiter._bucket.capacity == 10
        assert limiter._bucket.tokens == 10.0  # Should start full
        assert limiter._bucket.rate == 10.0
    
    def test_rate_limiter_initialization_custom_burst(self):
        """Test RateLimiter initialization with custom burst limit."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0, burst_limit=20)
        
        assert limiter.rate == 10.0
        assert limiter.capacity == 20
        assert limiter._bucket.capacity == 20
        assert limiter._bucket.tokens == 20.0  # Should start full
        assert limiter._bucket.rate == 10.0
    
    def test_rate_limiter_different_time_windows(self):
        """Test RateLimiter with different time windows."""
        # 60 requests per minute = 1 request per second
        limiter_minute = RateLimiter(rate_limit=60, time_window=60.0)
        assert limiter_minute.rate == 1.0
        
        # 100 requests per 10 seconds = 10 requests per second
        limiter_10s = RateLimiter(rate_limit=100, time_window=10.0)
        assert limiter_10s.rate == 10.0
    
    def test_acquire_tokens_success(self):
        """Test successful token acquisition."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0)
        
        # Should be able to acquire tokens initially
        result = limiter.acquire(tokens=1)
        assert result is True
        
        result = limiter.acquire(tokens=5)
        assert result is True
        
        # Check available tokens decreased
        assert limiter.available_tokens < 10.0
    
    def test_acquire_tokens_exhaustion(self):
        """Test token acquisition when bucket is exhausted."""
        limiter = RateLimiter(rate_limit=5, time_window=1.0)
        
        # Exhaust all tokens
        result = limiter.acquire(tokens=5)
        assert result is True
        
        # Try to acquire more with zero timeout - should fail
        result = limiter.try_acquire(tokens=1)
        assert result is False
    
    def test_acquire_more_than_capacity(self):
        """Test error when requesting more tokens than capacity."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0)
        
        with pytest.raises(ValueError, match="Requested tokens 15 exceed capacity 10"):
            limiter.acquire(tokens=15)
    
    def test_try_acquire_success_and_failure(self):
        """Test try_acquire method for both success and failure cases."""
        limiter = RateLimiter(rate_limit=3, time_window=1.0)
        
        # Should succeed initially
        assert limiter.try_acquire(tokens=2) is True
        assert limiter.try_acquire(tokens=1) is True
        
        # Should fail when exhausted
        assert limiter.try_acquire(tokens=1) is False
    
    def test_token_bucket_refill_over_time(self):
        """Test that tokens are refilled over time."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0)  # 10 tokens per second
        
        # Exhaust most tokens
        limiter.acquire(tokens=8)
        initial_tokens = limiter.available_tokens
        
        # Wait and check tokens increased
        time.sleep(0.2)  # Wait 200ms, should get ~2 more tokens
        
        current_tokens = limiter.available_tokens
        assert current_tokens > initial_tokens
        assert current_tokens <= limiter.capacity
    
    def test_token_bucket_capacity_limit(self):
        """Test that token bucket doesn't exceed capacity."""
        limiter = RateLimiter(rate_limit=5, time_window=1.0, burst_limit=10)
        
        # Start with some tokens used
        limiter.acquire(tokens=3)
        
        # Wait longer than needed to refill
        time.sleep(2.0)  # Wait 2 seconds, more than enough to refill
        
        # Tokens should not exceed capacity
        tokens = limiter.available_tokens
        assert tokens <= limiter.capacity
        assert tokens == limiter.capacity  # Should be exactly at capacity
    
    def test_acquire_with_timeout_success(self):
        """Test acquire with timeout that succeeds after waiting."""
        limiter = RateLimiter(rate_limit=2, time_window=1.0)  # 2 tokens per second
        
        # Exhaust tokens
        limiter.acquire(tokens=2)
        
        # Acquire with timeout - should succeed after waiting
        start_time = time.time()
        result = limiter.acquire(tokens=1, timeout=1.0)
        end_time = time.time()
        
        assert result is True
        assert end_time - start_time >= 0.4  # Should have waited at least ~0.5 seconds
        assert end_time - start_time < 1.0   # But less than full timeout
    
    def test_acquire_with_timeout_failure(self):
        """Test acquire with timeout that fails."""
        limiter = RateLimiter(rate_limit=1, time_window=1.0)  # 1 token per second
        
        # Exhaust tokens
        limiter.acquire(tokens=1)
        
        # Try to acquire more tokens than can be generated in timeout period
        start_time = time.time()
        result = limiter.acquire(tokens=5, timeout=0.1)  # Very short timeout
        end_time = time.time()
        
        assert result is False
        assert end_time - start_time >= 0.09  # Should have waited close to timeout
        assert end_time - start_time <= 0.2   # But not much longer
    
    @pytest.mark.asyncio
    async def test_acquire_async_success(self):
        """Test async token acquisition success."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0)
        
        result = await limiter.acquire_async(tokens=3)
        assert result is True
        
        # Check tokens were consumed
        remaining = limiter.available_tokens
        assert remaining == 7.0
    
    @pytest.mark.asyncio
    async def test_acquire_async_with_wait(self):
        """Test async token acquisition with waiting."""
        limiter = RateLimiter(rate_limit=2, time_window=1.0)  # 2 tokens per second
        
        # Exhaust tokens
        await limiter.acquire_async(tokens=2)
        
        # Acquire more - should wait and succeed
        start_time = time.time()
        result = await limiter.acquire_async(tokens=1, timeout=1.0)
        end_time = time.time()
        
        assert result is True
        assert end_time - start_time >= 0.4  # Should have waited
        assert end_time - start_time < 1.0
    
    @pytest.mark.asyncio
    async def test_acquire_async_timeout(self):
        """Test async token acquisition timeout."""
        limiter = RateLimiter(rate_limit=1, time_window=1.0)
        
        # Exhaust tokens
        await limiter.acquire_async(tokens=1)
        
        # Try to acquire more than can be generated in timeout
        result = await limiter.acquire_async(tokens=5, timeout=0.1)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_acquire_async_more_than_capacity(self):
        """Test async acquire with more tokens than capacity."""
        limiter = RateLimiter(rate_limit=5, time_window=1.0)
        
        with pytest.raises(ValueError, match="Requested tokens 10 exceed capacity 5"):
            await limiter.acquire_async(tokens=10)
    
    def test_window_operations(self):
        """Test window-based operations for tracking requests."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0)
        window_id = 1
        
        # Initially no operations in window
        count = limiter.get_window_count(window_id)
        assert count == 0
        
        # Record some operations
        limiter.record_operation(window_id)
        limiter.record_operation(window_id)
        limiter.record_operation(window_id)
        
        count = limiter.get_window_count(window_id)
        assert count == 3
        
        # Different window should be independent
        limiter.record_operation(window_id + 1)
        assert limiter.get_window_count(window_id) == 3
        assert limiter.get_window_count(window_id + 1) == 1
    
    def test_window_cleanup_over_time(self):
        """Test that old window entries are cleaned up."""
        limiter = RateLimiter(rate_limit=10, time_window=0.1)  # Very short window
        window_id = 1
        
        # Record operations
        limiter.record_operation(window_id)
        limiter.record_operation(window_id)
        assert limiter.get_window_count(window_id) == 2
        
        # Wait for window to expire
        time.sleep(0.15)
        
        # Old operations should be cleaned up
        count = limiter.get_window_count(window_id)
        assert count == 0
    
    def test_reset_limiter(self):
        """Test resetting the rate limiter to initial state."""
        limiter = RateLimiter(rate_limit=5, time_window=1.0)
        
        # Use some tokens and record operations
        limiter.acquire(tokens=3)
        limiter.record_operation(1)
        limiter.record_operation(2)
        
        # Verify state is modified
        assert limiter.available_tokens < 5.0
        assert limiter.get_window_count(1) == 1
        assert limiter.get_window_count(2) == 1
        
        # Reset and verify clean state
        limiter.reset()
        
        assert limiter.available_tokens == 5.0
        assert limiter.get_window_count(1) == 0
        assert limiter.get_window_count(2) == 0
    
    def test_available_tokens_property(self):
        """Test available_tokens property updates correctly."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0)
        
        # Initially should be at capacity
        assert limiter.available_tokens == 10.0
        
        # After acquiring tokens
        limiter.acquire(tokens=3)
        assert limiter.available_tokens == 7.0
        
        # After time passes, should increase
        time.sleep(0.1)
        tokens_after_wait = limiter.available_tokens
        assert tokens_after_wait > 7.0
        assert tokens_after_wait <= 10.0


class TestRateLimiterConcurrency:
    """Test suite for RateLimiter thread safety."""
    
    def test_concurrent_acquire(self):
        """Test concurrent token acquisition from multiple threads."""
        limiter = RateLimiter(rate_limit=100, time_window=1.0)  # High rate for testing
        successful_acquires = []
        failed_acquires = []
        
        def worker():
            for _ in range(10):
                if limiter.try_acquire(tokens=1):
                    successful_acquires.append(1)
                else:
                    failed_acquires.append(1)
        
        # Start multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Should have some successful acquires, total shouldn't exceed capacity
        assert len(successful_acquires) > 0
        assert len(successful_acquires) <= limiter.capacity
        assert len(successful_acquires) + len(failed_acquires) == 50  # 5 threads * 10 attempts
    
    def test_concurrent_window_operations(self):
        """Test concurrent window operations."""
        limiter = RateLimiter(rate_limit=10, time_window=1.0)
        operations_completed = []
        
        def record_worker(window_id):
            for i in range(20):
                limiter.record_operation(window_id)
                operations_completed.append(f"record_{window_id}_{i}")
        
        def count_worker(window_id):
            for i in range(10):
                count = limiter.get_window_count(window_id)
                operations_completed.append(f"count_{window_id}_{count}")
                time.sleep(0.001)
        
        threads = []
        
        # Start record and count workers for different windows
        for window_id in range(3):
            threads.append(threading.Thread(target=record_worker, args=(window_id,)))
            threads.append(threading.Thread(target=count_worker, args=(window_id,)))
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Verify operations completed without errors
        record_ops = [op for op in operations_completed if op.startswith("record")]
        count_ops = [op for op in operations_completed if op.startswith("count")]
        
        assert len(record_ops) == 60  # 3 windows * 20 records each
        assert len(count_ops) == 30   # 3 windows * 10 counts each
    
    def test_thread_safety_with_reset(self):
        """Test thread safety when reset is called during operations."""
        limiter = RateLimiter(rate_limit=50, time_window=1.0)
        operations = []
        
        def acquire_worker():
            for _ in range(100):
                try:
                    result = limiter.try_acquire(tokens=1)
                    operations.append(f"acquire_{result}")
                except Exception as e:
                    operations.append(f"error_{e}")
        
        def reset_worker():
            time.sleep(0.1)  # Let some acquires happen first
            limiter.reset()
            operations.append("reset_done")
        
        threads = [
            threading.Thread(target=acquire_worker),
            threading.Thread(target=reset_worker)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have reset operation and no errors
        reset_ops = [op for op in operations if op == "reset_done"]
        error_ops = [op for op in operations if op.startswith("error")]
        
        assert len(reset_ops) == 1
        assert len(error_ops) == 0