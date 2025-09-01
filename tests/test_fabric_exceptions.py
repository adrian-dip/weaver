# test_fabric_exceptions.py
import pytest
from datetime import datetime, timedelta
from fabric_exceptions import (
    FabricException, FabricConfigError, FabricConnectionError, 
    FabricPoolError, FabricAuthenticationError, FabricTimeoutError,
    FabricResourceError, FabricOperationError, FabricRateLimitError,
    FabricCleanupError
)


class TestFabricException:
    """Test suite for base FabricException class."""
    
    def test_fabric_exception_with_message_and_details(self):
        """Test FabricException creation with message and details."""
        message = "Database connection failed"
        details = {"host": "localhost", "port": 5432, "database": "test_db"}
        
        exc = FabricException(message, details)
        
        assert exc.message == message
        assert exc.details == details
        assert exc.details["host"] == "localhost"
        assert exc.details["port"] == 5432
        assert exc.details["database"] == "test_db"
        assert isinstance(exc.timestamp, datetime)
        assert str(exc) == message
        # Verify timestamp is recent
        assert datetime.now() - exc.timestamp < timedelta(seconds=1)
    
    def test_fabric_exception_without_details(self):
        """Test FabricException creation without details defaults to empty dict."""
        message = "Simple error occurred"
        exc = FabricException(message)
        
        assert exc.message == message
        assert exc.details == {}
        assert isinstance(exc.details, dict)
        assert len(exc.details) == 0
        assert isinstance(exc.timestamp, datetime)
    
    def test_fabric_exception_empty_details(self):
        """Test FabricException with explicitly empty details."""
        message = "Error with empty details"
        exc = FabricException(message, {})
        
        assert exc.message == message
        assert exc.details == {}
        assert len(exc.details) == 0
    
    def test_fabric_exception_inheritance(self):
        """Test FabricException inherits from Exception."""
        exc = FabricException("test message")
        
        assert isinstance(exc, Exception)
        assert isinstance(exc, FabricException)


class TestFabricConfigError:
    """Test suite for FabricConfigError class."""
    
    def test_config_error_with_section_and_details(self):
        """Test FabricConfigError with config section and details."""
        message = "Invalid connection string format"
        section = "database.primary"
        details = {"field": "connection_string", "expected": "postgresql://...", "actual": "invalid"}
        
        exc = FabricConfigError(message, section, details)
        
        assert exc.message == message
        assert exc.config_section == section
        assert exc.details == details
        assert exc.details["field"] == "connection_string"
        assert isinstance(exc, FabricException)
    
    def test_config_error_without_section(self):
        """Test FabricConfigError without config section."""
        message = "Missing required configuration"
        details = {"missing_fields": ["host", "port"]}
        
        exc = FabricConfigError(message, details=details)
        
        assert exc.message == message
        assert exc.config_section is None
        assert exc.details == details
    
    def test_config_error_minimal(self):
        """Test FabricConfigError with only message."""
        message = "Configuration error"
        exc = FabricConfigError(message)
        
        assert exc.message == message
        assert exc.config_section is None
        assert exc.details == {}


class TestFabricConnectionError:
    """Test suite for FabricConnectionError class."""
    
    def test_connection_error_full_attributes(self):
        """Test FabricConnectionError with all attributes."""
        message = "Connection pool exhausted"
        connection_id = "conn_12345"
        pool_name = "primary_pool"
        details = {"active_connections": 10, "max_connections": 10}
        retry_allowed = False
        
        exc = FabricConnectionError(message, connection_id, pool_name, details, retry_allowed)
        
        assert exc.message == message
        assert exc.connection_id == connection_id
        assert exc.pool_name == pool_name
        assert exc.details == details
        assert exc.retry_allowed is False
        assert isinstance(exc, FabricException)
    
    def test_connection_error_defaults(self):
        """Test FabricConnectionError with default values."""
        message = "Connection failed"
        exc = FabricConnectionError(message)
        
        assert exc.message == message
        assert exc.connection_id is None
        assert exc.pool_name is None
        assert exc.details == {}
        assert exc.retry_allowed is True  # Default should be True
    
    def test_connection_error_retry_allowed_true(self):
        """Test connection error with retry allowed."""
        message = "Temporary connection failure"
        exc = FabricConnectionError(message, retry_allowed=True)
        
        assert exc.retry_allowed is True
        assert exc.message == message


class TestFabricAuthenticationError:
    """Test suite for FabricAuthenticationError class."""
    
    def test_authentication_error_full_attributes(self):
        """Test FabricAuthenticationError with all attributes."""
        message = "Invalid API key"
        auth_type = "api_key"
        provider = "oauth2_provider"
        details = {"key_prefix": "sk_test_", "expiry": "2024-12-31"}
        
        exc = FabricAuthenticationError(message, auth_type, provider, details)
        
        assert exc.message == message
        assert exc.auth_type == auth_type
        assert exc.provider == provider
        assert exc.details == details
        assert exc.retry_allowed is False  # Should always be False for auth errors
        assert isinstance(exc, FabricConnectionError)
    
    def test_authentication_error_inheritance(self):
        """Test FabricAuthenticationError inherits from FabricConnectionError."""
        exc = FabricAuthenticationError("Auth failed")
        
        assert isinstance(exc, FabricConnectionError)
        assert isinstance(exc, FabricException)
        assert exc.retry_allowed is False
    
    def test_authentication_error_different_auth_types(self):
        """Test authentication error with different auth types."""
        auth_types = ["basic", "bearer", "api_key", "oauth2"]
        
        for auth_type in auth_types:
            exc = FabricAuthenticationError(f"{auth_type} auth failed", auth_type)
            assert exc.auth_type == auth_type
            assert exc.retry_allowed is False


class TestFabricRateLimitError:
    """Test suite for FabricRateLimitError class."""
    
    def test_rate_limit_error_with_reset_time(self):
        """Test FabricRateLimitError with reset time."""
        message = "API rate limit exceeded"
        limit = 100
        window = 60.0
        reset_time = datetime.now() + timedelta(minutes=1)
        details = {"requests_made": 100, "endpoint": "/api/v1/data"}
        
        exc = FabricRateLimitError(message, limit, window, reset_time, details)
        
        assert exc.message == message
        assert exc.limit == limit
        assert exc.window == window
        assert exc.reset_time == reset_time
        assert exc.details == details
        assert exc.resource_type == "rate_limit"
        assert isinstance(exc, FabricResourceError)
    
    def test_rate_limit_error_without_reset_time(self):
        """Test FabricRateLimitError without reset time."""
        message = "Rate limit hit"
        limit = 50
        window = 30.0
        
        exc = FabricRateLimitError(message, limit, window)
        
        assert exc.limit == limit
        assert exc.window == window
        assert exc.reset_time is None
        assert exc.resource_type == "rate_limit"
    
    def test_rate_limit_error_inheritance(self):
        """Test FabricRateLimitError inheritance chain."""
        exc = FabricRateLimitError("Rate limited", 10, 1.0)
        
        assert isinstance(exc, FabricResourceError)
        assert isinstance(exc, FabricException)


class TestFabricTimeoutError:
    """Test suite for FabricTimeoutError class."""
    
    def test_timeout_error_full_attributes(self):
        """Test FabricTimeoutError with all attributes."""
        message = "Operation timed out"
        operation = "database_query"
        timeout_value = 30.0
        elapsed_time = 35.2
        details = {"query": "SELECT * FROM large_table", "rows_processed": 1000000}
        
        exc = FabricTimeoutError(message, operation, timeout_value, elapsed_time, details)
        
        assert exc.message == message
        assert exc.operation == operation
        assert exc.timeout_value == timeout_value
        assert exc.elapsed_time == elapsed_time
        assert exc.details == details
        assert exc.elapsed_time > exc.timeout_value  # Verify timeout actually occurred
    
    def test_timeout_error_minimal(self):
        """Test FabricTimeoutError with minimal attributes."""
        message = "Timeout occurred"
        operation = "api_call"
        
        exc = FabricTimeoutError(message, operation)
        
        assert exc.message == message
        assert exc.operation == operation
        assert exc.timeout_value is None
        assert exc.elapsed_time is None
        assert exc.details == {}


class TestFabricPoolError:
    """Test suite for FabricPoolError class."""
    
    def test_pool_error_with_sizes(self):
        """Test FabricPoolError with current and max sizes."""
        message = "Pool size exceeded"
        pool_name = "connection_pool_1"
        current_size = 15
        max_size = 10
        details = {"waiting_requests": 5}
        
        exc = FabricPoolError(message, pool_name, current_size, max_size, details)
        
        assert exc.message == message
        assert exc.pool_name == pool_name
        assert exc.current_size == current_size
        assert exc.max_size == max_size
        assert exc.details == details
        assert exc.current_size > exc.max_size  # Verify over-allocation
    
    def test_pool_error_minimal(self):
        """Test FabricPoolError with only required attributes."""
        message = "Pool error"
        pool_name = "test_pool"
        
        exc = FabricPoolError(message, pool_name)
        
        assert exc.message == message
        assert exc.pool_name == pool_name
        assert exc.current_size is None
        assert exc.max_size is None
        assert exc.details == {}


class TestFabricCleanupError:
    """Test suite for FabricCleanupError class."""
    
    def test_cleanup_error_partial_success(self):
        """Test FabricCleanupError with partial cleanup success."""
        message = "Failed to cleanup all resources"
        cleanup_target = "database_connections"
        partial_cleanup = True
        details = {"cleaned": 8, "failed": 2, "total": 10}
        
        exc = FabricCleanupError(message, cleanup_target, partial_cleanup, details)
        
        assert exc.message == message
        assert exc.cleanup_target == cleanup_target
        assert exc.partial_cleanup is True
        assert exc.details == details
        assert exc.details["cleaned"] + exc.details["failed"] == exc.details["total"]
    
    def test_cleanup_error_complete_failure(self):
        """Test FabricCleanupError with complete failure."""
        message = "Complete cleanup failure"
        cleanup_target = "api_clients"
        
        exc = FabricCleanupError(message, cleanup_target, partial_cleanup=False)
        
        assert exc.message == message
        assert exc.cleanup_target == cleanup_target
        assert exc.partial_cleanup is False
        assert exc.details == {}