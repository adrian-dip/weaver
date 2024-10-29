from typing import Optional, Dict, Any
from datetime import datetime

class FabricException(Exception):
    """
    Base exception class for all Fabric-related errors.
    Provides a consistent interface for error handling across all Fabric implementations.
    """
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the exception with a message and optional details.
        
        Args:
            message: Human-readable error message
            details: Optional dictionary containing additional error details
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now()

class FabricConfigError(FabricException):
    """
    Raised when there is an error in Fabric configuration.
    Examples:
    - Missing required configuration fields
    - Invalid configuration values
    - Unsupported database/API types
    - YAML configuration parsing errors
    """
    
    def __init__(self, message: str, config_section: Optional[str] = None, 
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the configuration error.
        
        Args:
            message: Human-readable error message
            config_section: Section of configuration where the error occurred
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.config_section = config_section

class FabricConnectionError(FabricException):
    """
    Raised when there is an error establishing or maintaining a connection.
    Examples:
    - Database connection failures
    - Pool initialization errors
    - Connection timeout
    - Pool exhaustion
    """
    
    def __init__(self, message: str, connection_id: Optional[str] = None,
                 pool_name: Optional[str] = None, 
                 details: Optional[Dict[str, Any]] = None,
                 retry_allowed: bool = True):
        """
        Initialize the connection error.
        
        Args:
            message: Human-readable error message
            connection_id: Identifier of the failed connection
            pool_name: Name of the connection pool
            details: Optional dictionary containing additional error details
            retry_allowed: Whether the operation can be retried
        """
        super().__init__(message, details)
        self.connection_id = connection_id
        self.pool_name = pool_name
        self.retry_allowed = retry_allowed

class FabricPoolError(FabricException):
    """
    Raised when there is an error related to connection pool management.
    Examples:
    - Pool initialization failures
    - Pool size limits exceeded
    - Pool cleanup errors
    - Invalid pool configuration
    """
    
    def __init__(self, message: str, pool_name: str, 
                 current_size: Optional[int] = None,
                 max_size: Optional[int] = None,
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the pool error.
        
        Args:
            message: Human-readable error message
            pool_name: Name of the affected connection pool
            current_size: Current size of the pool when error occurred
            max_size: Maximum allowed size of the pool
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.pool_name = pool_name
        self.current_size = current_size
        self.max_size = max_size

class FabricAuthenticationError(FabricConnectionError):
    """
    Raised when there is an authentication or authorization error.
    Examples:
    - Invalid credentials
    - Expired tokens
    - Insufficient permissions
    - Missing required authentication parameters
    """
    
    def __init__(self, message: str, auth_type: Optional[str] = None,
                 provider: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the authentication error.
        
        Args:
            message: Human-readable error message
            auth_type: Type of authentication that failed (e.g., 'basic', 'bearer', 'api_key')
            provider: Authentication provider or service
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details=details, retry_allowed=False)
        self.auth_type = auth_type
        self.provider = provider

class FabricTimeoutError(FabricException):
    """
    Raised when an operation times out.
    Examples:
    - Connection establishment timeout
    - Pool acquisition timeout
    - Operation execution timeout
    """
    
    def __init__(self, message: str, operation: str,
                 timeout_value: Optional[float] = None,
                 elapsed_time: Optional[float] = None,
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the timeout error.
        
        Args:
            message: Human-readable error message
            operation: Operation that timed out
            timeout_value: The timeout value that was exceeded (in seconds)
            elapsed_time: Actual time elapsed before timeout
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.operation = operation
        self.timeout_value = timeout_value
        self.elapsed_time = elapsed_time

class FabricResourceError(FabricException):
    """
    Raised when there is an error related to resource management.
    Examples:
    - Memory limits exceeded
    - Rate limits exceeded
    - Resource cleanup failures
    - Resource allocation failures
    """
    
    def __init__(self, message: str, resource_type: str,
                 resource_id: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the resource error.
        
        Args:
            message: Human-readable error message
            resource_type: Type of resource that caused the error
            resource_id: Identifier of the specific resource
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.resource_type = resource_type
        self.resource_id = resource_id

class FabricOperationError(FabricException):
    """
    Raised when a Fabric operation fails.
    Examples:
    - Connection release failures
    - Pool shutdown errors
    - Metric collection failures
    - Health check failures
    """
    
    def __init__(self, message: str, operation: str,
                 connection_id: Optional[str] = None,
                 pool_name: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the operation error.
        
        Args:
            message: Human-readable error message
            operation: Name of the failed operation
            connection_id: Identifier of the connection involved
            pool_name: Name of the connection pool involved
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.operation = operation
        self.connection_id = connection_id
        self.pool_name = pool_name

class FabricRateLimitError(FabricResourceError):
    """
    Raised when rate limits are exceeded.
    Examples:
    - API rate limit exceeded
    - Database query rate limit exceeded
    - Connection rate limit exceeded
    """
    
    def __init__(self, message: str, limit: int,
                 window: float,
                 reset_time: Optional[datetime] = None,
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the rate limit error.
        
        Args:
            message: Human-readable error message
            limit: Rate limit that was exceeded
            window: Time window for the rate limit (in seconds)
            reset_time: When the rate limit will reset
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, resource_type="rate_limit", details=details)
        self.limit = limit
        self.window = window
        self.reset_time = reset_time

class FabricCleanupError(FabricException):
    """
    Raised when there is an error during resource cleanup.
    Examples:
    - Connection cleanup failures
    - Pool shutdown errors
    - Resource deallocation failures
    """
    
    def __init__(self, message: str, cleanup_target: str,
                 partial_cleanup: bool = False,
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the cleanup error.
        
        Args:
            message: Human-readable error message
            cleanup_target: Target resource being cleaned up
            partial_cleanup: Whether partial cleanup was successful
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.cleanup_target = cleanup_target
        self.partial_cleanup = partial_cleanup