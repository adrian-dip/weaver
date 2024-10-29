from typing import Optional, Dict, Any

class YarnException(Exception):
    """
    Base exception class for all Yarn-related errors.
    Provides a consistent interface for error handling across all Yarn implementations.
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

class YarnConfigError(YarnException):
    """
    Raised when there is an error in Yarn configuration.
    Examples:
    - Missing required configuration fields
    - Invalid configuration values
    - Unsupported database/API types
    - YAML configuration parsing errors
    """
    pass

class YarnConnectionError(YarnException):
    """
    Raised when there is an error establishing or maintaining a connection.
    Examples:
    - Database connection failures
    - API endpoint unreachable
    - Authentication failures
    - Connection timeout
    """
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, retry_allowed: bool = True):
        """
        Initialize the connection error.
        
        Args:
            message: Human-readable error message
            details: Optional dictionary containing additional error details
            retry_allowed: Whether the operation can be retried
        """
        super().__init__(message, details)
        self.retry_allowed = retry_allowed

class YarnQueryError(YarnException):
    """
    Raised when there is an error executing a query or request.
    Examples:
    - Invalid query syntax
    - Query timeout
    - Database constraints violation
    - API request failures
    """
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, query: Optional[str] = None):
        """
        Initialize the query error.
        
        Args:
            message: Human-readable error message
            details: Optional dictionary containing additional error details
            query: The query that caused the error (sanitized if contains sensitive data)
        """
        super().__init__(message, details)
        self.query = query

class YarnAuthenticationError(YarnConnectionError):
    """
    Raised when there is an authentication or authorization error.
    Examples:
    - Invalid credentials
    - Expired tokens
    - Insufficient permissions
    - Missing required authentication parameters
    """
    
    def __init__(self, message: str, auth_type: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the authentication error.
        
        Args:
            message: Human-readable error message
            auth_type: Type of authentication that failed (e.g., 'basic', 'bearer', 'api_key')
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details, retry_allowed=False)
        self.auth_type = auth_type

class YarnTimeoutError(YarnException):
    """
    Raised when an operation times out.
    Examples:
    - Query execution timeout
    - Connection establishment timeout
    - API request timeout
    """
    
    def __init__(self, message: str, timeout_value: Optional[float] = None, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the timeout error.
        
        Args:
            message: Human-readable error message
            timeout_value: The timeout value that was exceeded (in seconds)
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.timeout_value = timeout_value

class YarnValidationError(YarnException):
    """
    Raised when there is a validation error in input data or parameters.
    Examples:
    - Invalid parameter types
    - Missing required parameters
    - Parameter value constraints violation
    - Invalid vector dimensions
    """
    
    def __init__(self, message: str, validation_errors: Optional[Dict[str, str]] = None, 
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the validation error.
        
        Args:
            message: Human-readable error message
            validation_errors: Dictionary mapping field names to specific validation errors
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.validation_errors = validation_errors or {}

class YarnResourceError(YarnException):
    """
    Raised when there is an error related to resource management.
    Examples:
    - Connection pool exhaustion
    - Memory limits exceeded
    - Rate limits exceeded
    - Resource cleanup failures
    """
    
    def __init__(self, message: str, resource_type: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the resource error.
        
        Args:
            message: Human-readable error message
            resource_type: Type of resource that caused the error
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.resource_type = resource_type

class YarnDataError(YarnException):
    """
    Raised when there is an error related to data handling or processing.
    Examples:
    - Data type mismatches
    - Data conversion errors
    - Invalid data format
    - Data integrity issues
    """
    
    def __init__(self, message: str, data_type: Optional[str] = None, 
                 operation: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the data error.
        
        Args:
            message: Human-readable error message
            data_type: Type of data that caused the error
            operation: Operation that was being performed on the data
            details: Optional dictionary containing additional error details
        """
        super().__init__(message, details)
        self.data_type = data_type
        self.operation = operation