class LoomException(Exception):
    """Base exception class for all Loom-related errors."""
    pass

class LoomFactoryException(LoomException):
    """Exception raised for errors in the LoomFactory."""
    pass

class LoomInitializationError(LoomException):
    """Exception raised when Loom initialization fails."""
    pass

class LoomExecutionError(LoomException):
    """Exception raised when pipeline execution fails."""
    pass

class LoomTimeoutError(LoomException):
    """Exception raised when a pipeline step times out."""
    pass

class LoomConfigurationError(LoomException):
    """Exception raised for configuration-related errors."""
    pass