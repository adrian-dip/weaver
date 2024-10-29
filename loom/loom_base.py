from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Tuple
from dataclasses import dataclass

@dataclass
class PipelineStep:
    """
    Represents a single step in a data pipeline.
    
    Attributes:
        yarn_name: The name of the Yarn to execute
        query_template: The template string for the query
        params: Parameters to be passed to the query
        priority: Optional priority level for the retriever/ranker
        cache_key: Optional key for caching the results
        timeout: Optional timeout in seconds for the step execution
    """
    yarn_name: str
    query_template: str
    params: Dict[str, Any]
    priority: Optional[float] = None
    cache_key: Optional[str] = None
    timeout: Optional[float] = None

class LoomException(Exception):
    """Base exception class for all Loom-related errors."""
    pass

class LoomBase(ABC):
    """
    Abstract base class for all Loom implementations.
    
    The Loom is responsible for orchestrating the data flow through the pipeline,
    managing Yarns (data retrievers), Fabrics (connection managers), and
    coordinating with the Shuttle (messaging/caching).
    """
    
    def __init__(self):
        self._yarns: Dict[str, Any] = {}
        self._fabrics: Dict[str, Any] = {}
        self._shuttle = None
        self._initialized = False
    
    @abstractmethod
    def weave(self, pipeline_config: List[PipelineStep]) -> Any:
        """
        Orchestrate the data flow through the pipeline.
        
        Args:
            pipeline_config: List of PipelineStep objects defining the pipeline
            
        Returns:
            The result of the data pipeline execution
            
        Raises:
            LoomException: If any errors occur during pipeline execution
        """
        pass
    
    def register_yarn(self, yarn_name: str, yarn_instance: Any) -> None:
        """
        Register a Yarn instance with the Loom.
        
        Args:
            yarn_name: Unique identifier for the Yarn
            yarn_instance: The Yarn instance to register
            
        Raises:
            LoomException: If a Yarn with the same name already exists
        """
        if yarn_name in self._yarns:
            raise LoomException(f"Yarn '{yarn_name}' already registered")
        self._yarns[yarn_name] = yarn_instance
    
    def register_fabric(self, fabric_name: str, fabric_instance: Any) -> None:
        """
        Register a Fabric instance with the Loom.
        
        Args:
            fabric_name: Unique identifier for the Fabric
            fabric_instance: The Fabric instance to register
            
        Raises:
            LoomException: If a Fabric with the same name already exists
        """
        if fabric_name in self._fabrics:
            raise LoomException(f"Fabric '{fabric_name}' already registered")
        self._fabrics[fabric_name] = fabric_instance
    
    def register_shuttle(self, shuttle_instance: Any) -> None:
        """
        Register a Shuttle instance with the Loom.
        
        Args:
            shuttle_instance: The Shuttle instance to register
            
        Raises:
            LoomException: If a Shuttle is already registered
        """
        if self._shuttle is not None:
            raise LoomException("Shuttle already registered")
        self._shuttle = shuttle_instance
    
    def get_yarn(self, yarn_name: str) -> Any:
        """
        Retrieve a registered Yarn instance.
        
        Args:
            yarn_name: Name of the Yarn to retrieve
            
        Returns:
            The registered Yarn instance
            
        Raises:
            LoomException: If no Yarn exists with the given name
        """
        if yarn_name not in self._yarns:
            raise LoomException(f"Yarn '{yarn_name}' not found")
        return self._yarns[yarn_name]
    
    def get_fabric(self, fabric_name: str) -> Any:
        """
        Retrieve a registered Fabric instance.
        
        Args:
            fabric_name: Name of the Fabric to retrieve
            
        Returns:
            The registered Fabric instance
            
        Raises:
            LoomException: If no Fabric exists with the given name
        """
        if fabric_name not in self._fabrics:
            raise LoomException(f"Fabric '{fabric_name}' not found")
        return self._fabrics[fabric_name]
    
    def get_shuttle(self) -> Any:
        """
        Retrieve the registered Shuttle instance.
        
        Returns:
            The registered Shuttle instance
            
        Raises:
            LoomException: If no Shuttle is registered
        """
        if self._shuttle is None:
            raise LoomException("No Shuttle registered")
        return self._shuttle
    
    @property
    def initialized(self) -> bool:
        """Check if the Loom has been properly initialized."""
        return self._initialized
    
    def initialize(self) -> None:
        """
        Initialize the Loom after all components have been registered.
        
        This method should be called after registering all required Yarns,
        Fabrics, and the Shuttle, but before calling weave().
        
        Raises:
            LoomException: If initialization fails
        """
        if not self._yarns:
            raise LoomException("No Yarns registered")
        if not self._fabrics:
            raise LoomException("No Fabrics registered")
        if self._shuttle is None:
            raise LoomException("No Shuttle registered")
        
        self._initialized = True