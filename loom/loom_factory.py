from typing import Dict, Any, Optional, Type
from .loom_base import LoomBase
from .simple_loom import SimpleLoom
from .parallel_loom import ParallelLoom
from .loom_exceptions import LoomFactoryException

class LoomFactory:
    """
    Factory class for creating Loom instances based on configuration.
    
    This factory manages the creation and configuration of different Loom
    implementations. It ensures proper initialization and setup of Loom
    instances based on the provided configuration.
    """
    
    _implementations: Dict[str, Type[LoomBase]] = {
        'simple': SimpleLoom,
        'parallel': ParallelLoom
    }
    
    @classmethod
    def register_implementation(cls, name: str, implementation: Type[LoomBase]) -> None:
        """
        Register a new Loom implementation with the factory.
        
        Args:
            name: Unique identifier for the implementation
            implementation: The Loom implementation class to register
            
        Raises:
            LoomFactoryException: If an implementation with the same name exists
        """
        if name in cls._implementations:
            raise LoomFactoryException(
                f"Loom implementation '{name}' already registered"
            )
        cls._implementations[name] = implementation
    
    @classmethod
    def create_loom(
        cls,
        implementation_type: str,
        config: Dict[str, Any],
        yarns: Optional[Dict[str, Any]] = None,
        fabrics: Optional[Dict[str, Any]] = None,
        shuttle: Optional[Any] = None
    ) -> LoomBase:
        """
        Create and configure a new Loom instance.
        
        Args:
            implementation_type: Type of Loom to create ('simple' or 'parallel')
            config: Configuration dictionary for the Loom
            yarns: Optional dictionary of Yarn instances to register
            fabrics: Optional dictionary of Fabric instances to register
            shuttle: Optional Shuttle instance to register
            
        Returns:
            Configured Loom instance
            
        Raises:
            LoomFactoryException: If creation or initialization fails
        """
        try:
            # Get the implementation class
            impl_class = cls._implementations.get(implementation_type)
            if impl_class is None:
                raise LoomFactoryException(
                    f"Unknown Loom implementation type: {implementation_type}"
                )
            
            # Create instance with configuration
            if implementation_type == 'parallel':
                max_workers = config.get('max_workers', 4)
                loom = impl_class(max_workers=max_workers)
            else:
                loom = impl_class()
            
            # Register components if provided
            if yarns:
                for name, yarn in yarns.items():
                    loom.register_yarn(name, yarn)
            
            if fabrics:
                for name, fabric in fabrics.items():
                    loom.register_fabric(name, fabric)
            
            if shuttle:
                loom.register_shuttle(shuttle)
            
            # Initialize the Loom
            loom.initialize()
            
            return loom
            
        except Exception as e:
            raise LoomFactoryException(
                f"Failed to create Loom instance: {str(e)}"
            ) from e