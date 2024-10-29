from typing import Any, List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import logging
from .loom_base import LoomBase, LoomException, PipelineStep

logger = logging.getLogger(__name__)

class ParallelLoom(LoomBase):
    """
    A parallel implementation of the Loom pipeline orchestrator.
    
    This implementation executes pipeline steps concurrently using a thread pool.
    Steps are executed in parallel when possible, while respecting dependencies.
    """
    
    def __init__(self, max_workers: int = 4):
        """
        Initialize the ParallelLoom.
        
        Args:
            max_workers: Maximum number of worker threads in the thread pool
        """
        super().__init__()
        self._max_workers = max_workers
        self._executor: Optional[ThreadPoolExecutor] = None
        self._results_cache: Dict[str, Any] = {}
    
    def initialize(self) -> None:
        """Initialize the ParallelLoom and create the thread pool."""
        super().initialize()
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
    
    def _execute_step(self, step: PipelineStep) -> Any:
        """
        Execute a single pipeline step.
        
        Args:
            step: The PipelineStep to execute
            
        Returns:
            The result of the step execution
            
        Raises:
            LoomException: If any errors occur during step execution
        """
        shuttle = self.get_shuttle()
        
        if step.cache_key and shuttle.exists(step.cache_key):
            logger.debug(f"Cache hit for key: {step.cache_key}")
            return shuttle.get(step.cache_key)
        
        yarn = self.get_yarn(step.yarn_name)
        
        try:
            result = yarn.query(
                query_template=step.query_template,
                params=step.params
            )
            
            if step.cache_key:
                shuttle.set(step.cache_key, result)
            
            return result
            
        except Exception as e:
            raise LoomException(
                f"Error executing step with yarn '{step.yarn_name}': {str(e)}"
            ) from e
    
    def weave(self, pipeline_config: List[PipelineStep]) -> List[Any]:
        """
        Execute the pipeline steps in parallel where possible.
        
        Args:
            pipeline_config: List of PipelineStep objects defining the pipeline
            
        Returns:
            List of results from each pipeline step
            
        Raises:
            LoomException: If any errors occur during pipeline execution
        """
        if not self.initialized:
            raise LoomException("Loom not initialized. Call initialize() first.")
        
        if not self._executor:
            raise LoomException("Thread pool not initialized")
            
        results = [None] * len(pipeline_config)
        futures_map = {}
        
        try:
            for i, step in enumerate(pipeline_config):
                future = self._executor.submit(self._execute_step, step)
                futures_map[future] = i
            
            for future in as_completed(futures_map.keys()):
                step_index = futures_map[future]
                step = pipeline_config[step_index]
                
                try:
                    if step.timeout:
                        result = future.result(timeout=step.timeout)
                    else:
                        result = future.result()
                        
                    results[step_index] = result
                    
                except TimeoutError:
                    raise LoomException(
                        f"Step '{step.yarn_name}' timed out after {step.timeout} seconds"
                    )
                except Exception as e:
                    raise LoomException(
                        f"Error in step '{step.yarn_name}': {str(e)}"
                    ) from e
                    
        except Exception as e:
            raise LoomException(f"Pipeline execution failed: {str(e)}") from e
            
        return results
    
    def __del__(self):
        """Clean up resources when the ParallelLoom is destroyed."""
        if self._executor:
            self._executor.shutdown(wait=True)