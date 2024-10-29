from typing import Any, List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor
import logging
from .loom_base import LoomBase, LoomException, PipelineStep

logger = logging.getLogger(__name__)

class SimpleLoom(LoomBase):
    """
    A simple sequential implementation of the Loom pipeline orchestrator.
    
    This implementation executes pipeline steps one after another in sequence.
    Results from each step can be cached and reused if specified in the PipelineStep.
    """
    
    def __init__(self):
        super().__init__()
        self._results_cache: Dict[str, Any] = {}
    
    def weave(self, pipeline_config: List[PipelineStep]) -> List[Any]:
        """
        Execute the pipeline steps sequentially.
        
        Args:
            pipeline_config: List of PipelineStep objects defining the pipeline
            
        Returns:
            List of results from each pipeline step
            
        Raises:
            LoomException: If any errors occur during pipeline execution
        """
        if not self.initialized:
            raise LoomException("Loom not initialized. Call initialize() first.")
            
        results = []
        shuttle = self.get_shuttle()
        
        try:
            for step in pipeline_config:
                if step.cache_key and shuttle.exists(step.cache_key):
                    logger.debug(f"Cache hit for key: {step.cache_key}")
                    result = shuttle.get(step.cache_key)
                    results.append(result)
                    continue
                
                yarn = self.get_yarn(step.yarn_name)
                
                try:
                    result = yarn.query(
                        query_template=step.query_template,
                        params=step.params
                    )
                    
                    if step.cache_key:
                        shuttle.set(step.cache_key, result)
                    
                    results.append(result)
                    
                except Exception as e:
                    raise LoomException(
                        f"Error executing step with yarn '{step.yarn_name}': {str(e)}"
                    ) from e
                    
        except Exception as e:
            raise LoomException(f"Pipeline execution failed: {str(e)}") from e
            
        return results
