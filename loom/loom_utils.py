import logging
from typing import Any, Dict, List, Optional
from .loom_base import PipelineStep
from .loom_exceptions import LoomConfigurationError

logger = logging.getLogger(__name__)

def validate_pipeline_config(pipeline_config: List[PipelineStep]) -> None:
    """
    Validate a pipeline configuration.
    
    Args:
        pipeline_config: List of PipelineStep objects to validate
        
    Raises:
        LoomConfigurationError: If the configuration is invalid
    """
    if not pipeline_config:
        raise LoomConfigurationError("Pipeline configuration is empty")
        
    for i, step in enumerate(pipeline_config):
        if not step.yarn_name:
            raise LoomConfigurationError(
                f"Step {i}: Missing yarn_name"
            )
        if not step.query_template:
            raise LoomConfigurationError(
                f"Step {i}: Missing query_template"
            )
        if step.timeout is not None and step.timeout <= 0:
            raise LoomConfigurationError(
                f"Step {i}: Invalid timeout value: {step.timeout}"
            )

def create_pipeline_step(
    yarn_name: str,
    query_template: str,
    params: Dict[str, Any],
    priority: Optional[float] = None,
    cache_key: Optional[str] = None,
    timeout: Optional[float] = None
) -> PipelineStep:
    """
    Create a PipelineStep object with validation.
    
    Args:
        yarn_name: Name of the Yarn to execute
        query_template: Template string for the query
        params: Parameters to pass to the query
        priority: Optional priority level for the retriever/ranker
        cache_key: Optional key for caching results
        timeout: Optional timeout in seconds
        
    Returns:
        Configured PipelineStep object
        
    Raises:
        LoomConfigurationError: If the parameters are invalid
    """
    try:
        step = PipelineStep(
            yarn_name=yarn_name,
            query_template=query_template,
            params=params,
            priority=priority,
            cache_key=cache_key,
            timeout=timeout
        )
        
        # Validate the single step
        validate_pipeline_config([step])
        
        return step
        
    except Exception as e:
        raise LoomConfigurationError(
            f"Failed to create pipeline step: {str(e)}"
        ) from e

def calculate_step_priority(
    step: PipelineStep,
    context: Dict[str, Any]
) -> float:
    """
    Calculate the execution priority for a pipeline step.
    
    This is used by the retriever/ranker to determine which information
    is most important for the LLM.
    
    Args:
        step: The PipelineStep to evaluate
        context: Additional context for priority calculation
        
    Returns:
        Priority score between 0 and 1 (higher is more important)
    """
    if step.priority is not None:
        base_priority = step.priority
    else:
        base_priority = 0.5  
    
    adjustments = []
    
    if step.timeout is not None and step.timeout < 60:
        adjustments.append(0.2)
    
    if step.cache_key is not None:
        adjustments.append(-0.1)
    
    final_priority = base_priority + sum(adjustments)
    return max(0.0, min(1.0, final_priority))