# test_simple_loom.py
import pytest
from unittest.mock import Mock, MagicMock, patch
from simple_loom import SimpleLoom
from loom_base import LoomException, PipelineStep


class TestSimpleLoom:
    """Test suite for SimpleLoom class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.loom = SimpleLoom()
        self.mock_yarn = Mock()
        self.mock_fabric = Mock()
        self.mock_shuttle = Mock()
        
        # Set up mock yarn behavior
        self.mock_yarn.query = Mock(return_value="yarn_result")
        
        # Set up mock shuttle behavior
        self.mock_shuttle.exists = Mock(return_value=False)
        self.mock_shuttle.get = Mock()
        self.mock_shuttle.set = Mock()
        
        # Register components
        self.loom.register_yarn("test_yarn", self.mock_yarn)
        self.loom.register_fabric("test_fabric", self.mock_fabric)
        self.loom.register_shuttle(self.mock_shuttle)
    
    def test_simple_loom_initialization(self):
        """Test SimpleLoom initialization."""
        loom = SimpleLoom()
        
        assert hasattr(loom, '_results_cache')
        assert isinstance(loom._results_cache, dict)
        assert len(loom._results_cache) == 0
        assert loom.initialized is False
    
    def test_weave_single_step_success(self):
        """Test weaving a single step successfully."""
        self.loom.initialize()
        
        pipeline_config = [
            PipelineStep(
                yarn_name="test_yarn",
                query_template="SELECT * FROM table",
                params={"id": 123}
            )
        ]
        
        results = self.loom.weave(pipeline_config)
        
        assert len(results) == 1
        assert results[0] == "yarn_result"
        
        # Verify yarn was called correctly
        self.mock_yarn.query.assert_called_once_with(
            query_template="SELECT * FROM table",
            params={"id": 123}
        )
        
        # Verify cache operations
        self.mock_shuttle.exists.assert_called_once()
    
    def test_weave_multiple_steps_success(self):
        """Test weaving multiple steps successfully."""
        self.loom.initialize()
        
        # Set up different return values for each step
        self.mock_yarn.query.side_effect = ["result1", "result2", "result3"]
        
        pipeline_config = [
            PipelineStep("test_yarn", "query1", {"param1": "value1"}),
            PipelineStep("test_yarn", "query2", {"param2": "value2"}),
            PipelineStep("test_yarn", "query3", {"param3": "value3"})
        ]
        
        results = self.loom.weave(pipeline_config)
        
        assert len(results) == 3
        assert results == ["result1", "result2", "result3"]
        
        # Verify all yarn calls were made
        assert self.mock_yarn.query.call_count == 3
        expected_calls = [
            (("query1",), {"params": {"param1": "value1"}}),
            (("query2",), {"params": {"param2": "value2"}}),
            (("query3",), {"params": {"param3": "value3"}})
        ]
        for i, call in enumerate(self.mock_yarn.query.call_args_list):
            assert call.args[0] == f"query{i+1}"
            assert call.kwargs["params"] == expected_calls[i][1]["params"]
    
    def test_weave_with_cache_hit(self):
        """Test weaving with cache hit."""
        self.loom.initialize()
        
        # Set up cache hit
        self.mock_shuttle.exists.return_value = True
        self.mock_shuttle.get.return_value = "cached_result"
        
        pipeline_config = [
            PipelineStep(
                yarn_name="test_yarn",
                query_template="cached_query",
                params={"id": 456},
                cache_key="test_cache_key"
            )
        ]
        
        results = self.loom.weave(pipeline_config)
        
        assert len(results) == 1
        assert results[0] == "cached_result"
        
        # Verify cache operations
        self.mock_shuttle.exists.assert_called_once_with("test_cache_key")
        self.mock_shuttle.get.assert_called_once_with("test_cache_key")
        
        # Yarn should not be called due to cache hit
        self.mock_yarn.query.assert_not_called()
    
    def test_weave_with_cache_miss_and_set(self):
        """Test weaving with cache miss and subsequent cache set."""
        self.loom.initialize()
        
        # Cache miss
        self.mock_shuttle.exists.return_value = False
        
        pipeline_config = [
            PipelineStep(
                yarn_name="test_yarn",
                query_template="query_to_cache",
                params={"data": "test"},
                cache_key="cache_key_123"
            )
        ]
        
        results = self.loom.weave(pipeline_config)
        
        assert len(results) == 1
        assert results[0] == "yarn_result"
        
        # Verify cache operations
        self.mock_shuttle.exists.assert_called_once_with("cache_key_123")
        self.mock_shuttle.set.assert_called_once_with("cache_key_123", "yarn_result")
        
        # Verify yarn was called
        self.mock_yarn.query.assert_called_once()
    
    def test_weave_mixed_cache_scenarios(self):
        """Test weaving with mixed cache hits and misses."""
        self.loom.initialize()
        
        # Set up mixed cache behavior
        cache_results = [True, False, True]  # hit, miss, hit
        self.mock_shuttle.exists.side_effect = cache_results
        
        # Set up cache get results
        cached_values = ["cached_1", "cached_3"]
        self.mock_shuttle.get.side_effect = cached_values
        
        # Set up yarn result for cache miss
        self.mock_yarn.query.return_value = "fresh_result_2"
        
        pipeline_config = [
            PipelineStep("test_yarn", "query1", {}, cache_key="key1"),
            PipelineStep("test_yarn", "query2", {}, cache_key="key2"),
            PipelineStep("test_yarn", "query3", {}, cache_key="key3")
        ]
        
        results = self.loom.weave(pipeline_config)
        
        assert len(results) == 3
        assert results[0] == "cached_1"      # cache hit
        assert results[1] == "fresh_result_2" # cache miss
        assert results[2] == "cached_3"      # cache hit
        
        # Verify yarn was only called once (for cache miss)
        self.mock_yarn.query.assert_called_once()
        
        # Verify cache set was called once (for cache miss)
        self.mock_shuttle.set.assert_called_once_with("key2", "fresh_result_2")
    
    def test_weave_not_initialized(self):
        """Test weave fails when loom not initialized."""
        # Don't call initialize()
        pipeline_config = [
            PipelineStep("test_yarn", "query", {})
        ]
        
        with pytest.raises(LoomException, match="Loom not initialized. Call initialize\\(\\) first."):
            self.loom.weave(pipeline_config)
    
    def test_weave_yarn_not_found(self):
        """Test weave fails when yarn not found."""
        self.loom.initialize()
        
        pipeline_config = [
            PipelineStep("nonexistent_yarn", "query", {})
        ]
        
        with pytest.raises(LoomException, match="Yarn 'nonexistent_yarn' not found"):
            self.loom.weave(pipeline_config)
    
    def test_weave_yarn_query_failure(self):
        """Test weave handles yarn query failure."""
        self.loom.initialize()
        
        # Set up yarn to raise exception
        self.mock_yarn.query.side_effect = ValueError("Database connection failed")
        
        pipeline_config = [
            PipelineStep("test_yarn", "failing_query", {})
        ]
        
        with pytest.raises(LoomException, match="Error executing step with yarn 'test_yarn': Database connection failed"):
            self.loom.weave(pipeline_config)
    
    def test_weave_shuttle_cache_failure(self):
        """Test weave handles shuttle cache operation failure."""
        self.loom.initialize()
        
        # Set up shuttle to raise exception on exists check
        self.mock_shuttle.exists.side_effect = Exception("Cache service unavailable")
        
        pipeline_config = [
            PipelineStep("test_yarn", "query", {}, cache_key="failing_cache")
        ]
        
        with pytest.raises(LoomException, match="Pipeline execution failed"):
            self.loom.weave(pipeline_config)
    
    def test_weave_empty_pipeline(self):
        """Test weaving empty pipeline."""
        self.loom.initialize()
        
        results = self.loom.weave([])
        
        assert results == []
        self.mock_yarn.query.assert_not_called()
        self.mock_shuttle.exists.assert_not_called()
    
    def test_weave_complex_parameters(self):
        """Test weaving with complex parameter structures."""
        self.loom.initialize()
        
        complex_params = {
            "filters": {
                "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
                "categories": ["A", "B", "C"]
            },
            "options": {
                "sort": "asc",
                "limit": 100,
                "include_meta": True
            }
        }
        
        pipeline_config = [
            PipelineStep(
                yarn_name="test_yarn",
                query_template="complex_query",
                params=complex_params,
                priority=0.8,
                timeout=60.0
            )
        ]
        
        results = self.loom.weave(pipeline_config)
        
        assert len(results) == 1
        assert results[0] == "yarn_result"
        
        # Verify yarn received complex parameters
        call_args = self.mock_yarn.query.call_args
        assert call_args[1]["params"] == complex_params
        assert call_args[0][0] == "complex_query"
    
    def test_weave_step_ordering(self):
        """Test that steps are executed in the correct order."""
        self.loom.initialize()
        
        execution_order = []
        
        def track_execution(query_template, params):
            execution_order.append(query_template)
            return f"result_for_{query_template}"
        
        self.mock_yarn.query.side_effect = track_execution
        
        pipeline_config = [
            PipelineStep("test_yarn", "first_query", {}),
            PipelineStep("test_yarn", "second_query", {}),
            PipelineStep("test_yarn", "third_query", {}),
            PipelineStep("test_yarn", "fourth_query", {})
        ]
        
        results = self.loom.weave(pipeline_config)
        
        # Verify execution order
        expected_order = ["first_query", "second_query", "third_query", "fourth_query"]
        assert execution_order == expected_order
        
        # Verify results order
        expected_results = [f"result_for_{query}" for query in expected_order]
        assert results == expected_results
    
    def test_weave_partial_failure_recovery(self):
        """Test behavior when one step fails but others could succeed."""
        self.loom.initialize()
        
        # Set up yarn to fail on second call
        self.mock_yarn.query.side_effect = [
            "success_1", 
            ValueError("Step 2 failed"),
            "success_3"
        ]
        
        pipeline_config = [
            PipelineStep("test_yarn", "query1", {}),
            PipelineStep("test_yarn", "query2", {}),
            PipelineStep("test_yarn", "query3", {})
        ]
        
        # Should fail on second step and not continue
        with pytest.raises(LoomException, match="Error executing step with yarn 'test_yarn': Step 2 failed"):
            self.loom.weave(pipeline_config)
        
        # Only first two calls should have been made
        assert self.mock_yarn.query.call_count == 2
    
    def test_cache_key_none_behavior(self):
        """Test behavior when cache_key is None."""
        self.loom.initialize()
        
        pipeline_config = [
            PipelineStep("test_yarn", "query", {}, cache_key=None)
        ]
        
        results = self.loom.weave(pipeline_config)
        
        assert len(results) == 1
        assert results[0] == "yarn_result"
        
        # Cache operations should not be called when cache_key is None
        self.mock_shuttle.exists.assert_not_called()
        self.mock_shuttle.get.assert_not_called()
        self.mock_shuttle.set.assert_not_called()
        
        # Yarn should be called directly
        self.mock_yarn.query.assert_called_once()