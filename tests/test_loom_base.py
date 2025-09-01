# test_loom_base.py
import pytest
from unittest.mock import Mock, MagicMock
from loom_base import LoomBase, LoomException, PipelineStep


class TestPipelineStep:
    """Test suite for PipelineStep dataclass."""
    
    def test_pipeline_step_creation_full(self):
        """Test PipelineStep creation with all attributes."""
        yarn_name = "data_retriever"
        query_template = "SELECT * FROM users WHERE id = {user_id}"
        params = {"user_id": 123, "limit": 50}
        priority = 0.8
        cache_key = "user_123_data"
        timeout = 30.0
        
        step = PipelineStep(
            yarn_name=yarn_name,
            query_template=query_template,
            params=params,
            priority=priority,
            cache_key=cache_key,
            timeout=timeout
        )
        
        assert step.yarn_name == yarn_name
        assert step.query_template == query_template
        assert step.params == params
        assert step.params["user_id"] == 123
        assert step.params["limit"] == 50
        assert step.priority == priority
        assert step.cache_key == cache_key
        assert step.timeout == timeout
    
    def test_pipeline_step_creation_minimal(self):
        """Test PipelineStep creation with only required attributes."""
        yarn_name = "simple_retriever"
        query_template = "GET /api/data"
        params = {}
        
        step = PipelineStep(
            yarn_name=yarn_name,
            query_template=query_template,
            params=params
        )
        
        assert step.yarn_name == yarn_name
        assert step.query_template == query_template
        assert step.params == params
        assert step.priority is None
        assert step.cache_key is None
        assert step.timeout is None
    
    def test_pipeline_step_with_complex_params(self):
        """Test PipelineStep with complex parameter structures."""
        complex_params = {
            "filters": {
                "date_range": {
                    "start": "2024-01-01",
                    "end": "2024-12-31"
                },
                "categories": ["tech", "finance", "health"]
            },
            "aggregations": ["count", "sum", "avg"],
            "nested_query": {
                "sub_filters": {"status": "active"},
                "sort_order": "desc"
            }
        }
        
        step = PipelineStep(
            yarn_name="complex_retriever",
            query_template="COMPLEX_QUERY",
            params=complex_params,
            priority=0.9
        )
        
        assert step.params["filters"]["date_range"]["start"] == "2024-01-01"
        assert "tech" in step.params["filters"]["categories"]
        assert len(step.params["aggregations"]) == 3
        assert step.params["nested_query"]["sub_filters"]["status"] == "active"


class TestLoomException:
    """Test suite for LoomException class."""
    
    def test_loom_exception_creation(self):
        """Test LoomException creation and inheritance."""
        message = "Pipeline execution failed"
        exc = LoomException(message)
        
        assert str(exc) == message
        assert isinstance(exc, Exception)
    
    def test_loom_exception_with_cause(self):
        """Test LoomException with underlying cause."""
        original_error = ValueError("Invalid configuration")
        
        try:
            raise original_error
        except ValueError as e:
            loom_exc = LoomException("Loom initialization failed")
        
        assert isinstance(loom_exc, LoomException)
        assert loom_exc.__cause__ == original_error


class TestLoomBase:
    """Test suite for LoomBase abstract class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Create a concrete implementation for testing
        class ConcreteLoom(LoomBase):
            def weave(self, pipeline_config):
                return f"Processed {len(pipeline_config)} steps"
        
        self.loom = ConcreteLoom()
    
    def test_loom_initialization(self):
        """Test LoomBase initialization state."""
        assert self.loom._yarns == {}
        assert self.loom._fabrics == {}
        assert self.loom._shuttle is None
        assert self.loom._initialized is False
        assert self.loom.initialized is False
    
    def test_register_yarn_success(self):
        """Test successful yarn registration."""
        yarn_name = "test_yarn"
        mock_yarn = Mock()
        mock_yarn.name = yarn_name
        
        self.loom.register_yarn(yarn_name, mock_yarn)
        
        assert yarn_name in self.loom._yarns
        assert self.loom._yarns[yarn_name] == mock_yarn
    
    def test_register_yarn_duplicate(self):
        """Test error when registering duplicate yarn name."""
        yarn_name = "duplicate_yarn"
        mock_yarn1 = Mock()
        mock_yarn2 = Mock()
        
        self.loom.register_yarn(yarn_name, mock_yarn1)
        
        with pytest.raises(LoomException, match="Yarn 'duplicate_yarn' already registered"):
            self.loom.register_yarn(yarn_name, mock_yarn2)
        
        # Original yarn should still be registered
        assert self.loom._yarns[yarn_name] == mock_yarn1
    
    def test_register_fabric_success(self):
        """Test successful fabric registration."""
        fabric_name = "database_fabric"
        mock_fabric = Mock()
        mock_fabric.name = fabric_name
        
        self.loom.register_fabric(fabric_name, mock_fabric)
        
        assert fabric_name in self.loom._fabrics
        assert self.loom._fabrics[fabric_name] == mock_fabric
    
    def test_register_fabric_duplicate(self):
        """Test error when registering duplicate fabric name."""
        fabric_name = "duplicate_fabric"
        mock_fabric1 = Mock()
        mock_fabric2 = Mock()
        
        self.loom.register_fabric(fabric_name, mock_fabric1)
        
        with pytest.raises(LoomException, match="Fabric 'duplicate_fabric' already registered"):
            self.loom.register_fabric(fabric_name, mock_fabric2)
    
    def test_register_shuttle_success(self):
        """Test successful shuttle registration."""
        mock_shuttle = Mock()
        mock_shuttle.name = "test_shuttle"
        
        self.loom.register_shuttle(mock_shuttle)
        
        assert self.loom._shuttle == mock_shuttle
    
    def test_register_shuttle_duplicate(self):
        """Test error when registering duplicate shuttle."""
        mock_shuttle1 = Mock()
        mock_shuttle2 = Mock()
        
        self.loom.register_shuttle(mock_shuttle1)
        
        with pytest.raises(LoomException, match="Shuttle already registered"):
            self.loom.register_shuttle(mock_shuttle2)
        
        # Original shuttle should still be registered
        assert self.loom._shuttle == mock_shuttle1
    
    def test_get_yarn_success(self):
        """Test successful yarn retrieval."""
        yarn_name = "retriever_yarn"
        mock_yarn = Mock()
        self.loom._yarns[yarn_name] = mock_yarn
        
        retrieved_yarn = self.loom.get_yarn(yarn_name)
        
        assert retrieved_yarn == mock_yarn
    
    def test_get_yarn_not_found(self):
        """Test error when yarn not found."""
        with pytest.raises(LoomException, match="Yarn 'nonexistent_yarn' not found"):
            self.loom.get_yarn("nonexistent_yarn")
    
    def test_get_fabric_success(self):
        """Test successful fabric retrieval."""
        fabric_name = "sql_fabric"
        mock_fabric = Mock()
        self.loom._fabrics[fabric_name] = mock_fabric
        
        retrieved_fabric = self.loom.get_fabric(fabric_name)
        
        assert retrieved_fabric == mock_fabric
    
    def test_get_fabric_not_found(self):
        """Test error when fabric not found."""
        with pytest.raises(LoomException, match="Fabric 'nonexistent_fabric' not found"):
            self.loom.get_fabric("nonexistent_fabric")
    
    def test_get_shuttle_success(self):
        """Test successful shuttle retrieval."""
        mock_shuttle = Mock()
        self.loom._shuttle = mock_shuttle
        
        retrieved_shuttle = self.loom.get_shuttle()
        
        assert retrieved_shuttle == mock_shuttle
    
    def test_get_shuttle_not_registered(self):
        """Test error when shuttle not registered."""
        with pytest.raises(LoomException, match="No Shuttle registered"):
            self.loom.get_shuttle()
    
    def test_initialization_success(self):
        """Test successful loom initialization."""
        # Register required components
        self.loom.register_yarn("test_yarn", Mock())
        self.loom.register_fabric("test_fabric", Mock())
        self.loom.register_shuttle(Mock())
        
        self.loom.initialize()
        
        assert self.loom._initialized is True
        assert self.loom.initialized is True
    
    def test_initialization_missing_yarns(self):
        """Test initialization failure when no yarns registered."""
        self.loom.register_fabric("test_fabric", Mock())
        self.loom.register_shuttle(Mock())
        
        with pytest.raises(LoomException, match="No Yarns registered"):
            self.loom.initialize()
        
        assert self.loom._initialized is False
    
    def test_initialization_missing_fabrics(self):
        """Test initialization failure when no fabrics registered."""
        self.loom.register_yarn("test_yarn", Mock())
        self.loom.register_shuttle(Mock())
        
        with pytest.raises(LoomException, match="No Fabrics registered"):
            self.loom.initialize()
        
        assert self.loom._initialized is False
    
    def test_initialization_missing_shuttle(self):
        """Test initialization failure when no shuttle registered."""
        self.loom.register_yarn("test_yarn", Mock())
        self.loom.register_fabric("test_fabric", Mock())
        
        with pytest.raises(LoomException, match="No Shuttle registered"):
            self.loom.initialize()
        
        assert self.loom._initialized is False
    
    def test_multiple_component_registration(self):
        """Test registering multiple yarns and fabrics."""
        # Register multiple yarns
        yarns = {"yarn1": Mock(), "yarn2": Mock(), "yarn3": Mock()}
        for name, yarn in yarns.items():
            self.loom.register_yarn(name, yarn)
        
        # Register multiple fabrics
        fabrics = {"fabric1": Mock(), "fabric2": Mock()}
        for name, fabric in fabrics.items():
            self.loom.register_fabric(name, fabric)
        
        # Register shuttle
        shuttle = Mock()
        self.loom.register_shuttle(shuttle)
        
        # Verify all registered correctly
        assert len(self.loom._yarns) == 3
        assert len(self.loom._fabrics) == 2
        assert self.loom._shuttle == shuttle
        
        # Test retrieval
        for name, yarn in yarns.items():
            assert self.loom.get_yarn(name) == yarn
        
        for name, fabric in fabrics.items():
            assert self.loom.get_fabric(name) == fabric
        
        assert self.loom.get_shuttle() == shuttle
    
    def test_weave_abstract_method(self):
        """Test that weave method is properly implemented in concrete class."""
        # Our concrete implementation should work
        pipeline_config = [
            PipelineStep("yarn1", "query1", {}),
            PipelineStep("yarn2", "query2", {})
        ]
        
        result = self.loom.weave(pipeline_config)
        assert result == "Processed 2 steps"
    
    def test_component_state_isolation(self):
        """Test that different loom instances have isolated state."""
        class AnotherConcreteLoom(LoomBase):
            def weave(self, pipeline_config):
                return "different implementation"
        
        loom2 = AnotherConcreteLoom()
        
        # Register components in first loom
        self.loom.register_yarn("yarn1", Mock())
        self.loom.register_fabric("fabric1", Mock())
        
        # Second loom should be empty
        assert len(loom2._yarns) == 0
        assert len(loom2._fabrics) == 0
        assert loom2._shuttle is None
        assert loom2.initialized is False
        
        # First loom should still have components
        assert len(self.loom._yarns) == 1
        assert len(self.loom._fabrics) == 1


class TestLoomBaseEdgeCases:
    """Test edge cases and error conditions for LoomBase."""
    
    def setup_method(self):
        """Set up test fixtures."""
        class TestLoom(LoomBase):
            def weave(self, pipeline_config):
                return [f"step_{i}" for i in range(len(pipeline_config))]
        
        self.loom = TestLoom()
    
    def test_register_none_values(self):
        """Test behavior when registering None values."""
        # These should work but might cause issues later
        self.loom.register_yarn("none_yarn", None)
        self.loom.register_fabric("none_fabric", None)
        
        assert self.loom._yarns["none_yarn"] is None
        assert self.loom._fabrics["none_fabric"] is None
        
        # Retrieval should still work
        assert self.loom.get_yarn("none_yarn") is None
        assert self.loom.get_fabric("none_fabric") is None
    
    def test_register_empty_string_names(self):
        """Test behavior with empty string names."""
        mock_yarn = Mock()
        mock_fabric = Mock()
        
        self.loom.register_yarn("", mock_yarn)
        self.loom.register_fabric("", mock_fabric)
        
        assert self.loom._yarns[""] == mock_yarn
        assert self.loom._fabrics[""] == mock_fabric
        
        # Should be retrievable
        assert self.loom.get_yarn("") == mock_yarn
        assert self.loom.get_fabric("") == mock_fabric
    
    def test_initialization_after_partial_setup(self):
        """Test initialization after partial component setup."""
        # Add yarn and fabric but no shuttle
        self.loom.register_yarn("test_yarn", Mock())
        self.loom.register_fabric("test_fabric", Mock())
        
        # Should fail without shuttle
        with pytest.raises(LoomException, match="No Shuttle registered"):
            self.loom.initialize()
        
        # Add shuttle and try again
        self.loom.register_shuttle(Mock())
        self.loom.initialize()  # Should succeed now
        
        assert self.loom.initialized is True
    
    def test_double_initialization(self):
        """Test calling initialize() multiple times."""
        # Set up all components
        self.loom.register_yarn("yarn", Mock())
        self.loom.register_fabric("fabric", Mock())
        self.loom.register_shuttle(Mock())
        
        # First initialization
        self.loom.initialize()
        assert self.loom.initialized is True
        
        # Second initialization should also work (idempotent)
        self.loom.initialize()
        assert self.loom.initialized is True