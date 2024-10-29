"""
Fabric module for database and API integration layer management.
Provides unified interfaces for database connections and API clients.
"""

from .fabric_base import FabricBase
from .fabric_exceptions import FabricException

__all__ = ['FabricBase', 'FabricException']
