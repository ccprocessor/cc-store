"""
Core abstractions and interfaces for CC-Store
"""

from .base import StorageBackend, MetadataManager
from .models import CCDocument, DomainMetadata, FileMetadata

__all__ = [
    "StorageBackend",
    "MetadataManager",
    "CCDocument",
    "DomainMetadata", 
    "FileMetadata"
] 