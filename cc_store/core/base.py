"""
Base abstractions for the CC-Store system.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any, Tuple
import datetime

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame


class StorageBackend(ABC):
    """Abstract base class for CC-Store storage backends."""
    
    @abstractmethod
    def write_data(
        self, 
        dataframe: SparkDataFrame,
        overwrite: bool = False,
        partition_size_hint: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Write data to storage.
        
        Args:
            dataframe: Spark DataFrame containing the data to write
            overwrite: Whether to overwrite existing data
            partition_size_hint: Target size for partitions in MB
            
        Returns:
            Dictionary with statistics about the write operation
        """
        pass
    
    @abstractmethod
    def read_by_domain(
        self,
        domain: str,
        date_range: Optional[Tuple[str, str]] = None,
        fields: Optional[List[str]] = None
    ) -> SparkDataFrame:
        """
        Read data for a domain.
        
        Args:
            domain: Domain name to read data for
            date_range: Optional tuple of (start_date, end_date) in YYYYMMDD format
            fields: Optional list of fields to include in the result
            
        Returns:
            Spark DataFrame containing the data
        """
        pass
    
    @abstractmethod
    def read_by_domains(
        self,
        domains: List[str],
        date_range: Optional[Tuple[str, str]] = None,
        fields: Optional[List[str]] = None
    ) -> SparkDataFrame:
        """
        Read data for multiple domains.
        
        Args:
            domains: List of domain names to read data for
            date_range: Optional tuple of (start_date, end_date) in YYYYMMDD format
            fields: Optional list of fields to include in the result
            
        Returns:
            Spark DataFrame containing the data
        """
        pass
    
    @abstractmethod
    def get_domain_metadata(
        self,
        domain: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary with domain metadata or None if not found
        """
        pass
    
    @abstractmethod
    def list_domains(
        self,
        prefix: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[str]:
        """
        List domains in the store.
        
        Args:
            prefix: Optional domain name prefix filter
            limit: Maximum number of domains to return
            offset: Offset to start from for pagination
            
        Returns:
            List of domain names
        """
        pass


class MetadataManager(ABC):
    """
    Abstract base class for metadata management.
    
    This class defines the interface for metadata managers that handle
    storing and retrieving metadata about domains and files.
    """
    
    @abstractmethod
    def get_domain_metadata(self, domain: str) -> Dict[str, Any]:
        """
        Get metadata for a specific domain.
        
        Args:
            domain: Domain name to retrieve metadata for
            
        Returns:
            Dictionary containing domain metadata
        """
        pass
    
    @abstractmethod
    def list_domains(
        self,
        prefix: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[str]:
        """
        List domains in the store.
        
        Args:
            prefix: Optional domain name prefix filter
            limit: Maximum number of domains to return
            offset: Offset to start from for pagination
            
        Returns:
            List of domain names
        """
        pass
    
    @abstractmethod
    def get_domain_files(
        self,
        domain: str,
        date_range: Optional[Tuple[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get file metadata for a specific domain.
        
        Args:
            domain: Domain name to retrieve file metadata for
            date_range: Optional tuple of (start_date, end_date) in YYYYMMDD format
            
        Returns:
            List of dictionaries containing file metadata
        """
        pass
    
    @abstractmethod
    def update_metadata_after_write(
        self,
        domain: str,
        date: str,
        files_info: List[Dict[str, Any]]
    ) -> None:
        """
        Update metadata after writing files.
        
        Args:
            domain: Domain name
            date: Date in YYYYMMDD format
            files_info: List of dictionaries containing file metadata
        """
        pass


class DomainStateStore(ABC):
    """
    Abstract base class for storing domain state data.
    """
    
    @abstractmethod
    def store_domain_state(
        self,
        domain: str,
        date: str,
        files_info: List[Dict]
    ) -> None:
        """
        Store state data for a domain.
        
        Args:
            domain: Domain name
            date: Date in YYYYMMDD format
            files_info: List of dictionaries containing file metadata
        """
        pass 