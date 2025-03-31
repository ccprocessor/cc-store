"""
Base abstractions for the CC-Store system.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any, Tuple
import datetime

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    
    This class defines the interface for storage implementations that handle
    reading and writing Common Crawl documents.
    """
    
    @abstractmethod
    def read_by_domain(
        self, 
        domain: str, 
        date_range: Optional[Tuple[str, str]] = None,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> SparkDataFrame:
        """
        Read data for a specific domain.
        
        Args:
            domain: Domain name to retrieve data for
            date_range: Optional tuple of (start_date, end_date) in YYYYMMDD format
            fields: Optional list of fields to include
            limit: Optional maximum number of records to return
            
        Returns:
            Spark DataFrame containing the requested data
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
            domains: List of domain names to retrieve data for
            date_range: Optional tuple of (start_date, end_date) in YYYYMMDD format
            fields: Optional list of fields to include
            
        Returns:
            Spark DataFrame containing the requested data
        """
        pass
    
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
    def append_data(
        self,
        dataframe: SparkDataFrame,
        deduplicate: bool = True
    ) -> Dict[str, Any]:
        """
        Append incremental data to storage.
        
        Args:
            dataframe: Spark DataFrame containing the data to append
            deduplicate: Whether to deduplicate HTML content
            
        Returns:
            Dictionary with statistics about the append operation
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


class HTMLContentStore(ABC):
    """
    Abstract base class for HTML content storage with deduplication.
    """
    
    @abstractmethod
    def store_html_content(
        self,
        dataframe: SparkDataFrame,
        date: str,
        method: str = "exact"
    ) -> Tuple[SparkDataFrame, Dict[str, Any]]:
        """
        Store HTML content with deduplication.
        
        Args:
            dataframe: Spark DataFrame containing documents with HTML content
            date: Date in YYYYMMDD format
            method: Deduplication method ('exact', 'simhash', 'minhash')
            
        Returns:
            Tuple of (modified DataFrame with content_hash, statistics dict)
        """
        pass
    
    @abstractmethod
    def get_html_content(
        self,
        content_hashes: List[str]
    ) -> Dict[str, str]:
        """
        Get HTML content by hash.
        
        Args:
            content_hashes: List of content hash values
            
        Returns:
            Dictionary mapping content hashes to HTML content
        """
        pass
    
    @abstractmethod
    def get_deduplication_stats(
        self,
        domain: Optional[str] = None,
        date_range: Optional[Tuple[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Get deduplication statistics.
        
        Args:
            domain: Optional domain filter
            date_range: Optional date range filter
            
        Returns:
            Dictionary with deduplication statistics
        """
        pass 