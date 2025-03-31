"""
Main API class for CC-Store.
"""

from typing import Dict, List, Optional, Any, Tuple, Union
import os
import datetime

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

from cc_store.core import StorageBackend, MetadataManager
from cc_store.storage import ParquetStorageBackend, HBaseMetadataManager


class CCStore:
    """
    Main API class for CC-Store.
    
    This class provides a unified interface for working with CC-Store.
    It handles initialization of the storage backend and metadata manager.
    """
    
    def __init__(
        self,
        storage_path: str,
        metadata_backend: str = "hbase",
        metadata_connection: Optional[str] = None,
        spark: Optional[SparkSession] = None,
        max_file_size_gb: float = 2.0
    ):
        """
        Initialize the CC-Store.
        
        Args:
            storage_path: Base path for storing data
            metadata_backend: Metadata backend type ('hbase' or 'parquet')
            metadata_connection: Connection string for metadata backend (required for HBase)
            spark: Optional SparkSession instance
            max_file_size_gb: Maximum size of a single file in GB
        """
        self.storage_path = storage_path
        self.spark = spark or SparkSession.builder.getOrCreate()
        
        # Initialize metadata manager
        if metadata_backend == "hbase":
            if not metadata_connection:
                raise ValueError("metadata_connection is required for HBase backend")
            
            self.metadata_manager = HBaseMetadataManager(
                connection_string=metadata_connection,
                spark=self.spark
            )
        elif metadata_backend == "parquet":
            metadata_path = os.path.join(storage_path, "metadata")
            self.metadata_manager = ParquetHTMLContentStore(
                base_path=metadata_path,
                spark=self.spark
            )
        else:
            raise ValueError(f"Unsupported metadata backend: {metadata_backend}")
        
        # Initialize storage backend
        data_path = os.path.join(storage_path, "data")
        self.storage_backend = ParquetStorageBackend(
            base_path=data_path,
            spark=self.spark,
            max_file_size_gb=max_file_size_gb,
            metadata_manager=self.metadata_manager
        )
    
    def get_data_by_domain(
        self, 
        domain: str, 
        date_range: Optional[Tuple[str, str]] = None,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
        include_html: bool = True
    ) -> SparkDataFrame:
        """
        Get data for a specific domain.
        
        Args:
            domain: Domain name to retrieve data for
            date_range: Optional tuple of (start_date, end_date) in YYYYMMDD format
            fields: Optional list of fields to include
            limit: Optional maximum number of records to return
            include_html: Whether to include HTML content
            
        Returns:
            Spark DataFrame containing the requested data
        """
        # Adjust fields based on include_html
        if not include_html and fields is None:
            fields = [
                "raw_warc_path", "track_id", "domain", "url", "status",
                "response_header", "date", "content_length", "content_charset",
                "content_hash", "remark"
            ]
        elif not include_html and "html" in fields:
            fields.remove("html")
        
        return self.storage_backend.read_by_domain(
            domain=domain,
            date_range=date_range,
            fields=fields,
            limit=limit
        )
    
    def get_data_by_domains(
        self,
        domains: List[str],
        date_range: Optional[Tuple[str, str]] = None,
        fields: Optional[List[str]] = None,
        include_html: bool = True
    ) -> SparkDataFrame:
        """
        Get data for multiple domains.
        
        Args:
            domains: List of domain names to retrieve data for
            date_range: Optional tuple of (start_date, end_date) in YYYYMMDD format
            fields: Optional list of fields to include
            include_html: Whether to include HTML content
            
        Returns:
            Spark DataFrame containing the requested data
        """
        # Adjust fields based on include_html
        if not include_html and fields is None:
            fields = [
                "raw_warc_path", "track_id", "domain", "url", "status",
                "response_header", "date", "content_length", "content_charset",
                "content_hash", "remark"
            ]
        elif not include_html and "html" in fields:
            fields.remove("html")
        
        return self.storage_backend.read_by_domains(
            domains=domains,
            date_range=date_range,
            fields=fields
        )
    
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
        return self.storage_backend.write_data(
            dataframe=dataframe,
            overwrite=overwrite,
            partition_size_hint=partition_size_hint
        )
    
    def get_domain_metadata(self, domain: str) -> Dict[str, Any]:
        """
        Get metadata for a specific domain.
        
        Args:
            domain: Domain name to retrieve metadata for
            
        Returns:
            Dictionary containing domain metadata
        """
        return self.metadata_manager.get_domain_metadata(domain)
    
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
        return self.metadata_manager.list_domains(prefix, limit, offset) 