"""
Main CCStore class for interacting with the CC-Store system.
"""

import os
import logging
from typing import Dict, List, Optional, Union, Tuple, Any
import datetime

from pyspark.sql import DataFrame, SparkSession

from cc_store.core.storage import StorageBackend, StorageManager
from cc_store.core.models import DomainMetadata, FileMetadata
from cc_store.core.metadata_manager import MetadataManager, FileSystemMetadataManager, KVStoreMetadataManager


logger = logging.getLogger(__name__)


class CCStore:
    """
    Main class for interacting with the CC-Store.
    This provides high-level APIs for storing and retrieving web data.
    """
    
    def __init__(
        self,
        storage_path: str,
        spark: Optional[SparkSession] = None,
        metadata_backend: str = "file",
        metadata_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a CCStore instance.
        
        Args:
            storage_path: Path to the storage directory
            spark: Optional SparkSession instance
            metadata_backend: Type of metadata backend to use ('file', 'redis', 'rocksdb')
            metadata_config: Optional configuration for the metadata backend
        """
        self.storage_path = storage_path
        self.spark = spark or self._create_spark_session()
        
        # Initialize the storage manager
        self.storage = StorageManager(storage_path, self.spark)
        
        # Initialize the metadata manager based on the specified backend
        self._init_metadata_manager(metadata_backend, metadata_config)
        
    def _init_metadata_manager(self, backend: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the appropriate metadata manager based on the specified backend.
        
        Args:
            backend: Type of metadata backend ('file', 'redis', 'rocksdb')
            config: Configuration for the metadata backend
        """
        config = config or {}
        
        if backend == "file":
            # For file-based metadata, we need the storage path
            self.metadata = FileSystemMetadataManager(
                storage_path=self.storage_path,
                spark=self.spark
            )
        elif backend in ["redis", "rocksdb"]:
            # For KV store metadata, we need the store type and connection params
            self.metadata = KVStoreMetadataManager(
                store_type=backend,
                connection_params=config,
                spark=self.spark
            )
        else:
            raise ValueError(f"Unsupported metadata backend: {backend}")
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create a new Spark session.
        
        Returns:
            SparkSession instance
        """
        return SparkSession.builder \
            .appName("CCStore") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def write_domain(
        self,
        domain: str,
        data: DataFrame,
        date: Optional[str] = None
    ) -> bool:
        """
        Write data for a domain to storage.
        
        Args:
            domain: Domain name
            data: DataFrame containing the data
            date: Optional date string (format: "YYYYMMDD")
            
        Returns:
            True if write was successful, False otherwise
        """
        # Use current date if not provided
        if not date:
            date = datetime.datetime.now().strftime("%Y%m%d")
        
        # Write data to storage
        file_metadata = self.storage.write_dataframe(domain, data, date)
        
        if not file_metadata:
            return False
        
        # Get current domain metadata
        domain_metadata = self.metadata.get_domain_metadata(domain) or {
            "domain": domain,
            "total_records": 0,
            "total_files": 0,
            "total_size_bytes": 0,
            "min_date": date,
            "max_date": date,
            "date_count": 0,
            "stats": {}
        }
        
        # Update domain metadata with new file information
        domain_metadata["total_records"] += sum(file.records_count for file in file_metadata)
        domain_metadata["total_files"] += len(file_metadata)
        domain_metadata["total_size_bytes"] += sum(file.file_size_bytes for file in file_metadata)
        
        # Update date range
        current_dates = self.metadata.get_dates_for_domain(domain)
        if date not in current_dates:
            current_dates.append(date)
            current_dates.sort()
            # Update date-related metadata
            domain_metadata["min_date"] = current_dates[0]
            domain_metadata["max_date"] = current_dates[-1]
            domain_metadata["date_count"] = len(current_dates)
            # Update the dates list
            self.metadata.update_dates_for_domain(domain, current_dates)
        
        # Save updated metadata
        return self.metadata.update_domain_metadata(domain, domain_metadata)
    
    def read_domain(
        self,
        domain: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> DataFrame:
        """
        Read data for a domain from storage.
        
        Args:
            domain: Domain name
            start_date: Optional start date (format: "YYYYMMDD")
            end_date: Optional end date (format: "YYYYMMDD")
            limit: Optional maximum number of records to return
            
        Returns:
            DataFrame containing the data
        """
        # Get available dates for the domain
        available_dates = self.metadata.get_dates_for_domain(domain)
        
        if not available_dates:
            # Return empty DataFrame if no data is available
            return self.spark.createDataFrame([], schema=None)
        
        # Filter dates based on start_date and end_date
        dates_to_read = self._filter_dates(available_dates, start_date, end_date)
        
        if not dates_to_read:
            # Return empty DataFrame if no dates match the criteria
            return self.spark.createDataFrame([], schema=None)
        
        # Read data from storage for the specified dates
        return self.storage.read_domain(domain, dates_to_read, limit)
    
    def _filter_dates(
        self,
        dates: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[str]:
        """
        Filter dates based on start_date and end_date.
        
        Args:
            dates: List of date strings
            start_date: Optional start date (format: "YYYYMMDD")
            end_date: Optional end date (format: "YYYYMMDD")
            
        Returns:
            List of filtered date strings
        """
        if not start_date and not end_date:
            return dates
        
        filtered_dates = []
        for date in dates:
            if start_date and date < start_date:
                continue
            if end_date and date > end_date:
                continue
            filtered_dates.append(date)
        
        return filtered_dates
    
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
        return self.metadata.list_domains(prefix, limit, offset)
    
    def get_domain_statistics(self, domain: str) -> Dict[str, Any]:
        """
        Get statistics for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary with domain statistics
        """
        return self.metadata.get_domain_statistics(domain)
    
    def batch_get_domain_statistics(self, domains: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for multiple domains.
        
        Args:
            domains: List of domain names
            
        Returns:
            Dictionary mapping domain names to statistics dictionaries
        """
        # Use the batch get method from metadata manager
        domain_metadata = self.metadata.batch_get_domain_metadata(domains)
        
        # Convert metadata to statistics format
        result = {}
        for domain, metadata in domain_metadata.items():
            if metadata:
                result[domain] = self.metadata.get_domain_statistics(domain)
        
        return result
    
    def delete_domain(self, domain: str) -> bool:
        """
        Delete a domain and all its data.
        
        Args:
            domain: Domain name
            
        Returns:
            True if deletion was successful, False otherwise
        """
        # Delete data from storage
        storage_deleted = self.storage.delete_domain(domain)
        
        # Delete metadata
        metadata_deleted = self.metadata.delete_domain_metadata(domain)
        
        return storage_deleted and metadata_deleted
    
    def close(self):
        """
        Close connections and clean up resources.
        """
        if hasattr(self, 'metadata') and self.metadata:
            self.metadata.close()
        
        if hasattr(self, 'spark') and self.spark:
            self.spark.stop()

    def get_domain_metadata(self, domain: str) -> Optional[DomainMetadata]:
        """
        Get metadata for a domain.
        
        Args:
            domain: Domain name.
            
        Returns:
            DomainMetadata object or None if domain not found.
        """
        return self.metadata.get_domain_metadata(domain)
    
    def get_dates_for_domain(self, domain: str) -> List[str]:
        """
        Get available dates for a domain.
        
        Args:
            domain: Domain name.
            
        Returns:
            List of date strings in format "YYYYMMDD".
        """
        domain_path = self.storage._get_domain_path(domain)
        
        if not os.path.exists(domain_path):
            return []
            
        dates = []
        
        for item in os.listdir(domain_path):
            if item.startswith("date="):
                date = item.split("=")[1]
                dates.append(date)
        
        return sorted(dates)
    
    def search_content(self, domain: str, keyword: str, 
                      start_date: Optional[str] = None, 
                      end_date: Optional[str] = None) -> DataFrame:
        """
        Search for keyword in HTML content.
        
        Args:
            domain: Domain name.
            keyword: Keyword to search for.
            start_date: Optional start date string in format "YYYYMMDD".
            end_date: Optional end date string in format "YYYYMMDD".
            
        Returns:
            DataFrame with matching documents.
        """
        # Import here to avoid circular imports
        from pyspark.sql import functions as F
        
        # Get the domain data with HTML content
        df = self.read_domain(
            domain,
            start_date=start_date,
            end_date=end_date,
            limit=None
        )
        
        # Filter for documents with the keyword in HTML
        if "html" in df.columns:
            return df.filter(F.col("html").contains(keyword))
        else:
            # No HTML column available
            return self.spark.createDataFrame([], schema=None)
    
    def write_documents(
        self,
        df: DataFrame,
        date_column: str = "date"
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Write a DataFrame containing multiple domains to storage.
        
        This is a convenience method that groups data by domain and date,
        then calls write_domain for each group.
        
        Args:
            df: DataFrame with documents to write. Must have 'domain' and date columns.
            date_column: Name of the column containing date information.
                Date values should be in format "YYYYMMDD" (as string or integer).
                
        Returns:
            Dictionary mapping domains to lists of file metadata.
        """
        # Check required columns
        required_columns = ['domain', date_column]
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"DataFrame must have a '{column}' column")
        
        # Import here to avoid circular imports
        import pyspark.sql.functions as F
        
        # Get unique domains
        domains = df.select("domain").distinct().rdd.flatMap(lambda x: x).collect()
        
        results = {}
        for domain in domains:
            # Filter data for this domain
            domain_df = df.filter(F.col("domain") == domain)
            
            # Get unique dates for this domain
            dates = domain_df.select(date_column).distinct().rdd.flatMap(lambda x: x).collect()
            
            domain_results = []
            for date_val in dates:
                # Convert date to string if it's an integer
                date_str = str(date_val)
                
                # Filter data for this date
                date_df = domain_df.filter(F.col(date_column) == date_val)
                
                # Write data for this domain and date
                file_metadata = self.storage.write_dataframe(domain, date_df, date_str)
                
                if file_metadata:
                    # Add metadata to results
                    domain_results.extend([meta.to_dict() for meta in file_metadata])
                    
                    # Update domain metadata 
                    self._update_domain_metadata(domain, date_str, file_metadata)
            
            if domain_results:
                results[domain] = domain_results
        
        return results
    
    def _update_domain_metadata(self, domain: str, date: str, file_metadata: List[FileMetadata]) -> bool:
        """
        Update domain metadata with new file metadata.
        
        Args:
            domain: Domain name
            date: Date string (format: "YYYYMMDD")
            file_metadata: List of FileMetadata objects for new files
            
        Returns:
            True if update was successful, False otherwise
        """
        # Get current domain metadata
        domain_metadata = self.metadata.get_domain_metadata(domain) or {
            "domain": domain,
            "total_records": 0,
            "total_files": 0,
            "total_size_bytes": 0,
            "min_date": date,
            "max_date": date,
            "date_count": 0,
            "stats": {}
        }
        
        # Update domain metadata with new file information
        domain_metadata["total_records"] += sum(file.records_count for file in file_metadata)
        domain_metadata["total_files"] += len(file_metadata)
        domain_metadata["total_size_bytes"] += sum(file.file_size_bytes for file in file_metadata)
        
        # Update date range
        current_dates = self.metadata.get_dates_for_domain(domain)
        if date not in current_dates:
            current_dates.append(date)
            current_dates.sort()
            # Update date-related metadata
            domain_metadata["min_date"] = current_dates[0]
            domain_metadata["max_date"] = current_dates[-1]
            domain_metadata["date_count"] = len(current_dates)
            # Update the dates list
            self.metadata.update_dates_for_domain(domain, current_dates)
        
        # Save updated metadata
        return self.metadata.update_domain_metadata(domain, domain_metadata) 