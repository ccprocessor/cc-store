"""
Main CCStore class for interacting with the CC-Store system.
"""

import os
import logging
from typing import Dict, List, Optional, Union, Tuple
import datetime

from pyspark.sql import DataFrame, SparkSession

from cc_store.core.storage import StorageBackend
from cc_store.core.models import DomainMetadata, FileMetadata


logger = logging.getLogger(__name__)


class CCStore:
    """Main class for interacting with the CC-Store system."""
    
    def __init__(
        self,
        storage_path: str,
        spark: Optional[SparkSession] = None,
        target_part_size_bytes: int = 128 * 1024 * 1024,  # 128MB
        min_records_per_part: int = 1000
    ):
        """
        Initialize the CCStore.
        
        Args:
            storage_path: Base path for storing data.
            spark: SparkSession to use.
            target_part_size_bytes: Target size for each part file in bytes.
            min_records_per_part: Minimum number of records per part file.
        """
        self.storage_path = storage_path
        self.spark = spark or SparkSession.builder.getOrCreate()
        
        # Initialize storage backend
        self.storage = StorageBackend(
            storage_path=storage_path,
            spark=self.spark,
            target_part_size_bytes=target_part_size_bytes,
            min_records_per_part=min_records_per_part
        )
    
    def write_documents(self, df: DataFrame) -> List[FileMetadata]:
        """
        Write documents to storage.
        
        Args:
            df: DataFrame with documents to write. Must have columns:
                - domain: Domain name
                - url: URL of the document
                - date: Date string in format "YYYYMMDD"
                - html: HTML content (optional)
            
        Returns:
            List of FileMetadata objects for the written files.
        """
        # Validate DataFrame
        required_columns = ['domain', 'url', 'date']
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"DataFrame must have a '{column}' column")
        
        # Write directly to storage (no separate HTML store)
        return self.storage.write_dataframe(df)
    
    def read_domain(
        self,
        domain: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        filters: Optional[List] = None,
        with_html: bool = True,
        limit: Optional[int] = None
    ) -> DataFrame:
        """
        Read documents for a domain.
        
        Args:
            domain: Domain name.
            start_date: Optional start date string in format "YYYYMMDD".
            end_date: Optional end date string in format "YYYYMMDD".
            filters: Optional list of pyspark SQL filter conditions.
            with_html: Whether to include HTML content in the result.
            limit: Optional maximum number of records to return.
            
        Returns:
            DataFrame with documents.
        """
        # Read from storage with option to exclude HTML column
        df = self.storage.read_domain(
            domain, 
            start_date, 
            end_date, 
            filters, 
            include_html=with_html
        )
        
        # Apply limit if specified
        if limit is not None and limit > 0:
            df = df.limit(limit)
            
        return df
    
    def get_domains(self) -> List[str]:
        """
        Get a list of all domains in the store.
        
        Returns:
            List of domain names.
        """
        data_path = os.path.join(self.storage_path, "data")
        
        if not os.path.exists(data_path):
            return []
            
        domains = []
        
        for bucket_dir in os.listdir(data_path):
            if not bucket_dir.startswith("domain_bucket="):
                continue
                
            bucket_path = os.path.join(data_path, bucket_dir)
            
            for domain_dir in os.listdir(bucket_path):
                if not domain_dir.startswith("domain="):
                    continue
                    
                domain = domain_dir.split("=")[1]
                domains.append(domain)
        
        return domains
    
    def get_domain_metadata(self, domain: str) -> Optional[DomainMetadata]:
        """
        Get metadata for a domain.
        
        Args:
            domain: Domain name.
            
        Returns:
            DomainMetadata object or None if domain not found.
        """
        return self.storage.get_domain_metadata(domain)
    
    def get_domain_statistics(self, domain: str) -> Dict:
        """
        Get statistics for a domain.
        
        Args:
            domain: Domain name.
            
        Returns:
            Dictionary with domain statistics.
        """
        metadata = self.get_domain_metadata(domain)
        
        if not metadata:
            return {}
            
        return {
            "total_records": metadata.total_records,
            "total_files": metadata.total_files,
            "total_size_bytes": metadata.total_size_bytes,
            "date_range": {
                "min_date": metadata.min_date,
                "max_date": metadata.max_date,
                "date_count": metadata.date_count
            },
            "stats": metadata.stats
        }
    
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
            with_html=True
        )
        
        # Filter for documents with the keyword in HTML
        if "html" in df.columns:
            return df.filter(F.col("html").contains(keyword))
        else:
            # No HTML column available
            return self.spark.createDataFrame([], schema=None) 