"""
Storage backend for the CC-Store system.
"""

import os
import logging
import json
from typing import Dict, List, Optional, Union, Tuple
import datetime
import fnmatch
import shutil

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from cc_store.core.models import DomainMetadata, FileMetadata


logger = logging.getLogger(__name__)


class StorageBackend:
    """
    Base storage backend for the CC-Store system.
    
    This class handles the storage and retrieval of documents,
    partitioned by domain and date.
    """
    
    def __init__(
        self,
        storage_path: str,
        spark: SparkSession,
        target_part_size_bytes: int = 128 * 1024 * 1024,  # 128MB
        min_records_per_part: int = 1000
    ):
        """
        Initialize the storage backend.
        
        Args:
            storage_path: Base path for storing data.
            spark: SparkSession to use.
            target_part_size_bytes: Target size for each part file in bytes.
            min_records_per_part: Minimum number of records per part file.
        """
        self.storage_path = storage_path
        self.spark = spark
        self.target_part_size_bytes = target_part_size_bytes
        self.min_records_per_part = min_records_per_part
        
        # Create data directory if it doesn't exist
        self.data_path = os.path.join(storage_path, "data")
        os.makedirs(self.data_path, exist_ok=True)
        
        # Create metadata directory if it doesn't exist
        self.metadata_path = os.path.join(storage_path, "metadata")
        os.makedirs(self.metadata_path, exist_ok=True)
    
    def _get_domain_bucket(self, domain: str) -> str:
        """
        Get the bucket for a domain based on its first character.
        
        This helps distribute domains across directories to avoid
        having too many subdirectories in a single directory,
        which can cause performance issues on some file systems.
        
        Args:
            domain: Domain name.
            
        Returns:
            Domain bucket name (e.g., "a-f", "g-m", "n-s", "t-z", "other").
        """
        if not domain:
            return "other"
            
        first_char = domain[0].lower()
        
        if 'a' <= first_char <= 'f':
            return "a-f"
        elif 'g' <= first_char <= 'm':
            return "g-m"
        elif 'n' <= first_char <= 's':
            return "n-s"
        elif 't' <= first_char <= 'z':
            return "t-z"
        else:
            return "other"
    
    def _get_domain_path(self, domain: str) -> str:
        """
        Get the path for a domain.
        
        Args:
            domain: Domain name.
            
        Returns:
            Path to the domain directory.
        """
        bucket = self._get_domain_bucket(domain)
        return os.path.join(
            self.data_path,
            f"domain_bucket={bucket}",
            f"domain={domain}"
        )
    
    def _get_date_path(self, domain: str, date: str) -> str:
        """
        Get the path for a domain and date.
        
        Args:
            domain: Domain name.
            date: Date string in format "YYYYMMDD".
            
        Returns:
            Path to the date directory.
        """
        domain_path = self._get_domain_path(domain)
        return os.path.join(domain_path, f"date={date}")
    
    def _get_metadata_path(self, domain: str) -> str:
        """
        Get the path for domain metadata.
        
        Args:
            domain: Domain name.
            
        Returns:
            Path to the domain metadata file.
        """
        bucket = self._get_domain_bucket(domain)
        return os.path.join(
            self.metadata_path,
            f"domain_bucket={bucket}",
            f"domain={domain}",
            "metadata.json"
        )
    
    def _write_metadata(self, metadata: DomainMetadata) -> None:
        """
        Write domain metadata to disk.
        
        Args:
            metadata: DomainMetadata object.
        """
        metadata_path = self._get_metadata_path(metadata.domain)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
        
        with open(metadata_path, "w") as f:
            json.dump(metadata.to_dict(), f, indent=2)
    
    def _read_metadata(self, domain: str) -> Optional[DomainMetadata]:
        """
        Read domain metadata from disk.
        
        Args:
            domain: Domain name.
            
        Returns:
            DomainMetadata object or None if not found.
        """
        metadata_path = self._get_metadata_path(domain)
        
        if not os.path.exists(metadata_path):
            return None
            
        with open(metadata_path, "r") as f:
            metadata_dict = json.load(f)
            
        return DomainMetadata.from_dict(metadata_dict)
    
    def _update_metadata(self, domain: str, file_metadata: List[FileMetadata]) -> DomainMetadata:
        """
        Update domain metadata with new file metadata.
        
        Args:
            domain: Domain name.
            file_metadata: List of FileMetadata objects for new files.
            
        Returns:
            Updated DomainMetadata object.
        """
        # Read existing metadata or create new
        metadata = self._read_metadata(domain) or DomainMetadata(
            domain=domain,
            total_records=0,
            total_files=0,
            total_size_bytes=0,
            min_date=None,
            max_date=None,
            date_count=0,
            stats={}
        )
        
        # Extract dates from file metadata
        dates = set()
        for file in file_metadata:
            dates.add(file.date)
        
        # Update metadata fields
        for file in file_metadata:
            metadata.total_records += file.records_count
            metadata.total_size_bytes += file.file_size_bytes
        
        metadata.total_files += len(file_metadata)
        
        # Update date range
        all_dates = set(dates)
        if metadata.min_date and metadata.max_date:
            # Add existing dates if available
            for date in range(
                int(metadata.min_date), 
                int(metadata.max_date) + 1, 
                1
            ):
                date_str = str(date)
                if len(date_str) == 8:  # Ensure YYYYMMDD format
                    all_dates.add(date_str)
        
        if all_dates:
            metadata.min_date = min(all_dates)
            metadata.max_date = max(all_dates)
            metadata.date_count = len(all_dates)
        
        # Write updated metadata
        self._write_metadata(metadata)
        
        return metadata
    
    def write_dataframe(self, df: DataFrame) -> List[FileMetadata]:
        """
        Write a DataFrame to storage.
        
        Args:
            df: DataFrame to write. Must have domain, url, and date columns.
            
        Returns:
            List of FileMetadata objects for the written files.
        """
        # Check required columns
        required_columns = ['domain', 'url', 'date']
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"DataFrame must have a '{column}' column")
        
        # Apply optimized write options
        write_options = {
            "compression": "snappy",
            "parquet.bloom.filter.enabled#url": "true",
            "parquet.bloom.filter.expected.ndv#url": "100000",
            "parquet.enable.dictionary": "true",
            "parquet.page.size": "1048576",  # 1m = 1048576 bytes
            "parquet.block.size": "67108864"  # 64m = 67108864 bytes
        }
        
        # Group by domain and date for partitioning
        domains = df.select("domain").distinct().collect()
        
        file_metadata_list = []
        
        for domain_row in domains:
            domain = domain_row["domain"]
            
            # Get data for this domain
            domain_df = df.filter(df.domain == domain)
            
            # Group by date
            dates = domain_df.select("date").distinct().collect()
            
            for date_row in dates:
                date = date_row["date"]
                
                # Get data for this domain and date
                date_df = domain_df.filter(domain_df.date == date)
                
                # Determine optimal number of partitions based on data size
                estimated_size = date_df.count() * 1024  # Rough estimate: 1KB per record
                num_partitions = max(
                    1,
                    min(
                        100,  # Cap at 100 partitions
                        estimated_size // self.target_part_size_bytes + 1
                    )
                )
                
                # Repartition if needed
                if num_partitions > 1:
                    date_df = date_df.repartition(num_partitions)
                
                # Get the path for this domain and date
                date_path = self._get_date_path(domain, date)
                
                # Write to Parquet with the specified options
                date_df.write.mode("append").options(**write_options).parquet(date_path)
                
                # Generate file metadata
                for root, _, files in os.walk(date_path):
                    for file in files:
                        if file.endswith(".parquet"):
                            file_path = os.path.join(root, file)
                            file_size = os.path.getsize(file_path)
                            
                            # We can't directly get the number of records in a parquet file
                            # without reading it, but we can estimate based on the file size
                            # and the average record size from our earlier calculation
                            estimated_records = max(1, file_size // 1024)
                            
                            # Extract part ID from filename (typically part-00000.parquet)
                            part_id = 0
                            if "-" in file:
                                try:
                                    part_id = int(file.split("-")[1].split(".")[0])
                                except (IndexError, ValueError):
                                    part_id = 0
                            
                            # Use current time for timestamps
                            current_time = datetime.datetime.now()
                            
                            file_metadata = FileMetadata(
                                domain_id=domain,
                                date=date,
                                part_id=part_id,
                                file_path=file_path,
                                file_size_bytes=file_size,
                                records_count=estimated_records,
                                min_timestamp=current_time,
                                max_timestamp=current_time,
                                created_at=current_time,
                                checksum="dummy-checksum-" + str(hash(file_path))
                            )
                            
                            file_metadata_list.append(file_metadata)
        
        # Update domain metadata for all affected domains
        for domain_row in domains:
            domain = domain_row["domain"]
            domain_files = [meta for meta in file_metadata_list if meta.domain_id == domain]
            self._update_metadata(domain, domain_files)
        
        return file_metadata_list
    
    def read_domain(
        self, 
        domain: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        filters: Optional[List] = None,
        include_html: bool = True
    ) -> DataFrame:
        """
        Read data for a domain with optional date range filter.
        
        Args:
            domain: Domain name.
            start_date: Optional start date in YYYYMMDD format.
            end_date: Optional end date in YYYYMMDD format.
            filters: Optional list of pyspark SQL filter conditions.
            include_html: Whether to include HTML content in result.
            
        Returns:
            DataFrame with data for the domain.
        """
        domain_path = self._get_domain_path(domain)
        
        if not os.path.exists(domain_path):
            # Create an empty DataFrame with the expected schema
            return self.spark.createDataFrame([], schema=None)
        
        # Get all date directories
        date_dirs = []
        for item in os.listdir(domain_path):
            if item.startswith("date="):
                date = item.split("=")[1]
                
                # Apply date range filter if provided
                if start_date and date < start_date:
                    continue
                    
                if end_date and date > end_date:
                    continue
                    
                date_dirs.append(os.path.join(domain_path, item))
        
        if not date_dirs:
            # No data for the specified date range
            return self.spark.createDataFrame([], schema=None)
        
        # Read all Parquet files in the date directories
        df = self.spark.read.parquet(*date_dirs)
        
        # Apply additional filters if provided
        if filters:
            for filter_condition in filters:
                df = df.filter(filter_condition)
        
        # Exclude HTML column if not needed
        if not include_html and "html" in df.columns:
            df = df.drop("html")
        
        return df
    
    def get_domain_metadata(self, domain: str) -> Optional[DomainMetadata]:
        """
        Get metadata for a domain.
        
        Args:
            domain: Domain name.
            
        Returns:
            DomainMetadata object or None if not found.
        """
        return self._read_metadata(domain)
    
    def delete_domain(self, domain: str) -> bool:
        """
        Delete all data for a domain.
        
        Args:
            domain: Domain name.
            
        Returns:
            True if domain was deleted, False if it doesn't exist.
        """
        domain_path = self._get_domain_path(domain)
        
        if not os.path.exists(domain_path):
            return False
            
        # Delete the data directory
        shutil.rmtree(domain_path)
        
        # Delete the metadata
        metadata_path = self._get_metadata_path(domain)
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
            
        return True 