"""
Metadata management implementations.
"""

import os
import json
import datetime
from typing import Dict, List, Optional, Any, Tuple

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from cc_store.core import MetadataManager
from cc_store.core.models import DomainMetadata, FileMetadata


class HBaseMetadataManager(MetadataManager):
    """
    HBase-based implementation of metadata management.
    
    This class implements metadata management using HBase as the backend.
    It manages domain-level and file-level metadata for efficient access.
    
    Note: This is a simplified implementation that simulates HBase operations.
    In a real implementation, this would use the HBase API.
    """
    
    def __init__(
        self,
        connection_string: str,
        domain_table: str = "domain_metadata",
        file_table: str = "file_metadata",
        date_index_table: str = "date_index",
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize the HBase metadata manager.
        
        Args:
            connection_string: HBase connection string
            domain_table: Table name for domain metadata
            file_table: Table name for file metadata
            date_index_table: Table name for date index
            spark: Optional SparkSession instance
        """
        self.connection_string = connection_string
        self.domain_table = domain_table
        self.file_table = file_table
        self.date_index_table = date_index_table
        self.spark = spark or SparkSession.builder.getOrCreate()
        
        # In-memory cache for domain metadata
        self._domain_cache = {}
        
        # Connect to HBase (simulated)
        self._connect()
    
    def _connect(self):
        """Connect to HBase (simulated)."""
        # In a real implementation, this would establish a connection to HBase
        # For simulation, we'll just print a message
        print(f"Connected to HBase at {self.connection_string}")
    
    def get_domain_metadata(self, domain: str) -> Dict[str, Any]:
        """
        Get metadata for a specific domain.
        
        Args:
            domain: Domain name to retrieve metadata for
            
        Returns:
            Dictionary containing domain metadata
        """
        # Check cache first
        if domain in self._domain_cache:
            return self._domain_cache[domain]
        
        # In a real implementation, this would query HBase
        # For simulation, return a placeholder
        metadata = {
            "domain_id": domain,
            "bucket_id": "000",  # This would be calculated
            "total_files": 10,
            "total_records": 1000,
            "total_size_bytes": 10_000_000,
            "min_date": "20230101",
            "max_date": "20230630",
            "date_count": 5,
            "last_updated": datetime.datetime.now().isoformat(),
            "stats": {}
        }
        
        # Cache the result
        self._domain_cache[domain] = metadata
        
        return metadata
    
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
        # In a real implementation, this would query HBase
        # For simulation, return a placeholder list
        domains = [
            "example.com",
            "example.org",
            "example.net",
            "sample.com",
            "test.com"
        ]
        
        # Apply prefix filter if provided
        if prefix:
            domains = [d for d in domains if d.startswith(prefix)]
        
        # Apply pagination
        domains = domains[offset:offset + limit]
        
        return domains
    
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
        # In a real implementation, this would query HBase
        # For simulation, return placeholder data
        
        # Generate some sample file metadata
        files = []
        
        if date_range:
            start_date, end_date = date_range
            # Convert to datetime objects for easier manipulation
            start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
            
            # Generate a file for each date in the range
            current_dt = start_dt
            while current_dt <= end_dt:
                date_str = current_dt.strftime("%Y%m%d")
                
                # Create a file metadata entry
                file_metadata = {
                    "domain_id": domain,
                    "date": date_str,
                    "part_id": 0,
                    "file_path": f"/path/to/data/{domain}/{date_str}.parquet",
                    "file_size_bytes": 10_000_000,
                    "records_count": 1000,
                    "min_timestamp": (current_dt - datetime.timedelta(hours=12)).isoformat(),
                    "max_timestamp": current_dt.isoformat(),
                    "created_at": (current_dt + datetime.timedelta(days=1)).isoformat(),
                    "checksum": "abcdef1234567890",
                    "file_format_version": "1.0",
                    "compression": "snappy",
                    "statistics": {}
                }
                
                files.append(file_metadata)
                
                # Move to the next day
                current_dt += datetime.timedelta(days=1)
        else:
            # Without date range, just return some sample files
            for i in range(5):
                date_str = f"2023010{i+1}"
                
                file_metadata = {
                    "domain_id": domain,
                    "date": date_str,
                    "part_id": 0,
                    "file_path": f"/path/to/data/{domain}/{date_str}.parquet",
                    "file_size_bytes": 10_000_000,
                    "records_count": 1000,
                    "min_timestamp": f"2023-01-0{i+1}T00:00:00",
                    "max_timestamp": f"2023-01-0{i+1}T23:59:59",
                    "created_at": f"2023-01-0{i+2}T12:00:00",
                    "checksum": f"checksum{i}",
                    "file_format_version": "1.0",
                    "compression": "snappy",
                    "statistics": {}
                }
                
                files.append(file_metadata)
        
        return files
    
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
        # In a real implementation, this would:
        # 1. Update the file metadata table
        # 2. Update the date index table
        # 3. Update the domain metadata table
        
        # For simulation, we'll just print a message
        print(f"Updated metadata for domain {domain}, date {date}, files: {len(files_info)}")
        
        # Update domain metadata in cache
        if domain in self._domain_cache:
            domain_metadata = self._domain_cache[domain]
            
            # Update total files
            domain_metadata["total_files"] += len(files_info)
            
            # Update total records
            total_records = sum(file_info["records_count"] for file_info in files_info)
            domain_metadata["total_records"] += total_records
            
            # Update total size
            total_size = sum(file_info["file_size_bytes"] for file_info in files_info)
            domain_metadata["total_size_bytes"] += total_size
            
            # Update date range
            if date < domain_metadata["min_date"]:
                domain_metadata["min_date"] = date
            if date > domain_metadata["max_date"]:
                domain_metadata["max_date"] = date
            
            # Update last updated timestamp
            domain_metadata["last_updated"] = datetime.datetime.now().isoformat()
            
            # Update cache
            self._domain_cache[domain] = domain_metadata


class ParquetMetadataManager(MetadataManager):
    """
    Parquet-based implementation of metadata management.
    
    This class implements metadata management using Parquet files as the backend.
    It is suitable for systems where HBase is not available or for testing.
    """
    
    def __init__(
        self,
        base_path: str,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize the Parquet metadata manager.
        
        Args:
            base_path: Base path for storing metadata
            spark: Optional SparkSession instance
        """
        self.base_path = base_path
        self.spark = spark or SparkSession.builder.getOrCreate()
        
        # Paths for metadata tables
        self.domain_path = os.path.join(base_path, "domain_metadata")
        self.file_path = os.path.join(base_path, "file_metadata")
        self.date_index_path = os.path.join(base_path, "date_index")
        
        # In-memory cache for domain metadata
        self._domain_cache = {}
    
    def get_domain_metadata(self, domain: str) -> Dict[str, Any]:
        """
        Get metadata for a specific domain.
        
        Args:
            domain: Domain name to retrieve metadata for
            
        Returns:
            Dictionary containing domain metadata
        """
        # Check cache first
        if domain in self._domain_cache:
            return self._domain_cache[domain]
        
        # Read domain metadata from Parquet
        try:
            df = self.spark.read.parquet(self.domain_path).filter(F.col("domain_id") == domain)
            
            if df.count() > 0:
                # Convert to dictionary
                metadata = df.first().asDict()
                
                # Cache the result
                self._domain_cache[domain] = metadata
                
                return metadata
            else:
                # Domain not found, return empty metadata
                return {
                    "domain_id": domain,
                    "error": "Domain not found"
                }
        except Exception as e:
            # Handle errors, such as file not found
            return {
                "domain_id": domain,
                "error": str(e)
            }
    
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
        try:
            # Read domain metadata from Parquet
            df = self.spark.read.parquet(self.domain_path).select("domain_id")
            
            # Apply prefix filter if provided
            if prefix:
                df = df.filter(F.col("domain_id").startswith(prefix))
            
            # Apply pagination
            df = df.orderBy("domain_id").limit(limit).offset(offset)
            
            # Extract domain names
            domains = [row["domain_id"] for row in df.collect()]
            
            return domains
        except Exception as e:
            # Handle errors, such as file not found
            return []
    
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
        try:
            # Read file metadata from Parquet
            df = self.spark.read.parquet(self.file_path).filter(F.col("domain_id") == domain)
            
            # Apply date filter if provided
            if date_range:
                start_date, end_date = date_range
                df = df.filter((F.col("date") >= start_date) & (F.col("date") <= end_date))
            
            # Convert to list of dictionaries
            files = [row.asDict() for row in df.collect()]
            
            return files
        except Exception as e:
            # Handle errors, such as file not found
            return []
    
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
        try:
            # Update file metadata
            files_df = self.spark.createDataFrame(files_info)
            files_df.write.mode("append").parquet(self.file_path)
            
            # Update date index
            date_stats = {
                "date": date,
                "domain_id": domain,
                "files_count": len(files_info),
                "total_size_bytes": sum(file_info["file_size_bytes"] for file_info in files_info),
                "records_count": sum(file_info["records_count"] for file_info in files_info)
            }
            date_df = self.spark.createDataFrame([date_stats])
            date_df.write.mode("append").parquet(self.date_index_path)
            
            # Update domain metadata
            try:
                # Read existing domain metadata
                domain_df = self.spark.read.parquet(self.domain_path).filter(F.col("domain_id") == domain)
                
                if domain_df.count() > 0:
                    # Domain exists, update metadata
                    domain_metadata = domain_df.first().asDict()
                    
                    # Update fields
                    domain_metadata["total_files"] += len(files_info)
                    domain_metadata["total_records"] += sum(file_info["records_count"] for file_info in files_info)
                    domain_metadata["total_size_bytes"] += sum(file_info["file_size_bytes"] for file_info in files_info)
                    
                    # Update date range
                    if date < domain_metadata["min_date"]:
                        domain_metadata["min_date"] = date
                    if date > domain_metadata["max_date"]:
                        domain_metadata["max_date"] = date
                    
                    # Update date count (this is simplified)
                    domain_metadata["date_count"] += 1
                    
                    # Update last updated timestamp
                    domain_metadata["last_updated"] = datetime.datetime.now().isoformat()
                    
                    # Write updated metadata
                    updated_df = self.spark.createDataFrame([domain_metadata])
                    
                    # Delete old entry and append new one
                    # This is not an efficient approach for a real system,
                    # but it simulates an update operation
                    temp_df = self.spark.read.parquet(self.domain_path).filter(F.col("domain_id") != domain)
                    temp_df.union(updated_df).write.mode("overwrite").parquet(self.domain_path)
                    
                    # Update cache
                    self._domain_cache[domain] = domain_metadata
                else:
                    # Domain doesn't exist, create new metadata
                    domain_metadata = {
                        "domain_id": domain,
                        "bucket_id": self._get_bucket_id(domain),
                        "total_files": len(files_info),
                        "total_records": sum(file_info["records_count"] for file_info in files_info),
                        "total_size_bytes": sum(file_info["file_size_bytes"] for file_info in files_info),
                        "min_date": date,
                        "max_date": date,
                        "date_count": 1,
                        "last_updated": datetime.datetime.now().isoformat(),
                        "stats": {}
                    }
                    
                    # Write new metadata
                    new_df = self.spark.createDataFrame([domain_metadata])
                    new_df.write.mode("append").parquet(self.domain_path)
                    
                    # Update cache
                    self._domain_cache[domain] = domain_metadata
            except Exception as e:
                # Handle domain metadata update errors
                print(f"Error updating domain metadata: {e}")
        
        except Exception as e:
            # Handle overall update errors
            print(f"Error updating metadata: {e}")
    
    def _get_bucket_id(self, domain: str) -> str:
        """
        Get bucket ID for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Bucket ID (3-digit string)
        """
        # This is a simplified implementation
        # In a real system, this would use a consistent hashing algorithm
        return "000" 