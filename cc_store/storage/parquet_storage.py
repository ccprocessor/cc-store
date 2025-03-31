"""
Parquet-based storage backend implementation.
"""

import os
import hashlib
import math
from typing import Dict, List, Optional, Any, Tuple, Union
import datetime

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from cc_store.core import StorageBackend
from cc_store.core.models import CCDocument, FileMetadata


class ParquetStorageBackend(StorageBackend):
    """
    Parquet-based implementation of the storage backend.
    
    This class implements the StorageBackend interface using Parquet files
    stored in a hierarchical directory structure optimized for domain-based access.
    """
    
    def __init__(
        self,
        base_path: str,
        spark: Optional[SparkSession] = None,
        max_file_size_gb: float = 2.0,
        metadata_manager=None,
        html_content_store=None
    ):
        """
        Initialize the Parquet storage backend.
        
        Args:
            base_path: Base path for storing data files
            spark: Optional SparkSession instance
            max_file_size_gb: Maximum size of a single file in GB
            metadata_manager: Metadata manager implementation
            html_content_store: HTML content store implementation
        """
        self.base_path = base_path
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.max_file_size_gb = max_file_size_gb
        self.metadata_manager = metadata_manager
        self.html_content_store = html_content_store
        
        # Schema for CommonCrawl documents
        self.schema = self._create_schema()
    
    def _create_schema(self) -> StructType:
        """Create the schema for CommonCrawl documents."""
        return StructType([
            StructField("raw_warc_path", StringType(), False),
            StructField("track_id", StringType(), False),
            StructField("domain", StringType(), False),
            StructField("url", StringType(), False),
            StructField("status", IntegerType(), False),
            StructField("response_header", StringType(), True),  # JSON string
            StructField("date", LongType(), False),
            StructField("content_length", IntegerType(), False),
            StructField("content_charset", StringType(), True),
            StructField("html", StringType(), True),
            StructField("content_hash", StringType(), True),
            StructField("remark", StringType(), True)  # JSON string
        ])
    
    def _get_domain_bucket(self, domain: str) -> str:
        """
        Get the bucket ID for a domain based on hash.
        
        Args:
            domain: Domain name
            
        Returns:
            Bucket ID (3-digit string)
        """
        # Hash the domain and use first 3 digits of hash to distribute domains evenly
        hash_value = int(hashlib.md5(domain.encode()).hexdigest(), 16)
        bucket = hash_value % 1000
        return f"{bucket:03d}"
    
    def _get_domain_path(self, domain: str) -> str:
        """
        Get the path for a domain's data.
        
        Args:
            domain: Domain name
            
        Returns:
            Path to the domain's data directory
        """
        bucket = self._get_domain_bucket(domain)
        return os.path.join(self.base_path, f"domain_bucket={bucket}", f"domain={domain}")
    
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
        domain_path = self._get_domain_path(domain)
        
        # Check if we should use metadata to find files
        if self.metadata_manager and date_range:
            files = self.metadata_manager.get_domain_files(domain, date_range)
            file_paths = [file_info["file_path"] for file_info in files]
            if not file_paths:
                # Return empty DataFrame with correct schema
                return self.spark.createDataFrame([], self.schema)
            df = self.spark.read.parquet(*file_paths)
        else:
            # Read all files in the domain directory
            df = self.spark.read.parquet(domain_path)
            
            # Apply date filter if provided
            if date_range:
                start_date, end_date = date_range
                start_ts = int(datetime.datetime.strptime(start_date, "%Y%m%d").timestamp())
                end_ts = int(datetime.datetime.strptime(end_date, "%Y%m%d").timestamp())
                df = df.filter((F.col("date") >= start_ts) & (F.col("date") <= end_ts))
        
        # Select specific fields if requested
        if fields:
            df = df.select(*fields)
        
        # Apply limit if specified
        if limit:
            df = df.limit(limit)
        
        return df
    
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
        result_dfs = []
        
        for domain in domains:
            domain_df = self.read_by_domain(domain, date_range, fields)
            result_dfs.append(domain_df)
        
        if not result_dfs:
            return self.spark.createDataFrame([], self.schema)
        
        return result_dfs[0].unionAll(*result_dfs[1:]) if len(result_dfs) > 1 else result_dfs[0]
    
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
        # Group data by domain and date
        grouped = dataframe.groupBy("domain", F.date_format(F.from_unixtime("date"), "yyyyMMdd").alias("date_str"))
        
        # Get count by domain and date
        counts_by_domain_date = grouped.count().collect()
        
        stats = {
            "domains_processed": 0,
            "total_records": 0,
            "total_files": 0
        }
        
        for row in counts_by_domain_date:
            domain = row["domain"]
            date_str = row["date_str"]
            count = row["count"]
            
            # Filter dataframe for this domain and date
            domain_date_df = dataframe.filter(
                (F.col("domain") == domain) & 
                (F.date_format(F.from_unixtime("date"), "yyyyMMdd") == date_str)
            )
            
            # Calculate number of parts based on record count and max file size
            # This is a simplistic approach; a more accurate approach would estimate actual size
            avg_record_size_kb = 100  # Estimate average record size (adjusted based on your data)
            max_records_per_part = int((self.max_file_size_gb * 1024 * 1024) / avg_record_size_kb)
            parts_count = max(1, math.ceil(count / max_records_per_part))
            
            # Store files with appropriate naming
            domain_path = self._get_domain_path(domain)
            files_info = []
            
            for part in range(parts_count):
                part_df = domain_date_df.limit(max_records_per_part) if parts_count > 1 else domain_date_df
                
                if parts_count > 1:
                    # For multiple parts, subtract processed records
                    domain_date_df = domain_date_df.subtract(part_df)
                
                # Generate file path
                file_name = f"data_{date_str}_part{part}.parquet" if parts_count > 1 else f"data_{date_str}.parquet"
                file_path = os.path.join(domain_path, file_name)
                
                # Write the file
                part_df.write.parquet(file_path, mode="overwrite" if overwrite else "error")
                
                # Collect file metadata
                file_metadata = self._create_file_metadata(domain, date_str, part, file_path, part_df)
                files_info.append(file_metadata)
            
            # Update metadata if available
            if self.metadata_manager:
                self.metadata_manager.update_metadata_after_write(domain, date_str, files_info)
            
            stats["domains_processed"] += 1
            stats["total_records"] += count
            stats["total_files"] += parts_count
        
        return stats
    
    def _create_file_metadata(
        self, 
        domain: str, 
        date_str: str, 
        part_id: int, 
        file_path: str, 
        dataframe: SparkDataFrame
    ) -> Dict[str, Any]:
        """
        Create metadata for a stored file.
        
        Args:
            domain: Domain name
            date_str: Date string (YYYYMMDD)
            part_id: Part ID
            file_path: Path to the file
            dataframe: DataFrame containing the data
            
        Returns:
            Dictionary with file metadata
        """
        # Collect statistics from the dataframe
        count = dataframe.count()
        min_date = dataframe.agg(F.min("date")).collect()[0][0]
        max_date = dataframe.agg(F.max("date")).collect()[0][0]
        
        # Create a FileMetadata instance
        file_metadata = FileMetadata(
            domain_id=domain,
            date=date_str,
            part_id=part_id,
            file_path=file_path,
            file_size_bytes=0,  # This would be updated after file is written
            records_count=count,
            min_timestamp=datetime.datetime.fromtimestamp(min_date),
            max_timestamp=datetime.datetime.fromtimestamp(max_date),
            created_at=datetime.datetime.now(),
            checksum="",  # This would be computed after file is written
            file_format_version="1.0",
            compression="snappy",
            statistics={}
        )
        
        return file_metadata.to_dict()
    
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
        # Process HTML content deduplication if requested
        if deduplicate and self.html_content_store:
            current_date = datetime.datetime.now().strftime("%Y%m%d")
            dataframe, dedup_stats = self.html_content_store.store_html_content(
                dataframe, current_date, method="exact"
            )
        
        # Write the data
        write_stats = self.write_data(dataframe, overwrite=False)
        
        # Combine statistics
        stats = {
            "write_stats": write_stats
        }
        
        if deduplicate and self.html_content_store:
            stats["deduplication_stats"] = dedup_stats
        
        return stats 