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
        storage_path: str,
        spark: SparkSession,
        target_part_size_bytes: int = 128 * 1024 * 1024,  # 128MB
        min_records_per_part: int = 1000
    ):
        """
        Initialize the Parquet storage backend.
        
        Args:
            storage_path: Base path for storing data
            spark: SparkSession to use
            target_part_size_bytes: Target size for each part file in bytes
            min_records_per_part: Minimum number of records per part file
        """
        self.storage_path = storage_path
        self.spark = spark
        self.target_part_size_bytes = target_part_size_bytes
        self.min_records_per_part = min_records_per_part
        
        # Create directories
        self.data_path = os.path.join(storage_path, "data")
        self.metadata_path = os.path.join(storage_path, "metadata")
        
        # Create required directories
        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(self.metadata_path, exist_ok=True)
        
        # Initialize metadata manager
        self.metadata_manager = FileSystemMetadataManager(self.metadata_path, spark)
    
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
        dataframe: DataFrame,
        overwrite: bool = False,
        partition_size_hint: Optional[int] = None,
        deduplicate: bool = True
    ) -> Dict[str, Any]:
        """
        Write data to storage.
        
        Args:
            dataframe: DataFrame to write
            overwrite: Whether to overwrite existing data
            partition_size_hint: Hint for partition size in MB
            deduplicate: Whether to deduplicate HTML content
            
        Returns:
            Dictionary with statistics about the written data
        """
        # Check required columns
        required_columns = ['domain', 'url']
        for column in required_columns:
            if column not in dataframe.columns:
                raise ValueError(f"DataFrame must have a '{column}' column")
        
        # Calculate the number of records per partition
        total_records = dataframe.count()
        records_per_part = self._calculate_records_per_part(
            dataframe, 
            partition_size_hint
        )
        
        # Repartition the DataFrame
        num_partitions = max(1, total_records // records_per_part)
        repartitioned_df = dataframe.repartition(num_partitions)
        
        # Process each domain's data
        domain_dfs = self._process_domains(repartitioned_df)
        
        # Write each domain's data
        stats = {
            "total_records": total_records,
            "domains_processed": len(domain_dfs),
            "write_stats": {}
        }
        
        for domain, domain_df in domain_dfs.items():
            # Write data for this domain
            domain_stats = self._write_domain_data(
                domain, 
                domain_df,
                overwrite=overwrite
            )
            
            stats["write_stats"][domain] = domain_stats
        
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

    def _write_domain_data(
        self,
        domain: str,
        dataframe: DataFrame,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Write data for a domain.
        
        Args:
            domain: Domain name
            dataframe: DataFrame with domain data
            overwrite: Whether to overwrite existing data
            
        Returns:
            Statistics about written data
        """
        # Create domain directory
        domain_path = self._get_domain_path(domain)
        os.makedirs(domain_path, exist_ok=True)
        
        # Get dates in the dataframe
        dates = dataframe.select(self._get_date_column()).distinct().collect()
        
        stats = {
            "total_records": 0,
            "total_files": 0,
            "dates_processed": len(dates)
        }
        
        for date_row in dates:
            date_str = date_row[0]
            
            # Filter data for this date
            date_df = dataframe.filter(self._get_date_column() == date_str)
            
            # Prepare destination path
            date_path = os.path.join(domain_path, f"date={date_str}")
            
            # Write data
            date_df.write.mode("overwrite" if overwrite else "error").parquet(date_path)
            
            # Update stats
            record_count = date_df.count()
            stats["total_records"] += record_count
            stats["total_files"] += 1
            
            # Create file metadata
            file_info = {
                "path": date_path,
                "record_count": record_count,
                "date": date_str
            }
            
            # Update metadata
            self._update_metadata(domain, date_str, [file_info])
        
        return stats 