"""
Main CCStore class for interacting with the CC-Store system.
"""

import os
import logging
from typing import Dict, List, Optional, Union, Tuple, Any
import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

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
        schema: Optional[StructType] = None
    ):
        """
        Initialize a CCStore instance.
        
        Args:
            storage_path: Path to the storage directory
            spark: Optional SparkSession instance
            schema: Optional schema to use for empty DataFrames
        """
        self.storage_path = storage_path
        self.spark = spark or self._create_spark_session()
        
        # 初始化存储管理器
        self.storage = StorageManager(storage_path, self.spark, schema=schema)
    
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
    
    def write_data(
        self,
        df: DataFrame,
        date: Optional[str] = None
    ) -> bool:
        """
        Write data to storage.
        
        Args:
            df: DataFrame containing the data
            date: Optional date string (format: "YYYYMMDD")
            
        Returns:
            True if write was successful, False otherwise
        """
        try:
            self.storage.write_dataframe(df, date)
            return True
        except Exception as e:
            logger.error(f"写入数据失败: {str(e)}")
            return False
    
    def write_incremental_data(
        self,
        df: DataFrame,
        date: Optional[str] = None
    ) -> bool:
        """
        将增量数据写入增量存储区域，按日期组织
        
        Args:
            df: DataFrame containing the data
            date: Optional date string (format: "YYYYMMDD")
            
        Returns:
            True if write was successful, False otherwise
        """
        if not date:
            date = datetime.datetime.now().strftime("%Y%m%d")
            
        # 写入增量存储区域
        incremental_path = os.path.join(self.storage.incremental_path, date)
        
        try:
            # 确保增量数据按domain分区
            df.write.partitionBy("domain").parquet(incremental_path)
            return True
        except Exception as e:
            logger.error(f"Error writing incremental data for date {date}: {str(e)}")
            return False
    
    def read_domain(
        self,
        domain: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_html: bool = True,
        limit: Optional[int] = None
    ) -> DataFrame:
        """
        Read data for a domain from storage.
        
        Args:
            domain: Domain name
            start_date: Optional start date (format: "YYYYMMDD")
            end_date: Optional end date (format: "YYYYMMDD")
            include_html: Whether to include HTML content in result
            limit: Maximum number of records to return
            
        Returns:
            DataFrame containing the data
        """
        return self.storage.read_domain(domain, start_date, end_date, include_html, limit)
    
    def get_domain_metadata(self, domain: str) -> Optional[Dict]:
        """
        Get metadata for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary containing domain metadata or None if not found
        """
        return self.storage.get_domain_metadata(domain)
    
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
        return self.storage.list_domains(prefix, limit, offset)
    
    def process_incremental(self, date: Optional[str] = None) -> bool:
        """
        处理特定日期的增量数据并更新主存储
        
        Args:
            date: Optional date string (format: "YYYYMMDD")
            
        Returns:
            True if processing was successful, False otherwise
        """
        if date:
            return len(self.storage.process_incremental_data(date)) > 0
            
        # 如果没有指定日期，处理最近的增量数据
        incremental_dirs = []
        
        if os.path.exists(self.storage.incremental_path):
            for dir_name in os.listdir(self.storage.incremental_path):
                if os.path.isdir(os.path.join(self.storage.incremental_path, dir_name)):
                    # 确保是日期格式 YYYYMMDD
                    if len(dir_name) == 8 and dir_name.isdigit():
                        incremental_dirs.append(dir_name)
        
        if not incremental_dirs:
            logger.info("No incremental data to process")
            return True
                
        # 选择最近的日期
        date = max(incremental_dirs)
        return len(self.storage.process_incremental_data(date)) > 0
    
    def merge_incremental(self) -> bool:
        """
        合并所有增量数据到主存储
        
        Returns:
            True if merge was successful, False otherwise
        """
        return self.storage.merge_incremental_data()
    
    def get_domain_statistics(self, domain: str) -> Dict[str, Any]:
        """
        Get statistics for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary with domain statistics
        """
        metadata = self.get_domain_metadata(domain)
        if not metadata:
            return {
                "domain": domain,
                "total_records": 0,
                "total_files": 0,
                "total_size_bytes": 0,
                "date_range": {
                    "min_date": None,
                    "max_date": None
                }
            }
        
        return {
            "domain": domain,
            "total_records": metadata.get("total_records", 0),
            "total_files": metadata.get("total_files", 0),
            "total_size_bytes": metadata.get("total_size_bytes", 0),
            "date_range": {
                "min_date": metadata.get("min_date"),
                "max_date": metadata.get("max_date")
            }
        }
    
    def close(self):
        """
        Close connections and clean up resources.
        """
        if hasattr(self, 'metadata') and self.metadata:
            self.metadata.close()
        
        if hasattr(self, 'spark') and self.spark:
            self.spark.stop()

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
            include_html=True
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
        domain_metadata = self.storage.get_domain_metadata(domain) or {
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
        domain_metadata["total_records"] += sum(file.record_count for file in file_metadata)
        domain_metadata["total_files"] += len(file_metadata)
        domain_metadata["total_size_bytes"] += sum(file.size_bytes for file in file_metadata)
        
        # Get current dates for this domain
        if "min_date" in domain_metadata and "max_date" in domain_metadata:
            current_dates = []
            min_date = domain_metadata["min_date"]
            max_date = domain_metadata["max_date"]
            
            # Simple way to create a list of dates between min and max
            if min_date and max_date:
                try:
                    min_date_obj = datetime.datetime.strptime(min_date, "%Y%m%d")
                    max_date_obj = datetime.datetime.strptime(max_date, "%Y%m%d")
                    
                    delta = max_date_obj - min_date_obj
                    for i in range(delta.days + 1):
                        day = min_date_obj + datetime.timedelta(days=i)
                        current_dates.append(day.strftime("%Y%m%d"))
                except (ValueError, TypeError):
                    current_dates = [min_date, max_date]
        else:
            current_dates = []
            
        # Update date range
        if date not in current_dates:
            current_dates.append(date)
            current_dates.sort()
            
            # Update date-related metadata
            domain_metadata["min_date"] = current_dates[0]
            domain_metadata["max_date"] = current_dates[-1]
            domain_metadata["date_count"] = len(current_dates)
        
        # Update the files list in metadata
        if "files" not in domain_metadata:
            domain_metadata["files"] = []
            
        # Add new file records
        for file in file_metadata:
            domain_metadata["files"].append(file.to_dict())
        
        # For now, just return True as we don't have a direct way to update metadata
        # In a production system, you would save this back to the storage layer
        return True 