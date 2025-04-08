"""
Storage backend for the CC-Store system.
"""

import os
import logging
import json
from typing import Dict, List, Optional, Union, Tuple, Any
import datetime
import fnmatch
import shutil
import time
import hashlib
import pandas as pd
from pathlib import Path
import tempfile
import glob

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, ArrayType, LongType

from cc_store.core.models import DomainMetadata, FileMetadata, FileRecord


logger = logging.getLogger(__name__)


class StorageManager:
    """
    Storage manager for the CC-Store system.
    
    This class handles the storage and retrieval of document data,
    按域名哈希分桶存储，保证同一域名数据连续存储。每个文件约2GB。
    """
    
    def __init__(
        self,
        storage_path: str,
        spark: SparkSession,
        schema: Optional[StructType] = None,
        target_part_size_bytes: int = 2 * 1024 * 1024 * 1024,  # 目标2GB每文件
        min_records_per_part: int = 10000
    ):
        """
        Initialize the storage manager.
        
        Args:
            storage_path: Base path for storing data
            spark: SparkSession to use
            schema: Optional schema to use for empty DataFrames
            target_part_size_bytes: Target size for each part file in bytes
            min_records_per_part: Minimum number of records per part file
        """
        self.storage_path = storage_path
        self.spark = spark
        self._schema = schema  # 配置的默认schema
        self._cached_schema = None  # 运行时缓存的schema
        self.target_part_size_bytes = target_part_size_bytes
        self.min_records_per_part = min_records_per_part
        
        # 初始化目录结构
        self.data_path = os.path.join(storage_path, "data")
        os.makedirs(self.data_path, exist_ok=True)
        
        self.metadata_path = os.path.join(storage_path, "metadata")
        os.makedirs(self.metadata_path, exist_ok=True)
        
        self.domain_index_path = os.path.join(self.metadata_path, "domain_index")
        os.makedirs(self.domain_index_path, exist_ok=True)
        
        self.incremental_path = os.path.join(storage_path, "incremental")
        os.makedirs(self.incremental_path, exist_ok=True)
    
    def compute_domain_hash_id(self, domain: str) -> int:
        """
        计算域名的哈希ID
        
        Args:
            domain: 域名
            
        Returns:
            哈希ID (0-999)
        """
        if not domain:
            return None
        import xxhash
        return xxhash.xxh64_intdigest(domain) % 1000
    
    def format_hash_id(self, hash_id: int) -> str:
        """
        格式化哈希ID为四位字符串
        
        Args:
            hash_id: 哈希ID
            
        Returns:
            四位字符串表示的哈希ID
        """
        return f"{hash_id:04d}"
    
    def get_bucket_path(self, hash_id: int) -> str:
        """
        获取哈希桶的路径
        
        Args:
            hash_id: 哈希ID
            
        Returns:
            哈希桶的绝对路径
        """
        formatted_id = self.format_hash_id(hash_id)
        return os.path.join(self.data_path, formatted_id)
    
    def get_metadata_path(self, domain_hash_id: int) -> str:
        """
        Get the path to the metadata file for the given domain hash ID.
        
        Args:
            domain_hash_id: Domain hash ID
        
        Returns:
            Path to the metadata file
        """
        formatted_id = f"{domain_hash_id:03d}"  # 将ID格式化为3位数，前导零填充
        metadata_dir = os.path.join(self.metadata_path, "domain_index")
        os.makedirs(metadata_dir, exist_ok=True)
        return os.path.join(metadata_dir, f"{formatted_id}.parquet")
    
    def _get_domain_path(self, domain: str) -> str:
        """
        按照新的存储结构获取域名对应的目录路径
        
        Args:
            domain: Domain name.
            
        Returns:
            Path to the domain directory.
        """
        hash_id = self.compute_domain_hash_id(domain)
        return self.get_bucket_path(hash_id)
    
    def write_dataframe(self, domain: str, df: DataFrame, 
                       date: str = None,
                       output_path: Optional[str] = None,
                       partition_cols: Optional[List[str]] = None,
                       format: str = "parquet", 
                       index: bool = False) -> List[FileRecord]:
        """
        Write a dataframe to storage, using domain hash ID for bucket organization
        and creating files of approximately 2GB each.
        
        Args:
            domain: Domain name
            df: DataFrame to write
            date: Date string (format: "YYYYMMDD") 
            output_path: Optional explicit output path
            partition_cols: Additional columns to partition by
            format: Output format (parquet or csv)
            index: Whether to write the index
            
        Returns:
            List of FileRecord objects for the written files
        """
        # Create target dir if it doesn't exist
        if output_path is None:
            if self.data_path is None:
                raise ValueError("No data_path set and no output_path provided")
            output_path = self.data_path
        os.makedirs(output_path, exist_ok=True)
            
        # Timestamp for this write operation
        current_timestamp = int(time.time())
        
        # Compute domain hash
        domain_hash_id = self.compute_domain_hash_id(domain)
        formatted_hash = self.format_hash_id(domain_hash_id)
        
        # Create bucket directory structure
        bucket_dir = os.path.join(output_path, formatted_hash)
        os.makedirs(bucket_dir, exist_ok=True)
        
        # Estimate size to determine how to partition the data
        # For simplicity, estimate based on record count
        # Assuming approximately 1KB per record on average
        record_count = df.count()
        estimated_size_bytes = record_count * 1024  # Rough estimate
        
        # Determine if data needs to be split into multiple files
        # Target is 2GB per file
        target_size_bytes = 2 * 1024 * 1024 * 1024  # 2GB
        
        # Number of partitions needed
        num_partitions = max(1, int((estimated_size_bytes + target_size_bytes - 1) / target_size_bytes))
        
        # Find existing part files to determine next part number
        existing_parts = glob.glob(os.path.join(bucket_dir, "part-*.parquet"))
        next_part_num = len(existing_parts) + 1
        
        file_records = []
        
        # Setup Spark write options for Parquet
        write_options = {
            "compression": "snappy",
            "parquet.bloom.filter.enabled#url": "true",
            "parquet.bloom.filter.expected.ndv#url": "100000",
            "parquet.enable.dictionary": "true",
            "parquet.page.size": "1048576",  # 1MB
            "parquet.block.size": "67108864"  # 64MB
        }
        
        # Ensure we're writing with the right partitioning
        repartition_df = df.repartition(1)  # Force to a single partition initially
        
        # Process each partition separately as a single file
        for i in range(num_partitions):
            part_num = next_part_num + i
            filename = f"part-{part_num:05d}.parquet"
            filepath = os.path.join(bucket_dir, filename)
            
            # Calculate record ranges for this partition
            records_per_partition = record_count // num_partitions
            start_idx = i * records_per_partition
            
            # Handle the last partition to include remaining records
            if i < num_partitions - 1:
                end_idx = (i + 1) * records_per_partition - 1
                part_record_count = records_per_partition
                part_df = df.limit(records_per_partition).offset(start_idx)
            else:
                # Last partition gets remaining records
                part_record_count = record_count - start_idx
                part_df = df.offset(start_idx)
            
            # Create a temporary directory for writing
            temp_dir = tempfile.mkdtemp()
            temp_file_path = os.path.join(temp_dir, "temp.parquet")
            
            try:
                # Write data to temporary location
                part_df.coalesce(1).write.format("parquet").options(**write_options).mode("overwrite").save(temp_file_path)
                
                # Find the written file
                temp_parquet_files = glob.glob(os.path.join(temp_file_path, "*.parquet"))
                
                if temp_parquet_files:
                    # Move the first file to the target location
                    shutil.move(temp_parquet_files[0], filepath)
                else:
                    raise FileNotFoundError(f"No parquet files found in {temp_file_path}")
                
                # Estimate file size (in a real implementation, get the actual file size)
                file_size = os.path.getsize(filepath)
                
                # Create file record
                file_record = FileRecord(
                    filepath=filepath,
                    start_offset=start_idx,
                    end_offset=start_idx + part_record_count - 1,
                    record_count=part_record_count,
                    timestamp=current_timestamp,
                    size_bytes=file_size
                )
                
                file_records.append(file_record)
                
            finally:
                # Clean up temporary directory
                shutil.rmtree(temp_dir)
        
        # Update domain metadata for each file
        for file_record in file_records:
            self.update_domain_metadata(
                domain=domain,
                domain_hash_id=domain_hash_id,
                file_record=file_record
            )
        
        return file_records

    def update_domain_metadata(self, domain: str, domain_hash_id: int, 
                              file_record: FileRecord) -> None:
        """
        Update metadata for a domain with new file information.
        
        Args:
            domain: Domain name
            domain_hash_id: Computed domain hash ID
            file_record: Record for the file containing domain data
        """
        # 获取元数据文件路径
        metadata_path = self.get_metadata_path(domain_hash_id)
        
        # 尝试读取现有元数据文件
        domain_metadata = None
        all_domains_metadata = {}
        
        if os.path.exists(metadata_path):
            try:
                # 读取所有域名的元数据
                metadata_df = self.spark.read.parquet(metadata_path)
                for row in metadata_df.collect():
                    row_dict = row.asDict()
                    all_domains_metadata[row_dict["domain"]] = row_dict
                
                # 获取当前域名的元数据
                if domain in all_domains_metadata:
                    domain_metadata = DomainMetadata.from_dict(all_domains_metadata[domain])
            except Exception as e:
                logger.warning(f"读取元数据文件失败 {metadata_path}: {str(e)}")
        
        # 如果元数据不存在，创建新的
        if domain_metadata is None:
            domain_metadata = DomainMetadata(domain=domain, domain_hash_id=domain_hash_id)
        
        # 提取日期信息
        try:
            # 假设文件名格式包含时间戳，如YYYYMMDD
            filename = os.path.basename(file_record.filepath)
            if filename.startswith("part-"):
                # 新的文件命名格式，尝试从路径中提取日期
                dir_path = os.path.dirname(file_record.filepath)
                if "date=" in dir_path:
                    date_part = dir_path.split("date=")[1].split("/")[0]
                    if len(date_part) == 8 and date_part.isdigit():
                        date_str = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                        
                        # 更新最小/最大日期
                        if domain_metadata.min_date is None or date_str < domain_metadata.min_date:
                            domain_metadata.min_date = date_str
                        if domain_metadata.max_date is None or date_str > domain_metadata.max_date:
                            domain_metadata.max_date = date_str
                            
                        # 更新日期计数
                        domain_metadata.date_count += 1
            else:
                # 旧的文件命名格式
                parts = filename.split('_')
                if len(parts) >= 2:
                    date_part = parts[-1].split('.')[0][:8]  # 提取YYYYMMDD
                    if len(date_part) == 8 and date_part.isdigit():
                        date_str = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                        
                        # 更新最小/最大日期
                        if domain_metadata.min_date is None or date_str < domain_metadata.min_date:
                            domain_metadata.min_date = date_str
                        if domain_metadata.max_date is None or date_str > domain_metadata.max_date:
                            domain_metadata.max_date = date_str
                            
                        # 更新日期计数
                        domain_metadata.date_count += 1
        except Exception as e:
            # 记录错误但继续执行
            logging.warning(f"从文件名提取日期失败: {e}")
        
        # 更新元数据
        domain_metadata.total_records += file_record.record_count
        domain_metadata.total_files += 1
        domain_metadata.total_size_bytes += file_record.size_bytes
        domain_metadata.files.append(file_record)  # 使用FileRecord对象
        
        # 更新全局元数据字典
        all_domains_metadata[domain] = domain_metadata.to_dict()
        
        # 将元数据转换为DataFrame并保存
        metadata_rows = [row for row in all_domains_metadata.values()]
        
        # 创建明确的Schema
        schema = StructType([
            StructField("domain", StringType(), False),
            StructField("domain_hash_id", IntegerType(), False),
            StructField("total_records", LongType(), True),
            StructField("total_files", IntegerType(), True),
            StructField("total_size_bytes", LongType(), True),
            StructField("min_date", StringType(), True),
            StructField("max_date", StringType(), True),
            StructField("date_count", IntegerType(), True),
            StructField("files", ArrayType(
                StructType([
                    StructField("filepath", StringType(), True),
                    StructField("start_offset", IntegerType(), True),
                    StructField("end_offset", IntegerType(), True),
                    StructField("record_count", IntegerType(), True),
                    StructField("timestamp", LongType(), True),
                    StructField("size_bytes", LongType(), True)
                ])
            ), True)
        ])
        
        # 使用临时目录和文件来确保写入单个文件
        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, "temp.parquet")
        
        try:
            # 使用显式schema创建DataFrame
            metadata_df = self.spark.createDataFrame(metadata_rows, schema=schema)
            
            # 写入临时位置
            metadata_df.coalesce(1).write.mode("overwrite").parquet(temp_file_path)
            
            # 找到写入的文件
            temp_parquet_files = glob.glob(os.path.join(temp_file_path, "*.parquet"))
            
            if temp_parquet_files:
                # 确保目标目录存在
                os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
                
                # 将第一个文件移动到目标位置
                shutil.move(temp_parquet_files[0], metadata_path)
            else:
                raise FileNotFoundError(f"No parquet files found in {temp_file_path}")
        finally:
            # 清理临时目录
            shutil.rmtree(temp_dir)
    
    def read_domain(self, domain: str, 
                start_date: Optional[str] = None, 
                end_date: Optional[str] = None,
                include_html: bool = True,
                limit: Optional[int] = None) -> DataFrame:
        """
        Read data for a domain.
        
        Args:
            domain: Domain name
            start_date: Optional start date (format: "YYYYMMDD")
            end_date: Optional end date (format: "YYYYMMDD")
            include_html: Whether to include HTML content
            limit: Maximum number of records to return
            
        Returns:
            DataFrame with domain data
        """
        domain_hash_id = self.compute_domain_hash_id(domain)
        bucket_path = self.get_bucket_path(domain_hash_id)
        
        if not os.path.exists(bucket_path):
            # Return empty DataFrame if bucket not found
            return self._create_empty_dataframe()
        
        # Find all parquet files for this bucket
        all_files = glob.glob(os.path.join(bucket_path, "part-*.parquet"))
        
        if not all_files:
            # Check for other formats too
            all_files = []
            
            # Check for domain-specific directories
            domain_specific_dirs = glob.glob(os.path.join(bucket_path, f"{domain.replace('.', '_')}*"))
            for dir_path in domain_specific_dirs:
                if os.path.isdir(dir_path):
                    dir_files = glob.glob(os.path.join(dir_path, "*.parquet"))
                    all_files.extend(dir_files)
                elif dir_path.endswith(".parquet"):
                    all_files.append(dir_path)
            
            if not all_files:
                # Return empty DataFrame if no files found
                return self._create_empty_dataframe()
        
        # Get domain metadata to find which files contain this domain's data
        metadata = self.get_domain_metadata(domain)
        
        if not metadata or "files" not in metadata:
            # Fallback to reading all files in the bucket
            df = self.spark.read.parquet(*all_files)
            # Filter for the requested domain
            df = df.filter(F.col("domain") == domain)
        else:
            # Get list of files containing this domain's data from metadata
            domain_files = []
            for file_info in metadata.get("files", []):
                # Handle both dict and FileRecord objects
                filepath = file_info.get("filepath") if isinstance(file_info, dict) else file_info.filepath
                
                # Check if file exists
                if os.path.exists(filepath):
                    domain_files.append(filepath)
                    
            if not domain_files:
                # No files found in metadata or files don't exist
                # Fall back to reading all bucket files
                df = self.spark.read.parquet(*all_files)
                # Filter for the requested domain
                df = df.filter(F.col("domain") == domain)
            else:
                # Read the files from metadata
                df = self.spark.read.parquet(*domain_files)
        
        # Apply date filters if specified
        if start_date or end_date:
            if "date" in df.columns:
                # Convert date column to string if needed
                if df.schema["date"].dataType.typeName() != "string":
                    df = df.withColumn("date_str", F.col("date").cast("string"))
                else:
                    df = df.withColumn("date_str", F.col("date"))
                
                # Apply date range filters
                if start_date:
                    df = df.filter(F.col("date_str") >= start_date)
                if end_date:
                    df = df.filter(F.col("date_str") <= end_date)
                
                # Drop temporary date_str column
                df = df.drop("date_str")
        
        # Apply limit if specified
        if limit is not None:
            df = df.limit(limit)
            
        # Filter out HTML if not needed
        if not include_html and "html" in df.columns:
            df = df.drop("html")
            
        return df
    
    def get_domain_metadata(self, domain: str) -> Optional[Dict]:
        """
        获取域名的元数据
        
        Args:
            domain: 域名
            
        Returns:
            域名元数据字典，如果不存在则返回None
        """
        if not domain:
            return None
            
        try:
            hash_id = self.compute_domain_hash_id(domain)
            metadata_path = self.get_metadata_path(hash_id)
            
            if not os.path.exists(metadata_path):
                return None
                
            # 读取所有域名的元数据
            metadata_df = self.spark.read.parquet(metadata_path)
            domain_records = metadata_df.filter(F.col("domain") == domain).collect()
            
            if domain_records and len(domain_records) > 0:
                return domain_records[0].asDict()
            else:
                return None
        except Exception as e:
            logger.error(f"读取域名元数据失败: {str(e)}")
            return None
    
    def list_domains(
        self, 
        prefix: Optional[str] = None, 
        limit: int = 100, 
        offset: int = 0
    ) -> List[str]:
        """
        列出存储中的域名
        
        Args:
            prefix: 可选的域名前缀
            limit: 返回结果数量限制
            offset: 分页偏移量
            
        Returns:
            域名列表
        """
        domains = []
        
        # 扫描所有元数据文件
        for i in range(1000):
            metadata_path = self.get_metadata_path(i)
            if not os.path.exists(metadata_path):
                continue
            
            try:
                metadata_df = self.spark.read.parquet(metadata_path)
                
                # 应用前缀过滤
                if prefix:
                    metadata_df = metadata_df.filter(F.col("domain").startswith(prefix))
                
                # 收集域名
                for row in metadata_df.select("domain").collect():
                    domains.append(row.domain)
            except Exception as e:
                logger.error(f"读取元数据文件失败 {metadata_path}: {str(e)}")
        
        # 排序并应用分页
        domains.sort()
        
        if offset >= len(domains):
            return []
        
        end = min(offset + limit, len(domains))
        return domains[offset:end]
    
    def process_incremental_data(self, date: str) -> List[Dict]:
        """
        处理特定日期的增量数据
        
        Args:
            date: 日期字符串
            
        Returns:
            处理的文件元数据列表
        """
        incremental_path = os.path.join(self.incremental_path, date)
        if not os.path.exists(incremental_path):
            logger.warning(f"增量数据不存在: {date}")
            return []
        
        try:
            # 读取增量数据
            incremental_df = self.spark.read.parquet(incremental_path)
            
            # 写入主存储
            metadata = self.write_dataframe(incremental_df, "domain")
            
            # 处理完成后删除增量数据
            shutil.rmtree(incremental_path)
            
            return metadata
        except Exception as e:
            logger.error(f"处理增量数据失败 {date}: {str(e)}")
            return []
    
    def merge_incremental_data(self) -> bool:
        """
        合并所有增量数据
        
        Returns:
            合并是否成功
        """
        # 获取所有增量数据目录
        incremental_dirs = []
        
        if os.path.exists(self.incremental_path):
            for dirname in os.listdir(self.incremental_path):
                if os.path.isdir(os.path.join(self.incremental_path, dirname)):
                    if len(dirname) == 8 and dirname.isdigit():
                        incremental_dirs.append(dirname)
        
        if not incremental_dirs:
            logger.info("没有增量数据需要合并")
            return True
        
        # 按日期排序处理
        incremental_dirs.sort()
        
        success = True
        for date in incremental_dirs:
            try:
                logger.info(f"处理增量数据: {date}")
                self.process_incremental_data(date)
            except Exception as e:
                logger.error(f"处理增量数据失败 {date}: {str(e)}")
                success = False
        
        return success
    
    def _create_empty_dataframe(self) -> DataFrame:
        """
        创建空的DataFrame
        
        Returns:
            空DataFrame
        """
        if self._cached_schema:
            return self.spark.createDataFrame([], self._cached_schema)
        elif self._schema:
            return self.spark.createDataFrame([], self._schema)
        else:
            # 默认schema
            default_schema = StructType([
                StructField("domain", StringType(), True),
                StructField("url", StringType(), True),
                StructField("date", IntegerType(), True),
                StructField("content_length", IntegerType(), True),
                StructField("status", IntegerType(), True),
                StructField("html", StringType(), True)
            ])
            return self.spark.createDataFrame([], default_schema)


# Keep the original StorageBackend for backward compatibility
class StorageBackend:
    """
    Base storage backend for the CC-Store system.
    
    This class is retained for backward compatibility.
    New code should use StorageManager instead.
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
        # Use StorageManager for implementation
        self.manager = StorageManager(
            storage_path=storage_path,
            spark=spark,
            target_part_size_bytes=target_part_size_bytes,
            min_records_per_part=min_records_per_part
        )
        
        self.storage_path = storage_path
        self.spark = spark
        self.target_part_size_bytes = target_part_size_bytes
        self.min_records_per_part = min_records_per_part
    
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