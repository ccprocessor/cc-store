"""
HTML content storage with deduplication.
"""

import os
import hashlib
import mmh3
import zlib
import json
from typing import Dict, List, Optional, Any, Tuple, Union
import datetime

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from cc_store.core import HTMLContentStore
from cc_store.core.models import HTMLContentMetadata, HTMLReference


class ParquetHTMLContentStore(HTMLContentStore):
    """
    Parquet-based implementation of HTML content storage with deduplication.
    
    This class implements HTML content storage with deduplication capabilities,
    storing HTML content and references in separate files.
    """
    
    def __init__(
        self,
        base_path: str,
        spark: Optional[SparkSession] = None,
        compression_method: str = "zstd",
        compression_level: int = 3
    ):
        """
        Initialize the HTML content store.
        
        Args:
            base_path: Base path for storing HTML content
            spark: Optional SparkSession instance
            compression_method: Compression method for HTML content
            compression_level: Compression level
        """
        self.base_path = base_path
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.compression_method = compression_method
        self.compression_level = compression_level
        
        # Paths for content and references
        self.content_path = os.path.join(base_path, "html_content")
        self.references_path = os.path.join(base_path, "html_references")
        
        # Ensure directories exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure required directories exist."""
        # This would use appropriate methods based on the storage system
        # For local file system:
        # os.makedirs(self.content_path, exist_ok=True)
        # os.makedirs(self.references_path, exist_ok=True)
        
        # For distributed file systems, this might be handled differently
        # or might not be necessary at all
        pass
    
    def _compute_content_hash(self, html_content: str, method: str = "murmur3") -> str:
        """
        Compute hash for HTML content.
        
        Args:
            html_content: HTML content string
            method: Hash method ('sha256', 'murmur3')
            
        Returns:
            Content hash string
        """
        if not html_content:
            return "empty"
            
        if method == "sha256":
            return hashlib.sha256(html_content.encode("utf-8")).hexdigest()
        elif method == "murmur3":
            # MurmurHash is faster than SHA-256 and still has low collision probability
            hash_value = mmh3.hash(html_content, seed=42)
            return f"{hash_value:x}"  # Convert to hex string
        else:
            raise ValueError(f"Unsupported hash method: {method}")
    
    def _compress_html(self, html_content: str) -> Tuple[bytes, str, int]:
        """
        Compress HTML content.
        
        Args:
            html_content: HTML content string
            
        Returns:
            Tuple of (compressed_data, compression_method, original_size)
        """
        if not html_content:
            return b"", self.compression_method, 0
            
        original_size = len(html_content.encode("utf-8"))
        
        if self.compression_method == "zstd":
            # This would use zstandard library in a real implementation
            # For simplicity, using zlib here
            compressed_data = zlib.compress(
                html_content.encode("utf-8"), 
                level=self.compression_level
            )
        elif self.compression_method == "gzip":
            compressed_data = zlib.compress(
                html_content.encode("utf-8"), 
                level=self.compression_level
            )
        else:
            # Fallback to no compression
            compressed_data = html_content.encode("utf-8")
        
        return compressed_data, self.compression_method, original_size
    
    def _decompress_html(self, compressed_data: bytes, compression_method: str) -> str:
        """
        Decompress HTML content.
        
        Args:
            compressed_data: Compressed HTML data
            compression_method: Compression method used
            
        Returns:
            Decompressed HTML content string
        """
        if not compressed_data:
            return ""
            
        if compression_method in ["zstd", "gzip"]:
            # This would use appropriate library in a real implementation
            # For simplicity, using zlib for both
            decompressed_data = zlib.decompress(compressed_data)
            return decompressed_data.decode("utf-8")
        else:
            # No compression
            return compressed_data.decode("utf-8")
    
    def _get_content_file_path(self, content_hash: str) -> str:
        """
        Get file path for storing HTML content.
        
        Args:
            content_hash: Content hash
            
        Returns:
            Path to the content file
        """
        # Use first 2 characters of hash for directory sharding
        shard = content_hash[:2]
        return os.path.join(self.content_path, shard, f"{content_hash}.bin")
    
    def _get_references_path(self, date: str) -> str:
        """
        Get path for HTML references for a date.
        
        Args:
            date: Date string (YYYYMMDD)
            
        Returns:
            Path to the references directory
        """
        return os.path.join(self.references_path, f"date={date}")
    
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
        if method != "exact":
            # Other methods would be implemented here
            raise ValueError(f"Deduplication method not implemented: {method}")
        
        # Register UDF for content hashing
        def hash_html(html):
            if html is None:
                return None
            return self._compute_content_hash(html, method="murmur3")
        
        hash_udf = F.udf(hash_html, StringType())
        
        # Add content hash column to dataframe
        df_with_hash = dataframe.withColumn("content_hash", hash_udf(F.col("html")))
        
        # Get unique HTML contents and their hashes
        unique_html = df_with_hash.select("html", "content_hash").dropDuplicates(["content_hash"])
        
        # Store HTML contents
        content_count = unique_html.count()
        
        # This is a simplified implementation
        # In a real system, we would:
        # 1. Check which content hashes already exist
        # 2. Only store new content
        # 3. Update metadata for existing content
        
        # For demonstration purposes, we'll just process the DataFrame locally
        # In a real implementation, we would use Spark's distributed processing
        
        content_stats = {
            "total_unique_contents": content_count,
            "new_contents": content_count,
            "updated_contents": 0,
            "total_bytes_original": 0,
            "total_bytes_compressed": 0
        }
        
        # Create references
        references_df = df_with_hash.select(
            "domain",
            "url",
            "content_hash",
            F.lit(date).alias("date"),
            F.lit(1).alias("version")
        )
        
        # Store references as Parquet
        references_path = self._get_references_path(date)
        references_df.write.parquet(references_path, mode="append")
        
        # Return dataframe with content hash and stats
        stats = {
            "content_stats": content_stats,
            "references_count": df_with_hash.count()
        }
        
        return df_with_hash, stats
    
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
        # This is a simplified implementation
        # In a real system, we would:
        # 1. Batch the requests
        # 2. Use Spark to process in parallel
        # 3. Handle any storage-specific retrieval logic
        
        results = {}
        
        # Simulate retrieval
        for content_hash in content_hashes:
            # In a real implementation, we would read from storage
            # For demonstration, return a placeholder
            results[content_hash] = f"<html>Content for {content_hash}</html>"
        
        return results
    
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
        # Initialize stats
        stats = {
            "unique_content_count": 0,
            "total_references": 0,
            "deduplication_ratio": 0.0,
            "storage_savings_bytes": 0,
            "domains_count": 0
        }
        
        # This is a simplified implementation
        # In a real system, we would query the actual data
        
        # Read references data
        references_path = self.references_path
        if date_range:
            start_date, end_date = date_range
            # Filter by date range
            date_filter = f"date >= '{start_date}' AND date <= '{end_date}'"
            references_df = self.spark.read.parquet(references_path).filter(date_filter)
        else:
            references_df = self.spark.read.parquet(references_path)
        
        # Apply domain filter if provided
        if domain:
            references_df = references_df.filter(F.col("domain") == domain)
        
        # Calculate statistics
        try:
            total_references = references_df.count()
            stats["total_references"] = total_references
            
            unique_contents = references_df.select("content_hash").distinct().count()
            stats["unique_content_count"] = unique_contents
            
            if total_references > 0 and unique_contents > 0:
                stats["deduplication_ratio"] = total_references / unique_contents
            
            domains_count = references_df.select("domain").distinct().count()
            stats["domains_count"] = domains_count
            
            # Estimate storage savings (this would be more accurate in a real implementation)
            avg_html_size = 100 * 1024  # 100 KB average size (estimate)
            stats["storage_savings_bytes"] = avg_html_size * (total_references - unique_contents)
        except Exception as e:
            stats["error"] = str(e)
        
        return stats 