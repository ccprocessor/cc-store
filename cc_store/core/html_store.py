"""
HTML content store for CC-Store.

This module provides functionality for storing and retrieving HTML content
with automatic deduplication.
"""

import os
import logging
import zlib
from typing import Dict, List, Optional, Tuple, Union
import datetime
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from cc_store.core.models import HTMLContentMetadata, HTMLReference
from cc_store.utils.hash_utils import compute_content_hash


logger = logging.getLogger(__name__)


class HTMLContentStore:
    """Store for HTML content with deduplication."""
    
    def __init__(
        self,
        storage_path: str,
        spark: Optional[SparkSession] = None,
        compression_level: int = 9,
        compression_method: str = "zlib"
    ):
        """
        Initialize the HTML content store.
        
        Args:
            storage_path: Base path for storing HTML content.
            spark: SparkSession to use.
            compression_level: Compression level to use (0-9, higher is more compression).
            compression_method: Compression method to use (zlib or zstd).
        """
        self.storage_path = storage_path
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.compression_level = compression_level
        self.compression_method = compression_method
        
        # Create directories if they don't exist
        self.content_path = os.path.join(storage_path, "html", "html_content")
        self.references_path = os.path.join(storage_path, "html", "html_references")
        
        os.makedirs(self.content_path, exist_ok=True)
        os.makedirs(self.references_path, exist_ok=True)
    
    def _get_content_path(self, content_hash: str) -> str:
        """
        Get the path for storing HTML content.
        
        Args:
            content_hash: Hash of the HTML content.
            
        Returns:
            Path for storing the HTML content.
        """
        # Use first 2 characters as prefix directory
        prefix = content_hash[:2]
        prefix_path = os.path.join(self.content_path, prefix)
        os.makedirs(prefix_path, exist_ok=True)
        
        return os.path.join(prefix_path, f"{content_hash}.bin")
    
    def _get_references_path(self, date: str) -> str:
        """
        Get the path for storing HTML references for a date.
        
        Args:
            date: Date string in format "YYYYMMDD".
            
        Returns:
            Path for storing HTML references.
        """
        date_path = os.path.join(self.references_path, f"date={date}")
        os.makedirs(date_path, exist_ok=True)
        
        return date_path
    
    def _compress_html(self, html: str) -> bytes:
        """
        Compress HTML content.
        
        Args:
            html: HTML content to compress.
            
        Returns:
            Compressed HTML content.
        """
        if not html:
            return b""
            
        html_bytes = html.encode('utf-8')
        
        if self.compression_method == "zlib":
            return zlib.compress(html_bytes, self.compression_level)
        elif self.compression_method == "zstd":
            try:
                import zstandard as zstd
                compressor = zstd.ZstdCompressor(level=self.compression_level)
                return compressor.compress(html_bytes)
            except ImportError:
                logger.warning("zstandard not installed, falling back to zlib")
                return zlib.compress(html_bytes, self.compression_level)
        else:
            raise ValueError(f"Unsupported compression method: {self.compression_method}")
    
    def _decompress_html(self, compressed_html: bytes, compression_method: str = None) -> str:
        """
        Decompress HTML content.
        
        Args:
            compressed_html: Compressed HTML content.
            compression_method: Compression method used (defaults to self.compression_method).
            
        Returns:
            Decompressed HTML content.
        """
        if not compressed_html:
            return ""
            
        compression_method = compression_method or self.compression_method
        
        if compression_method == "zlib":
            return zlib.decompress(compressed_html).decode('utf-8')
        elif compression_method == "zstd":
            try:
                import zstandard as zstd
                decompressor = zstd.ZstdDecompressor()
                return decompressor.decompress(compressed_html).decode('utf-8')
            except ImportError:
                logger.warning("zstandard not installed, assuming zlib compression")
                return zlib.decompress(compressed_html).decode('utf-8')
        else:
            raise ValueError(f"Unsupported compression method: {compression_method}")
    
    def store_html(self, html: str, domain: str, url: str, date: str) -> Tuple[str, HTMLContentMetadata, HTMLReference]:
        """
        Store HTML content with deduplication.
        
        Args:
            html: HTML content to store.
            domain: Domain name.
            url: URL of the HTML content.
            date: Date string in format "YYYYMMDD".
            
        Returns:
            Tuple of (content_hash, content_metadata, reference)
        """
        if not html:
            return "empty", None, None
            
        # Compute content hash
        content_hash = compute_content_hash(html)
        
        # Check if content already exists
        content_path = self._get_content_path(content_hash)
        content_exists = os.path.exists(content_path)
        
        # Compress and store content if it doesn't exist
        if not content_exists:
            compressed_html = self._compress_html(html)
            
            # Create metadata
            content_metadata = HTMLContentMetadata(
                content_hash=content_hash,
                compressed_size=len(compressed_html),
                original_size=len(html.encode('utf-8')),
                first_seen_date=date,
                last_seen_date=date,
                reference_count=1,
                compression_method=self.compression_method
            )
            
            # Write content and metadata
            with open(content_path, 'wb') as f:
                f.write(compressed_html)
                
            metadata_path = f"{content_path}.meta"
            with open(metadata_path, 'w') as f:
                json.dump(content_metadata.to_dict(), f)
        else:
            # Update existing metadata
            metadata_path = f"{content_path}.meta"
            with open(metadata_path, 'r') as f:
                metadata_dict = json.load(f)
                
            content_metadata = HTMLContentMetadata.from_dict(metadata_dict)
            
            # Update metadata
            content_metadata.reference_count += 1
            if date < content_metadata.first_seen_date:
                content_metadata.first_seen_date = date
            if date > content_metadata.last_seen_date:
                content_metadata.last_seen_date = date
                
            # Write updated metadata
            with open(metadata_path, 'w') as f:
                json.dump(content_metadata.to_dict(), f)
        
        # Create reference
        reference = HTMLReference(
            domain=domain,
            url=url,
            content_hash=content_hash,
            date=date,
            version=1,
            metadata={}
        )
        
        # Store reference
        references_path = self._get_references_path(date)
        reference_path = os.path.join(references_path, f"{content_hash}_{domain.replace('.', '_')}.json")
        
        with open(reference_path, 'w') as f:
            json.dump(reference.to_dict(), f)
        
        return content_hash, content_metadata, reference
    
    def get_html(self, content_hash: str) -> Optional[str]:
        """
        Get HTML content by hash.
        
        Args:
            content_hash: Hash of the HTML content.
            
        Returns:
            HTML content or None if not found.
        """
        if content_hash == "empty":
            return ""
            
        content_path = self._get_content_path(content_hash)
        
        if not os.path.exists(content_path):
            return None
            
        # Read metadata to get compression method
        metadata_path = f"{content_path}.meta"
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                metadata_dict = json.load(f)
                compression_method = metadata_dict.get("compression_method", self.compression_method)
        else:
            compression_method = self.compression_method
        
        # Read and decompress content
        with open(content_path, 'rb') as f:
            compressed_html = f.read()
            
        return self._decompress_html(compressed_html, compression_method)
    
    def get_content_metadata(self, content_hash: str) -> Optional[HTMLContentMetadata]:
        """
        Get metadata for HTML content.
        
        Args:
            content_hash: Hash of the HTML content.
            
        Returns:
            HTMLContentMetadata or None if not found.
        """
        if content_hash == "empty":
            return None
            
        metadata_path = f"{self._get_content_path(content_hash)}.meta"
        
        if not os.path.exists(metadata_path):
            return None
            
        with open(metadata_path, 'r') as f:
            metadata_dict = json.load(f)
            
        return HTMLContentMetadata.from_dict(metadata_dict)
    
    def get_references(self, content_hash: str = None, domain: str = None, 
                       start_date: str = None, end_date: str = None) -> List[HTMLReference]:
        """
        Get references to HTML content.
        
        Args:
            content_hash: Optional hash of the HTML content.
            domain: Optional domain to filter by.
            start_date: Optional start date string in format "YYYYMMDD".
            end_date: Optional end date string in format "YYYYMMDD".
            
        Returns:
            List of HTMLReference objects.
        """
        references = []
        
        # If dates are specified, only search those directories
        if start_date and end_date:
            dates = []
            start = datetime.datetime.strptime(start_date, "%Y%m%d")
            end = datetime.datetime.strptime(end_date, "%Y%m%d")
            
            current = start
            while current <= end:
                dates.append(current.strftime("%Y%m%d"))
                current += datetime.timedelta(days=1)
                
            for date in dates:
                date_path = self._get_references_path(date)
                
                if not os.path.exists(date_path):
                    continue
                    
                for filename in os.listdir(date_path):
                    if not filename.endswith(".json"):
                        continue
                        
                    # Check if it matches content_hash filter
                    if content_hash and not filename.startswith(f"{content_hash}_"):
                        continue
                        
                    # Read reference
                    with open(os.path.join(date_path, filename), 'r') as f:
                        reference_dict = json.load(f)
                        
                    reference = HTMLReference.from_dict(reference_dict)
                    
                    # Check domain filter
                    if domain and reference.domain != domain:
                        continue
                        
                    references.append(reference)
        else:
            # Search all references
            for item in os.listdir(self.references_path):
                if not item.startswith("date="):
                    continue
                    
                date_path = os.path.join(self.references_path, item)
                
                for filename in os.listdir(date_path):
                    if not filename.endswith(".json"):
                        continue
                        
                    # Check if it matches content_hash filter
                    if content_hash and not filename.startswith(f"{content_hash}_"):
                        continue
                        
                    # Read reference
                    with open(os.path.join(date_path, filename), 'r') as f:
                        reference_dict = json.load(f)
                        
                    reference = HTMLReference.from_dict(reference_dict)
                    
                    # Check domain filter
                    if domain and reference.domain != domain:
                        continue
                        
                    references.append(reference)
        
        return references
    
    def store_html_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Store HTML content from a DataFrame with deduplication.
        
        The DataFrame must have 'html', 'domain', 'url', and 'date' columns.
        
        Args:
            df: DataFrame with HTML content.
            
        Returns:
            DataFrame with added 'content_hash' column.
        """
        # Validate DataFrame
        required_columns = ['html', 'domain', 'url', 'date']
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"DataFrame must have a '{column}' column")
                
        # Create schema for UDF
        schema = T.StructType([
            T.StructField("content_hash", T.StringType(), False),
            T.StructField("compressed_size", T.IntegerType(), True),
            T.StructField("original_size", T.IntegerType(), True)
        ])
        
        # Define UDF to store HTML
        def store_html_udf(html, domain, url, date):
            if not html:
                return ("empty", None, None)
                
            content_hash, content_metadata, _ = self.store_html(html, domain, url, date)
            
            if content_metadata:
                return (content_hash, content_metadata.compressed_size, content_metadata.original_size)
            else:
                return (content_hash, None, None)
                
        store_html_pandas_udf = F.pandas_udf(store_html_udf, schema)
        
        # Apply UDF
        result_df = df.withColumn(
            "html_content_info",
            store_html_pandas_udf(F.col("html"), F.col("domain"), F.col("url"), F.col("date"))
        )
        
        # Extract fields from struct
        result_df = result_df.withColumn("content_hash", F.col("html_content_info.content_hash"))
        result_df = result_df.withColumn("compressed_size", F.col("html_content_info.compressed_size"))
        result_df = result_df.withColumn("original_size", F.col("html_content_info.original_size"))
        
        # Drop intermediate column
        result_df = result_df.drop("html_content_info")
        
        return result_df
    
    def restore_html_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Restore HTML content in a DataFrame.
        
        The DataFrame must have a 'content_hash' column.
        
        Args:
            df: DataFrame with content hashes.
            
        Returns:
            DataFrame with added 'html' column.
        """
        # Validate DataFrame
        if "content_hash" not in df.columns:
            raise ValueError("DataFrame must have a 'content_hash' column")
            
        # Define UDF to restore HTML
        def restore_html_udf(content_hash):
            return self.get_html(content_hash)
            
        restore_html_pandas_udf = F.pandas_udf(restore_html_udf, T.StringType())
        
        # Apply UDF
        result_df = df.withColumn("html", restore_html_pandas_udf(F.col("content_hash")))
        
        return result_df 