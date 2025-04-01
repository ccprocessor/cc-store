"""
Metadata Manager abstraction for CC-Store.

This module provides an abstract base class for metadata management,
with support for both file-based and key-value store backends.
"""

import abc
from typing import Dict, List, Optional, Any, Tuple, Union
import datetime
import os

from pyspark.sql import SparkSession


class MetadataManager(abc.ABC):
    """
    Abstract base class for metadata management in CC-Store.
    
    This class defines the interface for managing metadata about domains,
    files, and other components of the storage system. Implementations can
    use file-based storage, key-value stores, or databases.
    """
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize the metadata manager.
        
        Args:
            spark: Optional SparkSession instance
        """
        self.spark = spark
    
    @abc.abstractmethod
    def get_domain_metadata(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary containing domain metadata or None if not found
        """
        pass
    
    @abc.abstractmethod
    def update_domain_metadata(self, domain: str, metadata: Dict[str, Any]) -> bool:
        """
        Update metadata for a domain.
        
        Args:
            domain: Domain name
            metadata: Updated metadata dictionary
            
        Returns:
            True if update was successful, False otherwise
        """
        pass
    
    @abc.abstractmethod
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
        pass
    
    @abc.abstractmethod
    def get_dates_for_domain(self, domain: str) -> List[str]:
        """
        Get available dates for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            List of date strings in format "YYYYMMDD"
        """
        pass
    
    @abc.abstractmethod
    def get_domain_statistics(self, domain: str) -> Dict[str, Any]:
        """
        Get statistics for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary with domain statistics
        """
        pass
    
    @abc.abstractmethod
    def delete_domain_metadata(self, domain: str) -> bool:
        """
        Delete metadata for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            True if deletion was successful, False otherwise
        """
        pass
    
    def batch_get_domain_metadata(self, domains: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get metadata for multiple domains.
        
        Default implementation calls get_domain_metadata for each domain,
        but implementations should override this with a more efficient batch operation.
        
        Args:
            domains: List of domain names
            
        Returns:
            Dictionary mapping domain names to metadata dictionaries
        """
        result = {}
        for domain in domains:
            metadata = self.get_domain_metadata(domain)
            if metadata:
                result[domain] = metadata
        return result
    
    def create_domain_metadata(self, domain: str, initial_metadata: Dict[str, Any]) -> bool:
        """
        Create metadata for a new domain.
        
        Default implementation calls update_domain_metadata.
        
        Args:
            domain: Domain name
            initial_metadata: Initial metadata dictionary
            
        Returns:
            True if creation was successful, False otherwise
        """
        return self.update_domain_metadata(domain, initial_metadata)


class FileSystemMetadataManager(MetadataManager):
    """
    File-based implementation of MetadataManager.
    
    This implementation stores metadata in JSON files on the file system.
    """
    
    def __init__(
        self, 
        storage_path: str, 
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize a file-based metadata manager.
        
        Args:
            storage_path: Base path for storing metadata
            spark: Optional SparkSession instance
        """
        super().__init__(spark)
        self.base_path = storage_path
        
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
            domain: Domain name
            
        Returns:
            Domain bucket name (e.g., "a-f", "g-m", "n-s", "t-z", "other")
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
    
    def _get_metadata_path(self, domain: str) -> str:
        """
        Get the path for domain metadata.
        
        Args:
            domain: Domain name
            
        Returns:
            Path to the domain metadata file
        """
        bucket = self._get_domain_bucket(domain)
        return os.path.join(
            self.metadata_path,
            f"domain_bucket={bucket}",
            f"domain={domain}",
            "metadata.json"
        )
    
    def _get_domain_dates_path(self, domain: str) -> str:
        """
        Get the path for domain dates metadata.
        
        Args:
            domain: Domain name
            
        Returns:
            Path to the domain dates file
        """
        bucket = self._get_domain_bucket(domain)
        return os.path.join(
            self.metadata_path,
            f"domain_bucket={bucket}",
            f"domain={domain}",
            "dates.json"
        )
    
    def _get_domain_path(self, domain: str) -> str:
        """
        Get the path for a domain's data directory.
        
        Args:
            domain: Domain name
            
        Returns:
            Path to the domain directory
        """
        bucket = self._get_domain_bucket(domain)
        return os.path.join(
            os.path.join(self.base_path, "data"),
            f"domain_bucket={bucket}",
            f"domain={domain}"
        )
    
    def get_domain_metadata(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary containing domain metadata or None if not found
        """
        metadata_path = self._get_metadata_path(domain)
        
        if not os.path.exists(metadata_path):
            return None
        
        try:
            import json
            with open(metadata_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading metadata for domain {domain}: {str(e)}")
            return None
    
    def update_domain_metadata(self, domain: str, metadata: Dict[str, Any]) -> bool:
        """
        Update metadata for a domain.
        
        Args:
            domain: Domain name
            metadata: Updated metadata dictionary
            
        Returns:
            True if update was successful, False otherwise
        """
        metadata_path = self._get_metadata_path(domain)
        
        try:
            import json
            # Create directory structure if it doesn't exist
            os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
            
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
            return True
        except Exception as e:
            print(f"Error updating metadata for domain {domain}: {str(e)}")
            return False
    
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
        domains = []
        
        # List domain bucket directories
        data_path = os.path.join(self.base_path, "data")
        if not os.path.exists(data_path):
            return []
        
        # Walk through domain bucket directories
        for bucket_dir in os.listdir(data_path):
            if not bucket_dir.startswith("domain_bucket="):
                continue
            
            bucket_path = os.path.join(data_path, bucket_dir)
            if not os.path.isdir(bucket_path):
                continue
            
            # Find domain directories in this bucket
            for domain_dir in os.listdir(bucket_path):
                if not domain_dir.startswith("domain="):
                    continue
                
                # Extract domain name
                domain = domain_dir[len("domain="):]
                
                # Apply prefix filter if provided
                if prefix and not domain.startswith(prefix):
                    continue
                
                domains.append(domain)
        
        # Sort domains for consistent ordering
        domains.sort()
        
        # Apply pagination
        return domains[offset:offset+limit]
    
    def get_dates_for_domain(self, domain: str) -> List[str]:
        """
        Get available dates for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            List of date strings in format "YYYYMMDD"
        """
        # First, try to get from the dedicated dates file
        dates_path = self._get_domain_dates_path(domain)
        if os.path.exists(dates_path):
            try:
                import json
                with open(dates_path, "r") as f:
                    dates = json.load(f)
                return sorted(dates)
            except Exception:
                pass  # Fall back to scanning directory
        
        # Fall back to scanning the data directory
        domain_path = self._get_domain_path(domain)
        if not os.path.exists(domain_path):
            return []
        
        dates = []
        for item in os.listdir(domain_path):
            if item.startswith("date="):
                date = item[len("date="):]
                dates.append(date)
        
        return sorted(dates)
    
    def update_dates_for_domain(self, domain: str, dates: List[str]) -> bool:
        """
        Update available dates for a domain.
        
        Args:
            domain: Domain name
            dates: List of date strings
            
        Returns:
            True if update was successful, False otherwise
        """
        dates_path = self._get_domain_dates_path(domain)
        
        try:
            import json
            # Create directory structure if it doesn't exist
            os.makedirs(os.path.dirname(dates_path), exist_ok=True)
            
            with open(dates_path, "w") as f:
                json.dump(dates, f)
            return True
        except Exception as e:
            print(f"Error updating dates for domain {domain}: {str(e)}")
            return False
    
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
            return {}
        
        # Extract statistics from metadata
        stats = {
            "total_records": metadata.get("total_records", 0),
            "total_files": metadata.get("total_files", 0),
            "total_size_bytes": metadata.get("total_size_bytes", 0),
            "date_range": {
                "min_date": metadata.get("min_date"),
                "max_date": metadata.get("max_date"),
                "date_count": metadata.get("date_count", 0)
            },
            "stats": metadata.get("stats", {})
        }
        
        return stats
    
    def delete_domain_metadata(self, domain: str) -> bool:
        """
        Delete metadata for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            True if deletion was successful, False otherwise
        """
        metadata_path = self._get_metadata_path(domain)
        dates_path = self._get_domain_dates_path(domain)
        success = True
        
        # Delete metadata file if it exists
        if os.path.exists(metadata_path):
            try:
                os.remove(metadata_path)
            except Exception as e:
                print(f"Error deleting metadata file for domain {domain}: {str(e)}")
                success = False
        
        # Delete dates file if it exists
        if os.path.exists(dates_path):
            try:
                os.remove(dates_path)
            except Exception as e:
                print(f"Error deleting dates file for domain {domain}: {str(e)}")
                success = False
        
        # Try to remove parent directory if empty
        try:
            parent_dir = os.path.dirname(metadata_path)
            if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                os.rmdir(parent_dir)
                
                # Try to remove bucket directory if empty
                bucket_dir = os.path.dirname(parent_dir)
                if os.path.exists(bucket_dir) and not os.listdir(bucket_dir):
                    os.rmdir(bucket_dir)
        except Exception:
            # Ignore errors when cleaning up empty directories
            pass
        
        return success
    
    def close(self):
        """Close the metadata manager."""
        # File-based implementation doesn't need explicit closing
        pass


class KVStoreMetadataManager(MetadataManager):
    """
    Key-Value store implementation of MetadataManager.
    
    This implementation stores metadata in a key-value store.
    Supports various KV stores through a common interface.
    """
    
    def __init__(
        self, 
        store_type: str,
        connection_params: Dict[str, Any],
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize a KV store metadata manager.
        
        Args:
            store_type: Type of KV store ("redis", "leveldb", "rocksdb", etc.)
            connection_params: Parameters for connecting to the KV store
            spark: Optional SparkSession instance
        """
        super().__init__(spark)
        self.store_type = store_type.lower()
        self.connection_params = connection_params
        self.client = None
        
        # Initialize connection based on store type
        if self.store_type == "redis":
            self._init_redis()
        elif self.store_type == "rocksdb":
            self._init_rocksdb()
        else:
            raise ValueError(f"Unsupported KV store type: {store_type}")
        
        # Key prefixes for different types of metadata
        self.domain_prefix = "domain:"
        self.dates_prefix = "dates:"
        self.domain_list_key = "domains:list"
    
    def _init_redis(self):
        """Initialize Redis client."""
        try:
            import redis
            
            # Extract connection parameters
            host = self.connection_params.get("host", "localhost")
            port = self.connection_params.get("port", 6379)
            db = self.connection_params.get("db", 0)
            password = self.connection_params.get("password", None)
            
            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True  # Automatically decode response bytes to str
            )
            # Test connection
            self.client.ping()
        except ImportError:
            raise ImportError("Redis Python client not installed. Install with: pip install redis")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redis: {str(e)}")
    
    def _init_rocksdb(self):
        """Initialize RocksDB client."""
        try:
            import rocksdb
            
            # Extract connection parameters
            db_path = self.connection_params.get("db_path")
            if not db_path:
                raise ValueError("db_path is required for RocksDB")
            
            # Create directory if it doesn't exist
            import os
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Configure RocksDB options
            opts = rocksdb.Options()
            opts.create_if_missing = True
            opts.max_open_files = self.connection_params.get("max_open_files", 300)
            opts.write_buffer_size = self.connection_params.get("write_buffer_size", 67108864)
            opts.max_write_buffer_number = self.connection_params.get("max_write_buffer_number", 3)
            opts.target_file_size_base = self.connection_params.get("target_file_size_base", 67108864)
            
            self.client = rocksdb.DB(db_path, opts)
        except ImportError:
            raise ImportError("RocksDB Python client not installed. Install with: pip install python-rocksdb")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize RocksDB: {str(e)}")
    
    def _encode_value(self, value: Any) -> bytes:
        """Encode a value to bytes for storage."""
        import json
        if value is None:
            return b''
        return json.dumps(value).encode('utf-8')
    
    def _decode_value(self, value: bytes) -> Any:
        """Decode a value from bytes."""
        import json
        if not value or value == b'':
            return None
        try:
            # Handle both bytes and strings
            if isinstance(value, bytes):
                return json.loads(value.decode('utf-8'))
            return json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
    
    def get_domain_metadata(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary containing domain metadata or None if not found
        """
        key = f"{self.domain_prefix}{domain}"
        
        if self.store_type == "redis":
            value = self.client.get(key)
            return self._decode_value(value)
        elif self.store_type == "rocksdb":
            value = self.client.get(key.encode('utf-8'))
            return self._decode_value(value)
        
        return None
    
    def update_domain_metadata(self, domain: str, metadata: Dict[str, Any]) -> bool:
        """
        Update metadata for a domain.
        
        Args:
            domain: Domain name
            metadata: Updated metadata dictionary
            
        Returns:
            True if update was successful, False otherwise
        """
        key = f"{self.domain_prefix}{domain}"
        encoded_value = self._encode_value(metadata)
        
        try:
            if self.store_type == "redis":
                self.client.set(key, encoded_value)
                # Add to domain list if not exists
                self.client.sadd(self.domain_list_key, domain)
            elif self.store_type == "rocksdb":
                self.client.put(key.encode('utf-8'), encoded_value)
                # For RocksDB we need to manually manage the domain list
                domains = self.list_domains(limit=10000000)  # Get all domains
                if domain not in domains:
                    domains.append(domain)
                    self.client.put(
                        self.domain_list_key.encode('utf-8'), 
                        self._encode_value(domains)
                    )
            return True
        except Exception as e:
            print(f"Error updating domain metadata: {str(e)}")
            return False
    
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
            if self.store_type == "redis":
                # Get all domains from set
                all_domains = list(self.client.smembers(self.domain_list_key))
                
                # Apply prefix filter if provided
                if prefix:
                    filtered_domains = [d for d in all_domains if d.startswith(prefix)]
                else:
                    filtered_domains = all_domains
                
                # Sort for consistent ordering
                filtered_domains.sort()
                
                # Apply pagination
                return filtered_domains[offset:offset+limit]
            
            elif self.store_type == "rocksdb":
                # Get the domain list
                domains_bytes = self.client.get(self.domain_list_key.encode('utf-8'))
                if not domains_bytes:
                    return []
                
                all_domains = self._decode_value(domains_bytes) or []
                
                # Apply prefix filter if provided
                if prefix:
                    filtered_domains = [d for d in all_domains if d.startswith(prefix)]
                else:
                    filtered_domains = all_domains
                
                # Sort for consistent ordering
                filtered_domains.sort()
                
                # Apply pagination
                return filtered_domains[offset:offset+limit]
        
        except Exception as e:
            print(f"Error listing domains: {str(e)}")
            return []
    
    def get_dates_for_domain(self, domain: str) -> List[str]:
        """
        Get available dates for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            List of date strings in format "YYYYMMDD"
        """
        key = f"{self.dates_prefix}{domain}"
        
        try:
            if self.store_type == "redis":
                dates = self.client.smembers(key)
                return sorted(list(dates)) if dates else []
            
            elif self.store_type == "rocksdb":
                value = self.client.get(key.encode('utf-8'))
                dates = self._decode_value(value) or []
                return sorted(dates)
        
        except Exception as e:
            print(f"Error getting dates for domain: {str(e)}")
            return []
    
    def update_dates_for_domain(self, domain: str, dates: List[str]) -> bool:
        """
        Update available dates for a domain.
        
        Args:
            domain: Domain name
            dates: List of date strings
            
        Returns:
            True if update was successful, False otherwise
        """
        key = f"{self.dates_prefix}{domain}"
        
        try:
            if self.store_type == "redis":
                # Clear existing dates
                self.client.delete(key)
                # Add new dates
                if dates:
                    self.client.sadd(key, *dates)
            
            elif self.store_type == "rocksdb":
                # Store as JSON array
                self.client.put(key.encode('utf-8'), self._encode_value(dates))
            
            return True
        
        except Exception as e:
            print(f"Error updating dates for domain: {str(e)}")
            return False
    
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
            return {}
        
        # Extract statistics from metadata
        stats = {
            "total_records": metadata.get("total_records", 0),
            "total_files": metadata.get("total_files", 0),
            "total_size_bytes": metadata.get("total_size_bytes", 0),
            "date_range": {
                "min_date": metadata.get("min_date"),
                "max_date": metadata.get("max_date"),
                "date_count": metadata.get("date_count", 0)
            },
            "stats": metadata.get("stats", {})
        }
        
        return stats
    
    def delete_domain_metadata(self, domain: str) -> bool:
        """
        Delete metadata for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            True if deletion was successful, False otherwise
        """
        domain_key = f"{self.domain_prefix}{domain}"
        dates_key = f"{self.dates_prefix}{domain}"
        
        try:
            if self.store_type == "redis":
                # Remove from domain list
                self.client.srem(self.domain_list_key, domain)
                # Delete metadata and dates
                self.client.delete(domain_key, dates_key)
            
            elif self.store_type == "rocksdb":
                # Remove domain from list
                domains_bytes = self.client.get(self.domain_list_key.encode('utf-8'))
                if domains_bytes:
                    domains = self._decode_value(domains_bytes) or []
                    if domain in domains:
                        domains.remove(domain)
                        self.client.put(
                            self.domain_list_key.encode('utf-8'),
                            self._encode_value(domains)
                        )
                
                # Delete metadata and dates
                self.client.delete(domain_key.encode('utf-8'))
                self.client.delete(dates_key.encode('utf-8'))
            
            return True
        
        except Exception as e:
            print(f"Error deleting domain metadata: {str(e)}")
            return False
    
    def batch_get_domain_metadata(self, domains: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get metadata for multiple domains.
        
        Args:
            domains: List of domain names
            
        Returns:
            Dictionary mapping domain names to metadata dictionaries
        """
        result = {}
        
        try:
            if self.store_type == "redis":
                # Create a pipeline for batch operations
                pipe = self.client.pipeline()
                
                # Queue all get operations
                for domain in domains:
                    key = f"{self.domain_prefix}{domain}"
                    pipe.get(key)
                
                # Execute pipeline and process results
                values = pipe.execute()
                
                for i, domain in enumerate(domains):
                    if values[i]:
                        result[domain] = self._decode_value(values[i])
            
            elif self.store_type == "rocksdb":
                # RocksDB doesn't have native batching, so we'll just iterate
                for domain in domains:
                    key = f"{self.domain_prefix}{domain}"
                    value = self.client.get(key.encode('utf-8'))
                    if value:
                        result[domain] = self._decode_value(value)
        
        except Exception as e:
            print(f"Error in batch get domain metadata: {str(e)}")
        
        return result
    
    def close(self):
        """Close the connection to the KV store."""
        if self.client:
            if self.store_type == "redis":
                self.client.close()
            # RocksDB doesn't need explicit closing 