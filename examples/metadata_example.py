#!/usr/bin/env python3
"""
CC-Store Metadata Backend Example

This example demonstrates how to use different metadata backends with CC-Store:
1. File System (default)
2. Redis
3. RocksDB
"""

import os
import sys
import json
import time
import random
from datetime import datetime, timedelta
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

from cc_store.core.cc_store import CCStore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_sample_data(spark, domain, num_records=100):
    """
    Generate sample data for testing.
    
    Args:
        spark: SparkSession
        domain: Domain name
        num_records: Number of records to generate
        
    Returns:
        DataFrame with sample data
    """
    # Create schema
    schema = StructType([
        StructField("url", StringType(), False),
        StructField("content", StringType(), True),
        StructField("title", StringType(), True),
        StructField("timestamp", IntegerType(), True)
    ])
    
    # Generate data
    data = []
    for i in range(num_records):
        url = f"https://{domain}/page{i}.html"
        content = f"This is sample content for {url}"
        title = f"Page {i} - {domain}"
        timestamp = int(time.time()) - random.randint(0, 86400 * 30)  # Random time in last 30 days
        data.append((url, content, title, timestamp))
    
    # Create DataFrame
    return spark.createDataFrame(data, schema=schema)


def test_file_system_backend(storage_path):
    """
    Test the file system metadata backend.
    
    Args:
        storage_path: Path for storage
    """
    logger.info("Testing File System metadata backend...")
    
    # Initialize CCStore with file system backend (default)
    cc_store = CCStore(
        storage_path=storage_path,
        metadata_backend="file"
    )
    
    # Generate and write sample data
    domain = "example.com"
    date = datetime.now().strftime("%Y%m%d")
    
    df = generate_sample_data(cc_store.spark, domain, num_records=500)
    cc_store.write_domain(domain, df, date)
    
    # Get domain statistics
    stats = cc_store.get_domain_statistics(domain)
    logger.info(f"Domain statistics: {json.dumps(stats, indent=2)}")
    
    # List domains
    domains = cc_store.list_domains()
    logger.info(f"Domains in store: {domains}")
    
    # Read data back
    result_df = cc_store.read_domain(domain, limit=5)
    logger.info(f"Sample data (5 records):")
    result_df.show(5, truncate=False)
    
    # Clean up
    cc_store.close()
    logger.info("File System backend test completed")


def test_redis_backend(storage_path):
    """
    Test the Redis metadata backend.
    
    Args:
        storage_path: Path for storage
    """
    try:
        import redis
    except ImportError:
        logger.warning("Redis Python client not installed. Install with: pip install redis")
        logger.warning("Skipping Redis backend test")
        return
    
    logger.info("Testing Redis metadata backend...")
    
    # Redis connection parameters
    redis_config = {
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "password": None
    }
    
    try:
        # Test Redis connection
        r = redis.Redis(**redis_config)
        r.ping()
        r.close()
    except Exception as e:
        logger.warning(f"Could not connect to Redis: {str(e)}")
        logger.warning("Skipping Redis backend test")
        return
    
    # Initialize CCStore with Redis backend
    cc_store = CCStore(
        storage_path=storage_path,
        metadata_backend="redis",
        metadata_config=redis_config
    )
    
    # Generate and write sample data for two domains
    domains = ["redis-example1.com", "redis-example2.com"]
    date = datetime.now().strftime("%Y%m%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    
    for domain in domains:
        # Write data for today
        df = generate_sample_data(cc_store.spark, domain, num_records=200)
        cc_store.write_domain(domain, df, date)
        
        # Write data for yesterday
        df = generate_sample_data(cc_store.spark, domain, num_records=100)
        cc_store.write_domain(domain, df, yesterday)
    
    # Get domain statistics
    for domain in domains:
        stats = cc_store.get_domain_statistics(domain)
        logger.info(f"Domain statistics for {domain}: {json.dumps(stats, indent=2)}")
    
    # Try batch get statistics
    batch_stats = cc_store.batch_get_domain_statistics(domains)
    logger.info(f"Batch statistics: {json.dumps(batch_stats, indent=2)}")
    
    # List domains with prefix filter
    filtered_domains = cc_store.list_domains(prefix="redis-")
    logger.info(f"Domains with prefix 'redis-': {filtered_domains}")
    
    # Read data from one domain
    result_df = cc_store.read_domain(domains[0], limit=5)
    logger.info(f"Sample data from {domains[0]} (5 records):")
    result_df.show(5, truncate=False)
    
    # Delete one domain
    cc_store.delete_domain(domains[1])
    logger.info(f"Deleted domain {domains[1]}")
    
    # Verify deletion
    remaining_domains = cc_store.list_domains(prefix="redis-")
    logger.info(f"Remaining domains: {remaining_domains}")
    
    # Clean up
    cc_store.close()
    logger.info("Redis backend test completed")


def test_rocksdb_backend(storage_path):
    """
    Test the RocksDB metadata backend.
    
    Args:
        storage_path: Path for storage
    """
    try:
        import rocksdb
    except ImportError:
        logger.warning("RocksDB Python client not installed. Install with: pip install python-rocksdb")
        logger.warning("Skipping RocksDB backend test")
        return
    
    logger.info("Testing RocksDB metadata backend...")
    
    # RocksDB connection parameters
    rocksdb_config = {
        "db_path": os.path.join(storage_path, "metadata_rocksdb"),
        "max_open_files": 300,
        "write_buffer_size": 67108864,  # 64MB
        "max_write_buffer_number": 3,
        "target_file_size_base": 67108864  # 64MB
    }
    
    # Initialize CCStore with RocksDB backend
    cc_store = CCStore(
        storage_path=storage_path,
        metadata_backend="rocksdb",
        metadata_config=rocksdb_config
    )
    
    # Generate and write sample data for multiple domains
    domains = [f"rocksdb-example{i}.com" for i in range(1, 6)]
    date = datetime.now().strftime("%Y%m%d")
    
    for i, domain in enumerate(domains):
        # Write different amounts of data for each domain
        num_records = 100 * (i + 1)
        df = generate_sample_data(cc_store.spark, domain, num_records=num_records)
        cc_store.write_domain(domain, df, date)
    
    # Get domain statistics
    for domain in domains[:2]:  # Just show first two for brevity
        stats = cc_store.get_domain_statistics(domain)
        logger.info(f"Domain statistics for {domain}: {json.dumps(stats, indent=2)}")
    
    # List domains with pagination
    page1 = cc_store.list_domains(prefix="rocksdb-", limit=2, offset=0)
    page2 = cc_store.list_domains(prefix="rocksdb-", limit=2, offset=2)
    logger.info(f"Domains page 1: {page1}")
    logger.info(f"Domains page 2: {page2}")
    
    # Batch get statistics
    batch_stats = cc_store.batch_get_domain_statistics(domains)
    total_records = sum(stats['total_records'] for stats in batch_stats.values())
    logger.info(f"Total records across all domains: {total_records}")
    
    # Delete some domains
    for domain in domains[3:]:
        cc_store.delete_domain(domain)
    logger.info(f"Deleted domains: {domains[3:]}")
    
    # List remaining domains
    remaining = cc_store.list_domains(prefix="rocksdb-")
    logger.info(f"Remaining domains: {remaining}")
    
    # Clean up
    cc_store.close()
    logger.info("RocksDB backend test completed")


def main():
    """
    Main function to run all metadata backend tests.
    """
    # Use temporary directory for testing
    import tempfile
    storage_path = tempfile.mkdtemp(prefix="cc_store_metadata_test_")
    logger.info(f"Using temporary storage path: {storage_path}")
    
    try:
        # Run tests for each backend
        test_file_system_backend(storage_path)
        print("\n" + "-"*80 + "\n")
        
        test_redis_backend(storage_path)
        print("\n" + "-"*80 + "\n")
        
        test_rocksdb_backend(storage_path)
    
    finally:
        # Clean up
        logger.info(f"Cleaning up temporary storage: {storage_path}")
        import shutil
        shutil.rmtree(storage_path, ignore_errors=True)


if __name__ == "__main__":
    main() 