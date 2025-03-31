#!/usr/bin/env python
"""
Basic usage example for CC-Store.

This example demonstrates how to:
1. Initialize a CCStore instance
2. Write documents to the store
3. Read documents from the store
4. Search HTML content
5. Analyze domain metadata
"""

import os
import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from cc_store.core.cc_store import CCStore


def create_sample_data(spark):
    """Create sample data for the example."""
    # Create some sample documents
    sample_docs = [
        {
            "domain": "example.com",
            "url": "http://example.com/page1",
            "date": "20230101",
            "status": 200,
            "content_length": 1000,
            "timestamp": datetime.datetime(2023, 1, 1, 12, 0, 0),
            "html": "<html><body><h1>Example Page 1</h1><p>This is example 1</p></body></html>"
        },
        {
            "domain": "example.com",
            "url": "http://example.com/page2",
            "date": "20230101",
            "status": 200,
            "content_length": 1200,
            "timestamp": datetime.datetime(2023, 1, 1, 14, 0, 0),
            "html": "<html><body><h1>Example Page 2</h1><p>This is example 2</p></body></html>"
        },
        {
            "domain": "example.com",
            "url": "http://example.com/page1",
            "date": "20230102",
            "status": 200,
            "content_length": 1100,
            "timestamp": datetime.datetime(2023, 1, 2, 10, 0, 0),
            "html": "<html><body><h1>Example Page 1</h1><p>This is example 1 (updated)</p></body></html>"
        },
        {
            "domain": "another.com",
            "url": "http://another.com/index",
            "date": "20230101",
            "status": 200,
            "content_length": 2000,
            "timestamp": datetime.datetime(2023, 1, 1, 9, 0, 0),
            "html": "<html><body><h1>Another Domain</h1><p>This is another domain</p></body></html>"
        }
    ]
    
    # Convert to Spark DataFrame
    return spark.createDataFrame([Row(**doc) for doc in sample_docs])


def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("CCStore Example") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    # Create a temporary directory for storage
    storage_path = "/tmp/cc_store_example"
    os.makedirs(storage_path, exist_ok=True)
    
    print(f"Using storage path: {storage_path}")
    
    # Initialize CCStore
    cc_store = CCStore(
        storage_path=storage_path,
        spark=spark
    )
    
    # Create sample data
    df = create_sample_data(spark)
    print("\nSample data:")
    df.show(truncate=False)
    
    # Write documents to the store
    print("\nWriting documents to CCStore...")
    metadata = cc_store.write_documents(df)
    print(f"Wrote {len(metadata)} files to storage")
    
    # Get list of domains
    domains = cc_store.get_domains()
    print(f"\nDomains in the store: {domains}")
    
    # Get domain metadata
    for domain in domains:
        metadata = cc_store.get_domain_metadata(domain)
        print(f"\nMetadata for domain '{domain}':")
        print(f"  Total records: {metadata.total_records}")
        print(f"  Total files: {metadata.total_files}")
        print(f"  Size: {metadata.total_size_bytes} bytes")
        print(f"  Date range: {metadata.min_date} - {metadata.max_date}")
    
    # Read documents for a specific domain
    domain = "example.com"
    print(f"\nReading documents for domain '{domain}':")
    domain_df = cc_store.read_domain(domain)
    domain_df.show(truncate=False)
    
    # Read documents without HTML content (for efficiency)
    print(f"\nReading documents without HTML content for domain '{domain}':")
    no_html_df = cc_store.read_domain(domain, with_html=False)
    print(f"Columns: {no_html_df.columns}")
    no_html_df.show(truncate=False)
    
    # Read documents for a specific date range
    print(f"\nReading documents for date range '20230101' to '20230101':")
    date_df = cc_store.read_domain(domain, start_date="20230101", end_date="20230101")
    date_df.show(truncate=False)
    
    # Read documents with filters
    print(f"\nReading documents with status=200:")
    filtered_df = cc_store.read_domain(
        domain, 
        filters=[F.col("status") == 200]
    )
    filtered_df.show(truncate=False)
    
    # Read documents with limit
    print(f"\nReading limited number of documents (limit=2):")
    limited_df = cc_store.read_domain(
        domain,
        limit=2
    )
    limited_df.show(truncate=False)
    
    # Search for content in HTML
    print(f"\nSearching for 'example 1' in HTML content:")
    search_df = cc_store.search_content(domain, "example 1")
    search_df.show(truncate=False)
    
    # Clean up
    print("\nExample completed.")
    # Uncomment to clean up storage
    # import shutil
    # shutil.rmtree(storage_path)


if __name__ == "__main__":
    main() 