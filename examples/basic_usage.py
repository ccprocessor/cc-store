#!/usr/bin/env python
"""
Basic usage example for CC-Store.

This script demonstrates how to:
1. Initialize a CCStore
2. Create sample data
3. Write data to storage
4. Read data back from storage
5. Perform basic queries
"""

import os
import datetime
import tempfile
import shutil
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from cc_store.core.cc_store import CCStore


def create_sample_data(spark):
    """Create sample data for demonstration."""
    # Create sample data
    rows = [
        Row(
            raw_warc_path="s3://commoncrawl/path1.warc.gz",
            track_id="20230101123456-12345",
            domain="example.com",
            url="https://example.com/page1",
            status=200,
            response_header={"Content-Type": "text/html"},
            date=20230101,
            content_length=12345,
            content_charset="utf-8",
            html="<html><body><h1>Example 1</h1><p>This is a sample page.</p></body></html>",
            remark={"crawl_id": "CC-MAIN-2023-01"}
        ),
        Row(
            raw_warc_path="s3://commoncrawl/path2.warc.gz",
            track_id="20230101123457-12346",
            domain="example.com",
            url="https://example.com/page2",
            status=200,
            response_header={"Content-Type": "text/html"},
            date=20230101,
            content_length=23456,
            content_charset="utf-8",
            html="<html><body><h1>Example 2</h1><p>This is another sample page.</p></body></html>",
            remark={"crawl_id": "CC-MAIN-2023-01"}
        ),
        Row(
            raw_warc_path="s3://commoncrawl/path3.warc.gz",
            track_id="20230201123458-12347",
            domain="example.com",
            url="https://example.com/page3",
            status=200,
            response_header={"Content-Type": "text/html"},
            date=20230201,
            content_length=34567,
            content_charset="utf-8",
            html="<html><body><h1>Example 3</h1><p>This is a newer page.</p></body></html>",
            remark={"crawl_id": "CC-MAIN-2023-02"}
        ),
        Row(
            raw_warc_path="s3://commoncrawl/path4.warc.gz",
            track_id="20230101123459-12348",
            domain="example.org",
            url="https://example.org/page1",
            status=404,
            response_header={"Content-Type": "text/html"},
            date=20230101,
            content_length=1234,
            content_charset="utf-8",
            html="<html><body><h1>404 Not Found</h1></body></html>",
            remark={"crawl_id": "CC-MAIN-2023-01"}
        )
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(rows)
    return df


def main():
    """Main function demonstrating CC-Store usage."""
    # Create a temporary directory for storage
    storage_path = tempfile.mkdtemp()
    print(f"Using temporary storage path: {storage_path}")
    
    try:
        # Initialize Spark
        spark = SparkSession.builder \
            .appName("CC-Store Basic Usage") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", os.path.join(storage_path, "spark-warehouse")) \
            .getOrCreate()
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        print("Creating sample data...")
        df = create_sample_data(spark)
        print("Sample data schema:")
        df.printSchema()
        
        # Initialize CCStore
        print("\nInitializing CCStore...")
        cc_store = CCStore(
            storage_path=storage_path,
            spark=spark
        )
        
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
            if metadata:
                print(f"\nMetadata for domain '{domain}':")
                print(f"  Total records: {metadata.total_records}")
                print(f"  Total files: {metadata.total_files}")
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
        print(f"\nSearching for 'Example 1' in HTML content:")
        search_df = cc_store.search_content(domain, "Example 1")
        search_df.show(truncate=False)
        
        # Show storage directory structure
        print("\nStorage directory structure:")
        for root, dirs, files in os.walk(storage_path):
            level = root.replace(storage_path, '').count(os.sep)
            indent = ' ' * 4 * level
            print(f"{indent}{os.path.basename(root)}/")
            sub_indent = ' ' * 4 * (level + 1)
            for f in files:
                if f.endswith(".parquet"):
                    print(f"{sub_indent}{f}")
                    
        print("\nBasic usage demo completed successfully!")
        
    finally:
        # Clean up the temporary directory
        print(f"\nCleaning up temporary storage: {storage_path}")
        shutil.rmtree(storage_path)


if __name__ == "__main__":
    main() 