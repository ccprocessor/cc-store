#!/usr/bin/env python
"""
Incremental Data Writing Example for CC-Store.

This example demonstrates how to:
1. Initialize a CCStore
2. Write an initial batch of data for multiple domains
3. Write additional incremental data in multiple batches
4. Show how statistics are updated across incremental writes
"""

import os
import sys
import time
import random
import datetime
import tempfile
import shutil
from typing import List, Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, LongType

from cc_store.core.cc_store import CCStore


def generate_batch_data(
    spark, 
    batch_id: int,
    domains: List[str], 
    start_date: str,
    end_date: str,
    urls_per_domain: int = 50
) -> Dict[str, Any]:
    """
    Generate a batch of data for testing incremental writes.
    
    Args:
        spark: SparkSession
        batch_id: Batch identifier (1, 2, 3, etc.)
        domains: List of domain names
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        urls_per_domain: Number of URLs per domain
        
    Returns:
        Dictionary with batch information and DataFrame
    """
    print(f"Generating batch {batch_id} data ({urls_per_domain} URLs per domain)...")
    
    # Define schema
    schema = StructType([
        StructField("domain", StringType(), False),
        StructField("url", StringType(), False),
        StructField("batch_id", IntegerType(), False),
        StructField("date", LongType(), False),
        StructField("timestamp", LongType(), False),
        StructField("status", IntegerType(), True),
        StructField("content_length", IntegerType(), True),
        StructField("html", StringType(), True),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])
    
    # Convert dates to list of yyyymmdd strings
    start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
    dates = []
    
    current_dt = start_dt
    while current_dt <= end_dt:
        dates.append(current_dt.strftime("%Y%m%d"))
        current_dt += datetime.timedelta(days=1)
    
    # Generate rows
    rows = []
    current_time = int(time.time())
    
    for domain in domains:
        for _ in range(urls_per_domain):
            # Randomly select a date
            date_str = random.choice(dates)
            date = int(date_str)
            
            # Generate URL - add batch ID to make them unique across batches
            path = f"/batch{batch_id}/page_{random.randint(1, 10000)}.html"
            url = f"https://{domain}{path}"
            
            # Generate content with batch ID for easy identification
            content_length = random.randint(1000, 100000)
            html = f"<html><body><h1>{domain} - Batch {batch_id}</h1><p>Page {path} created on {date_str}.</p></body></html>"
            
            # Generate metadata
            metadata = {
                "batch_id": str(batch_id),
                "crawl_time": str(current_time),
                "fetch_status": "success" if random.random() > 0.1 else "error",
                "content_type": "text/html",
                "language": random.choice(["en", "es", "fr", "de", "zh"])
            }
            
            # Create row
            rows.append((
                domain,
                url,
                batch_id,
                date,
                current_time,
                200 if random.random() > 0.1 else 404,
                content_length,
                html,
                metadata
            ))
    
    # Create DataFrame
    df = spark.createDataFrame(rows, schema=schema)
    
    # Batch information
    batch_info = {
        "id": batch_id,
        "domains": domains,
        "date_range": f"{start_date}-{end_date}",
        "record_count": len(rows),
        "df": df
    }
    
    return batch_info


def print_domain_stats(cc_store, domains):
    """Print statistics for all domains."""
    print("\nDomain Statistics:")
    print("-" * 80)
    print(f"{'Domain':<30} {'Records':<10} {'Files':<8} {'Size (MB)':<12} {'Date Range':<20}")
    print("-" * 80)
    
    # Get batch statistics for all domains
    batch_stats = cc_store.batch_get_domain_statistics(domains)
    
    # Individual domain stats
    for domain in sorted(domains):
        if domain in batch_stats:
            stats = batch_stats[domain]
            date_range = stats['date_range']
            size_mb = stats['total_size_bytes'] / (1024 * 1024)
            print(f"{domain:<30} {stats['total_records']:<10} {stats['total_files']:<8} {size_mb:<12.2f} {date_range['min_date']} - {date_range['max_date']}")
    
    # Calculate aggregate statistics
    total_records = sum(stats['total_records'] for stats in batch_stats.values())
    total_files = sum(stats['total_files'] for stats in batch_stats.values())
    total_size_mb = sum(stats['total_size_bytes'] for stats in batch_stats.values()) / (1024 * 1024)
    
    print("-" * 80)
    print(f"{'TOTAL':<30} {total_records:<10} {total_files:<8} {total_size_mb:<12.2f}")
    print("-" * 80)


def verify_record_counts(cc_store, batch_domains, expected_counts):
    """Verify that record counts match expected values."""
    actual_counts = {}
    
    for domain in batch_domains:
        df = cc_store.read_domain(domain)
        batch_counts = df.groupBy("batch_id").count().collect()
        
        domain_counts = {}
        for row in batch_counts:
            domain_counts[row["batch_id"]] = row["count"]
        
        actual_counts[domain] = domain_counts
    
    print("\nVerification of Record Counts:")
    print("-" * 80)
    print(f"{'Domain':<25} {'Batch':<8} {'Expected':<10} {'Actual':<10} {'Match':<10}")
    print("-" * 80)
    
    all_match = True
    
    for domain in sorted(batch_domains):
        for batch_id, expected in expected_counts.items():
            actual = actual_counts[domain].get(batch_id, 0)
            match = "✓" if actual == expected else "✗"
            if actual != expected:
                all_match = False
            print(f"{domain:<25} {batch_id:<8} {expected:<10} {actual:<10} {match:<10}")
    
    print("-" * 80)
    print(f"All counts match: {'Yes' if all_match else 'No'}")


def run_incremental_write_test(storage_path, spark):
    """Run the incremental write test."""
    # Test parameters
    num_domains = 3
    batch_count = 3
    
    # Generate domain names
    batch_domains = [f"test-domain-{i}.example.com" for i in range(1, num_domains + 1)]
    
    # Initialize CCStore
    print("\nInitializing CCStore...")
    cc_store = CCStore(
        storage_path=storage_path,
        spark=spark
    )
    
    # Keep track of expected record counts per batch
    urls_per_domain_per_batch = {
        1: 50,   # 50 URLs per domain in first batch
        2: 30,   # 30 URLs per domain in second batch
        3: 20    # 20 URLs per domain in third batch
    }
    
    # Generate and write multiple batches of data
    for batch_id in range(1, batch_count + 1):
        # For demonstration, use different date ranges for each batch
        if batch_id == 1:
            # First batch: 7 days ago to 5 days ago
            start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y%m%d")
            end_date = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime("%Y%m%d")
        elif batch_id == 2:
            # Second batch: 4 days ago to 2 days ago
            start_date = (datetime.datetime.now() - datetime.timedelta(days=4)).strftime("%Y%m%d")
            end_date = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y%m%d")
        else:
            # Third batch: yesterday and today
            start_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
            end_date = datetime.datetime.now().strftime("%Y%m%d")
        
        # Generate batch data
        batch_info = generate_batch_data(
            spark, 
            batch_id, 
            batch_domains, 
            start_date, 
            end_date,
            urls_per_domain=urls_per_domain_per_batch[batch_id]
        )
        
        print(f"\nBatch {batch_id} Information:")
        print(f"  Domains: {batch_info['domains']}")
        print(f"  Date Range: {batch_info['date_range']}")
        print(f"  Record Count: {batch_info['record_count']}")
        
        # Write this batch to CC-Store
        print(f"\nWriting Batch {batch_id} to CCStore...")
        start_time = time.time()
        result = cc_store.write_documents(batch_info['df'])
        end_time = time.time()
        
        print(f"Batch {batch_id} write completed in {end_time - start_time:.2f} seconds")
        print(f"  Domains written: {len(result)}")
        for domain, files in result.items():
            print(f"  - {domain}: {len(files)} files")
        
        # Print domain statistics after each batch
        print(f"\nStatistics after Batch {batch_id}:")
        print_domain_stats(cc_store, batch_domains)
        
        # Wait a short while to demonstrate this is happening incrementally
        time.sleep(1)
    
    # Verify that all batches were written correctly
    verify_record_counts(cc_store, batch_domains, urls_per_domain_per_batch)
    
    # Read sample data from one domain to show all batches
    sample_domain = batch_domains[0]
    print(f"\nSample data from {sample_domain} showing all batches:")
    
    # Read all data for this domain
    domain_df = cc_store.read_domain(sample_domain)
    
    # Show count by batch_id
    print("\nRecord counts by batch_id:")
    domain_df.groupBy("batch_id").count().orderBy("batch_id").show()
    
    # Show sample records from each batch
    for batch_id in range(1, batch_count + 1):
        print(f"\nSample records from Batch {batch_id}:")
        batch_df = domain_df.filter(F.col("batch_id") == batch_id).limit(2)
        batch_df.select("url", "batch_id", "date", "timestamp").show(truncate=False)
    
    # Clean up resources
    cc_store.close()
    print("\nIncremental write test completed successfully!")


def main():
    """Main function."""
    # Create a temporary directory for storage
    storage_path = tempfile.mkdtemp()
    print(f"Using temporary storage path: {storage_path}")
    
    try:
        # Initialize Spark
        spark = SparkSession.builder \
            .appName("CC-Store Incremental Write Example") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", os.path.join(storage_path, "spark-warehouse")) \
            .getOrCreate()
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        # Run the incremental write test
        run_incremental_write_test(storage_path, spark)
        
    finally:
        # Clean up the temporary directory
        print(f"\nCleaning up temporary storage: {storage_path}")
        # Comment out the following line to keep the data for inspection
        shutil.rmtree(storage_path)


if __name__ == "__main__":
    main() 