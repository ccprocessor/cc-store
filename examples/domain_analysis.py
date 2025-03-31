#!/usr/bin/env python
"""
Domain Analysis Examples for CC-Store.

This example demonstrates domain-centric analysis techniques:
1. Domain update frequency analysis
2. Top URLs by update frequency
3. Domain growth over time
"""

import os
import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
import matplotlib.pyplot as plt
import pandas as pd

from cc_store.core.cc_store import CCStore


def analyze_domain_update_frequency(cc_store, domain, start_date, end_date):
    """
    Analyze how frequently a domain's content is updated.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        DataFrame with update statistics by date
    """
    print(f"Analyzing update frequency for {domain} from {start_date} to {end_date}...")
    
    # Read domain data without HTML content for efficiency
    df = cc_store.read_domain(domain, start_date, end_date, with_html=False)
    
    if df.count() == 0:
        print(f"No data found for domain {domain} in the specified date range")
        return None
    
    # Group by date and count updates
    updates_by_date = df.groupBy("date").agg(
        F.countDistinct("url").alias("unique_urls"), 
        F.count("*").alias("total_updates")
    ).orderBy("date")
    
    # Calculate update ratio
    total_urls = df.select("url").distinct().count()
    print(f"Total unique URLs for {domain}: {total_urls}")
    
    updates_by_date = updates_by_date.withColumn(
        "update_ratio", F.col("unique_urls") / total_urls
    )
    
    return updates_by_date


def find_top_updated_urls(cc_store, domain, start_date, end_date, top_n=10):
    """
    Find the most frequently updated URLs for a domain.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        top_n: Number of top URLs to return
        
    Returns:
        DataFrame with top updated URLs
    """
    print(f"Finding top {top_n} updated URLs for {domain}...")
    
    # Read domain data
    df = cc_store.read_domain(domain, start_date, end_date, with_html=False)
    
    if df.count() == 0:
        print(f"No data found for domain {domain} in the specified date range")
        return None
    
    # Group by URL and count updates
    url_updates = df.groupBy("url").agg(
        F.count("*").alias("update_count"),
        F.countDistinct("date").alias("days_with_updates"),
        F.min("date").alias("first_seen"),
        F.max("date").alias("last_seen")
    )
    
    # Calculate date range covered
    date_range = df.agg(
        F.countDistinct("date").alias("total_days")
    ).collect()[0]["total_days"]
    
    # Calculate update frequency (updates per day)
    url_updates = url_updates.withColumn(
        "updates_per_day", F.col("update_count") / date_range
    )
    
    # Get top N most updated URLs
    top_urls = url_updates.orderBy(F.col("update_count").desc()).limit(top_n)
    
    return top_urls


def analyze_domain_growth(cc_store, domain, start_date, end_date):
    """
    Analyze the growth of a domain over time.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        DataFrame with cumulative growth statistics by date
    """
    print(f"Analyzing growth for {domain} from {start_date} to {end_date}...")
    
    # Read domain data
    df = cc_store.read_domain(domain, start_date, end_date, with_html=False)
    
    if df.count() == 0:
        print(f"No data found for domain {domain} in the specified date range")
        return None
    
    # Get first appearance of each URL
    first_appearances = df.groupBy("url").agg(
        F.min("date").alias("first_date")
    )
    
    # Count new URLs by date
    new_urls_by_date = first_appearances.groupBy("first_date").count() \
        .withColumnRenamed("first_date", "date") \
        .withColumnRenamed("count", "new_urls") \
        .orderBy("date")
    
    # Get all dates in range
    all_dates = df.select("date").distinct().orderBy("date")
    
    # Join with all dates (to include days with zero new URLs)
    new_urls_by_date = all_dates.join(
        new_urls_by_date, 
        all_dates["date"] == new_urls_by_date["date"], 
        "left_outer"
    ).select(
        all_dates["date"],
        F.coalesce(new_urls_by_date["new_urls"], F.lit(0)).alias("new_urls")
    )
    
    # Calculate cumulative growth
    # This requires a window function in Spark
    window_spec = F.window.orderBy("date").rowsBetween(
        F.Window.unboundedPreceding, F.Window.currentRow
    )
    
    growth_by_date = new_urls_by_date.withColumn(
        "cumulative_urls", F.sum("new_urls").over(window_spec)
    )
    
    return growth_by_date


def plot_results(updates_df, top_urls_df, growth_df):
    """
    Plot the results of domain analysis.
    
    Args:
        updates_df: DataFrame with update statistics
        top_urls_df: DataFrame with top updated URLs
        growth_df: DataFrame with growth statistics
    """
    # Convert Spark DataFrames to Pandas for plotting
    updates_pd = updates_df.toPandas()
    top_urls_pd = top_urls_df.toPandas()
    growth_pd = growth_df.toPandas()
    
    # Create a figure with 3 subplots
    fig, axs = plt.subplots(3, 1, figsize=(12, 15))
    
    # Plot 1: Update frequency
    axs[0].plot(updates_pd["date"], updates_pd["update_ratio"], "b-", marker="o")
    axs[0].set_title("Domain Update Frequency")
    axs[0].set_xlabel("Date")
    axs[0].set_ylabel("Ratio of URLs Updated")
    axs[0].grid(True)
    
    # Plot 2: Top URLs by update count
    axs[1].barh(top_urls_pd["url"].str[-30:], top_urls_pd["update_count"], color="g")
    axs[1].set_title("Top Updated URLs")
    axs[1].set_xlabel("Update Count")
    axs[1].set_ylabel("URL (truncated)")
    
    # Plot 3: Domain growth
    axs[2].plot(growth_pd["date"], growth_pd["cumulative_urls"], "r-", marker="o")
    axs[2].set_title("Cumulative Domain Growth")
    axs[2].set_xlabel("Date")
    axs[2].set_ylabel("Total URLs")
    axs[2].grid(True)
    
    plt.tight_layout()
    plt.savefig("domain_analysis_results.png")
    print("Results plotted and saved to domain_analysis_results.png")


def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("CC-Store Domain Analysis") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    # Parse command line arguments or use defaults
    import sys
    storage_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/cc_store_example"
    domain = sys.argv[2] if len(sys.argv) > 2 else "example.com"
    start_date = sys.argv[3] if len(sys.argv) > 3 else "20230101"
    end_date = sys.argv[4] if len(sys.argv) > 4 else "20230131"
    
    # Initialize CCStore
    cc_store = CCStore(
        storage_path=storage_path,
        spark=spark
    )
    
    # Print metadata for domain
    metadata = cc_store.get_domain_metadata(domain)
    if metadata:
        print(f"\nDomain Metadata for {domain}:")
        print(f"  Total records: {metadata.total_records}")
        print(f"  Total files: {metadata.total_files}")
        print(f"  Date range: {metadata.min_date} - {metadata.max_date}")
    else:
        print(f"No metadata found for domain {domain}")
    
    # Run analysis
    updates_df = analyze_domain_update_frequency(cc_store, domain, start_date, end_date)
    top_urls_df = find_top_updated_urls(cc_store, domain, start_date, end_date)
    growth_df = analyze_domain_growth(cc_store, domain, start_date, end_date)
    
    # Print results
    if updates_df:
        print("\nUpdate Frequency by Date:")
        updates_df.show()
    
    if top_urls_df:
        print("\nTop Updated URLs:")
        top_urls_df.show(truncate=False)
    
    if growth_df:
        print("\nDomain Growth by Date:")
        growth_df.show()
    
    # Plot results if all analyses were successful
    if updates_df and top_urls_df and growth_df:
        try:
            plot_results(updates_df, top_urls_df, growth_df)
        except Exception as e:
            print(f"Could not plot results: {e}")
    
    print("\nAnalysis completed.")


if __name__ == "__main__":
    main() 