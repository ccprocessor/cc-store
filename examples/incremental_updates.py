#!/usr/bin/env python
"""
Incremental Updates Examples for CC-Store.

This example demonstrates how to perform and analyze incremental updates:
1. Detect new URLs between crawl dates
2. Track content changes over time
3. Generate incremental update reports
"""

import os
import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
import matplotlib.pyplot as plt
import pandas as pd
from bs4 import BeautifulSoup
import difflib

from cc_store.core.cc_store import CCStore


def detect_new_urls(cc_store, domain, current_date, prev_date):
    """
    Detect new URLs that appeared in the current date but not in the previous date.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        current_date: Current date in YYYYMMDD format
        prev_date: Previous date in YYYYMMDD format
        
    Returns:
        DataFrame with new URLs and their data
    """
    print(f"Detecting new URLs for {domain} between {prev_date} and {current_date}...")
    
    # Read current and previous date data without HTML to improve performance
    current_df = cc_store.read_domain(domain, current_date, current_date, with_html=False)
    prev_df = cc_store.read_domain(domain, prev_date, prev_date, with_html=False)
    
    if current_df.count() == 0:
        print(f"No data found for domain {domain} on {current_date}")
        return None
        
    if prev_df.count() == 0:
        print(f"No data found for domain {domain} on {prev_date}")
        print(f"All URLs on {current_date} will be considered new")
        return current_df.withColumn("is_new", F.lit(True))
    
    # Extract distinct URLs from each date
    current_urls = current_df.select("url").distinct()
    prev_urls = prev_df.select("url").distinct()
    
    # Find URLs that exist in current but not in previous
    new_urls = current_urls.subtract(prev_urls)
    
    # Get full data for new URLs
    new_pages = current_df.join(new_urls, "url")
    
    # Add a flag indicating this is a new URL
    new_pages = new_pages.withColumn("is_new", F.lit(True))
    
    # Calculate statistics
    total_current = current_urls.count()
    total_new = new_urls.count()
    new_ratio = total_new / total_current if total_current > 0 else 0
    
    print(f"Statistics: {total_new} new URLs out of {total_current} total URLs ({new_ratio:.2%})")
    
    return new_pages


def track_content_changes(cc_store, domain, url_list, start_date, end_date):
    """
    Track how specific URLs' content changes over time.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        url_list: List of URLs to track
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        DataFrame with content change metrics for each URL and date
    """
    print(f"Tracking content changes for {len(url_list)} URLs from {start_date} to {end_date}...")
    
    # Create a DataFrame from the URL list
    url_df = cc_store.spark.createDataFrame([(url,) for url in url_list], ["url"])
    
    # Read domain data with HTML content
    df = cc_store.read_domain(domain, start_date, end_date, with_html=True)
    
    # Filter to only include the URLs we're interested in
    df = df.join(url_df, "url")
    
    if df.count() == 0:
        print(f"No data found for the specified URLs in the date range")
        return None
    
    # Group by URL and sort by date to get snapshots in chronological order
    snapshots = df.select("url", "date", "html").orderBy("url", "date")
    
    # Function to compute change metrics
    @F.udf(returnType=T.StructType([
        T.StructField("text_diff_ratio", T.FloatType()),
        T.StructField("length_diff", T.IntegerType()),
        T.StructField("length_diff_ratio", T.FloatType())
    ]))
    def compute_change_metrics(prev_html, curr_html):
        if not prev_html or not curr_html:
            return (0.0, 0, 0.0)
            
        try:
            # Extract text content
            def extract_text(html):
                soup = BeautifulSoup(html, "html.parser")
                for script in soup(["script", "style"]):
                    script.extract()
                return soup.get_text()
                
            prev_text = extract_text(prev_html)
            curr_text = extract_text(curr_html)
            
            # Compute text difference using difflib
            diff_ratio = difflib.SequenceMatcher(None, prev_text, curr_text).ratio()
            change_ratio = 1.0 - diff_ratio
            
            # Compute length differences
            prev_len = len(prev_html)
            curr_len = len(curr_html)
            len_diff = curr_len - prev_len
            len_diff_ratio = abs(len_diff) / max(prev_len, 1)
            
            return (float(change_ratio), int(len_diff), float(len_diff_ratio))
        except:
            return (0.0, 0, 0.0)
    
    # Window specification to access previous row's data
    window_spec = F.Window.partitionBy("url").orderBy("date")
    
    # Add previous date and HTML for comparison
    snapshots = snapshots.withColumn("prev_date", F.lag("date").over(window_spec))
    snapshots = snapshots.withColumn("prev_html", F.lag("html").over(window_spec))
    
    # Compute change metrics
    snapshots = snapshots.withColumn(
        "changes", 
        compute_change_metrics(F.col("prev_html"), F.col("html"))
    )
    
    # Extract metrics from struct
    snapshots = snapshots.withColumn("text_diff_ratio", F.col("changes.text_diff_ratio"))
    snapshots = snapshots.withColumn("length_diff", F.col("changes.length_diff"))
    snapshots = snapshots.withColumn("length_diff_ratio", F.col("changes.length_diff_ratio"))
    
    # Clean up temporary columns
    snapshots = snapshots.drop("changes", "html", "prev_html")
    
    # Filter out first snapshot for each URL (no previous to compare with)
    snapshots = snapshots.filter(F.col("prev_date").isNotNull())
    
    # Add a column indicating significant changes
    snapshots = snapshots.withColumn(
        "significant_change", 
        F.col("text_diff_ratio") > 0.1  # More than 10% change
    )
    
    return snapshots


def generate_update_report(cc_store, domain, report_date, prev_date=None):
    """
    Generate a comprehensive report of updates for a domain on a specific date.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        report_date: Date to report on in YYYYMMDD format
        prev_date: Previous date for comparison (defaults to day before report_date)
        
    Returns:
        Dictionary with report data
    """
    print(f"Generating update report for {domain} on {report_date}...")
    
    # If previous date not provided, use day before report date
    if prev_date is None:
        # Convert to datetime, subtract one day, convert back to string
        report_dt = datetime.datetime.strptime(report_date, "%Y%m%d")
        prev_dt = report_dt - datetime.timedelta(days=1)
        prev_date = prev_dt.strftime("%Y%m%d")
    
    # Read report date data
    df = cc_store.read_domain(domain, report_date, report_date, with_html=True)
    
    if df.count() == 0:
        print(f"No data found for domain {domain} on {report_date}")
        return None
    
    # 1. Detect new URLs
    new_urls_df = detect_new_urls(cc_store, domain, report_date, prev_date)
    
    # 2. Calculate update statistics
    total_urls = df.select("url").distinct().count()
    new_urls_count = new_urls_df.count() if new_urls_df is not None else 0
    
    # 3. Analyze content types
    content_types = df.withColumn(
        "content_type", 
        F.regexp_extract(
            F.concat_ws(" ", F.col("response_header")), 
            r"Content-Type: ([^;]+)", 
            1
        )
    ).groupBy("content_type").count()
    
    # 4. Find updated URLs (for existing URLs)
    if new_urls_count < total_urls:
        # Get existing URLs (subtract new URLs)
        existing_urls = df.select("url").distinct()
        if new_urls_df is not None:
            existing_urls = existing_urls.subtract(new_urls_df.select("url").distinct())
            
        # Track changes for existing URLs
        existing_url_list = [row["url"] for row in existing_urls.collect()]
        changes_df = track_content_changes(cc_store, domain, existing_url_list, prev_date, report_date)
        
        if changes_df is not None:
            significant_changes = changes_df.filter(F.col("significant_change")).count()
        else:
            significant_changes = 0
    else:
        changes_df = None
        significant_changes = 0
    
    # 5. Compile report
    report = {
        "domain": domain,
        "report_date": report_date,
        "previous_date": prev_date,
        "total_urls": total_urls,
        "new_urls": new_urls_count,
        "new_urls_ratio": new_urls_count / total_urls if total_urls > 0 else 0,
        "updated_urls": significant_changes,
        "updated_urls_ratio": significant_changes / (total_urls - new_urls_count) if (total_urls - new_urls_count) > 0 else 0,
        "content_types": [(row["content_type"], row["count"]) for row in content_types.collect()],
        "new_urls_sample": [row["url"] for row in new_urls_df.select("url").limit(10).collect()] if new_urls_df is not None else [],
        "changes_sample": []
    }
    
    # Add sample of changed URLs with their change ratios
    if changes_df is not None:
        changes_sample = changes_df.filter(F.col("significant_change")) \
            .orderBy(F.col("text_diff_ratio").desc()) \
            .select("url", "date", "text_diff_ratio") \
            .limit(10) \
            .collect()
            
        report["changes_sample"] = [(row["url"], row["text_diff_ratio"]) for row in changes_sample]
    
    return report


def export_incremental_stats(cc_store, domain, date_range, output_path):
    """
    Export incremental statistics for a sequence of dates.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        date_range: List of dates in YYYYMMDD format
        output_path: Path to save the output CSV file
        
    Returns:
        DataFrame with daily incremental statistics
    """
    print(f"Exporting incremental statistics for {domain} over {len(date_range)} dates...")
    
    daily_stats = []
    
    for i in range(1, len(date_range)):
        curr_date = date_range[i]
        prev_date = date_range[i-1]
        
        print(f"Processing {curr_date} vs {prev_date}...")
        
        # Get current day data
        curr_df = cc_store.read_domain(domain, curr_date, curr_date, with_html=False)
        
        if curr_df.count() == 0:
            print(f"No data found for {curr_date}, skipping...")
            continue
            
        # Get previous day data
        prev_df = cc_store.read_domain(domain, prev_date, prev_date, with_html=False)
        
        if prev_df.count() == 0:
            print(f"No data found for {prev_date}, treating all current URLs as new...")
            current_urls_count = curr_df.select("url").distinct().count()
            day_stats = {
                "date": curr_date,
                "total_urls": current_urls_count,
                "new_urls": current_urls_count,
                "new_ratio": 1.0,
                "changed_urls": 0,
                "changed_ratio": 0.0
            }
            daily_stats.append(day_stats)
            continue
        
        # Extract distinct URLs from each date
        curr_urls = curr_df.select("url").distinct()
        prev_urls = prev_df.select("url").distinct()
        
        # Calculate statistics
        curr_count = curr_urls.count()
        prev_count = prev_urls.count()
        
        # New URLs (in current but not in previous)
        new_urls = curr_urls.subtract(prev_urls)
        new_count = new_urls.count()
        
        # Calculate changed URLs
        existing_urls = curr_urls.intersect(prev_urls)
        existing_count = existing_urls.count()
        
        # To calculate changed URLs, we'd need to compare content
        # This would be expensive, so we'll just use a placeholder value
        # In a real implementation, you might want to use content hashes or other metrics
        changed_count = 0  # Placeholder
        
        day_stats = {
            "date": curr_date,
            "total_urls": curr_count,
            "new_urls": new_count,
            "new_ratio": new_count / curr_count if curr_count > 0 else 0.0,
            "changed_urls": changed_count,
            "changed_ratio": changed_count / existing_count if existing_count > 0 else 0.0,
            "removed_urls": prev_count - existing_count
        }
        
        daily_stats.append(day_stats)
    
    # Convert to DataFrame
    stats_df = cc_store.spark.createDataFrame(daily_stats)
    
    # Save to CSV
    if output_path:
        stats_pd = stats_df.toPandas()
        stats_pd.to_csv(output_path, index=False)
        print(f"Incremental statistics saved to {output_path}")
    
    return stats_df


def plot_update_report(report):
    """Plot a visual summary of the update report."""
    fig, axs = plt.subplots(2, 1, figsize=(10, 10))
    
    # Plot 1: URL update breakdown
    labels = ['New URLs', 'Updated URLs', 'Unchanged URLs']
    sizes = [
        report["new_urls"],
        report["updated_urls"],
        report["total_urls"] - report["new_urls"] - report["updated_urls"]
    ]
    colors = ['#ff9999', '#66b3ff', '#99ff99']
    
    axs[0].pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
    axs[0].axis('equal')
    axs[0].set_title(f'URL Breakdown for {report["domain"]} on {report["report_date"]}')
    
    # Plot 2: Content types
    content_types = [x[0] if x[0] else "Unknown" for x in report["content_types"]]
    counts = [x[1] for x in report["content_types"]]
    
    axs[1].barh(content_types, counts, color='#66b3ff')
    axs[1].set_title('Content Types Distribution')
    axs[1].set_xlabel('Count')
    
    plt.tight_layout()
    plt.savefig(f"update_report_{report['domain']}_{report['report_date']}.png")
    print(f"Report plot saved as update_report_{report['domain']}_{report['report_date']}.png")


def plot_incremental_stats(stats_df):
    """Plot incremental statistics over time."""
    # Convert to pandas for plotting
    stats_pd = stats_df.toPandas()
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Plot new URLs count and ratio
    ax1.bar(stats_pd["date"], stats_pd["new_urls"], color='#66b3ff')
    ax1.set_title('New URLs by Date')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('New URLs Count')
    
    # Add new ratio as a line on a secondary axis
    ax1_ratio = ax1.twinx()
    ax1_ratio.plot(stats_pd["date"], stats_pd["new_ratio"] * 100, 'r-', marker='o')
    ax1_ratio.set_ylabel('New URLs Ratio (%)', color='r')
    ax1_ratio.tick_params(axis='y', colors='r')
    
    # Plot total URLs and removed URLs
    ax2.bar(stats_pd["date"], stats_pd["total_urls"], color='#99ff99', label='Total URLs')
    ax2.bar(stats_pd["date"], stats_pd["removed_urls"], color='#ff9999', label='Removed URLs')
    ax2.set_title('Total URLs and Removed URLs by Date')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('URL Count')
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig("incremental_stats.png")
    print("Incremental statistics plot saved as incremental_stats.png")


def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("CC-Store Incremental Updates") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    # Parse command line arguments or use defaults
    import sys
    storage_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/cc_store_example"
    domain = sys.argv[2] if len(sys.argv) > 2 else "example.com"
    report_date = sys.argv[3] if len(sys.argv) > 3 else "20230102"
    prev_date = sys.argv[4] if len(sys.argv) > 4 else "20230101"
    
    # Initialize CCStore
    cc_store = CCStore(
        storage_path=storage_path,
        spark=spark
    )
    
    # Get domain metadata
    metadata = cc_store.get_domain_metadata(domain)
    if metadata:
        print(f"\nDomain Metadata for {domain}:")
        print(f"  Total records: {metadata.total_records}")
        print(f"  Total files: {metadata.total_files}")
        print(f"  Date range: {metadata.min_date} - {metadata.max_date}")
    else:
        print(f"No metadata found for domain {domain}")
        return
    
    # Get available dates in chronological order
    dates = cc_store.get_dates_for_domain(domain)
    print(f"Available dates: {dates}")
    
    # If specified dates aren't available, use available dates
    if report_date not in dates:
        if len(dates) > 1:
            report_date = dates[-1]  # Latest date
            prev_date = dates[-2]    # Second to latest
            print(f"Specified report date not found. Using {report_date} and {prev_date} instead.")
        elif len(dates) == 1:
            print(f"Only one date available: {dates[0]}. Cannot perform incremental analysis.")
            return
        else:
            print("No dates available for analysis.")
            return
    
    # 1. Generate update report for a specific date
    report = generate_update_report(cc_store, domain, report_date, prev_date)
    
    if report:
        print("\nUpdate Report Summary:")
        print(f"  Domain: {report['domain']}")
        print(f"  Date: {report['report_date']} (comparing to {report['previous_date']})")
        print(f"  Total URLs: {report['total_urls']}")
        print(f"  New URLs: {report['new_urls']} ({report['new_urls_ratio']:.2%})")
        print(f"  Updated URLs: {report['updated_urls']} ({report['updated_urls_ratio']:.2%})")
        print(f"  Content Types: {report['content_types']}")
        
        # Plot report
        try:
            plot_update_report(report)
        except Exception as e:
            print(f"Could not plot report: {e}")
    
    # 2. Export incremental statistics over all available dates
    if len(dates) > 1:
        stats_df = export_incremental_stats(cc_store, domain, dates, f"{domain}_incremental_stats.csv")
        
        if stats_df:
            print("\nIncremental Statistics Summary:")
            stats_df.show()
            
            # Plot incremental stats
            try:
                plot_incremental_stats(stats_df)
            except Exception as e:
                print(f"Could not plot incremental statistics: {e}")
    
    print("\nIncremental update analysis completed.")


if __name__ == "__main__":
    main() 